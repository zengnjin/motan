/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.codahale.metrics.Snapshot;
import com.google.common.collect.Lists;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.rpc.Application;
import com.weibo.api.motan.rpc.Referer;

/**
 * BackupRequestStatsUtil
 *
 * @author leijian
 */
public class BackupRequestStatsUtil {

    public static synchronized void registerServiceInterface(Application application, String serviceInterfaceName) {
        String name = wrapApplicationInfoForName(application, serviceInterfaceName);
        if (!SERVICE_INTERFACE_NAMES.contains(name)) {
            SERVICE_INTERFACE_NAMES.add(name);
        }
    }

    public static int getBackupRequestDelayTime(Application application, String serviceName, int defaultValue) {
        String name = wrapApplicationInfoForName(application, serviceName);
        Integer backupRequestTime = SECOND_REQUEST_DELAY_TIMES.get(name);
        if (backupRequestTime == null) {
            backupRequestTime = defaultValue;
        }
        if (backupRequestTime <= 10) {
            backupRequestTime = 10;
        }
        return backupRequestTime;
    }

    public static void incrFirstRequstCount(Application application, String serviceName) {
        String name = wrapApplicationInfoForName(application, serviceName);
        QpsCounter qpsCounter = FIRST_REQUEST_QPS_COUNTERS.get(name);
        if (qpsCounter == null) {
            qpsCounter = new QpsCounter();
            QpsCounter existQpsCounter = FIRST_REQUEST_QPS_COUNTERS.putIfAbsent(name, qpsCounter);
            if (existQpsCounter != null) {
                qpsCounter = existQpsCounter;
            }
        }
        qpsCounter.incr(System.currentTimeMillis(), 1L);
    }

    public static boolean incrAndCheckSecondRequestRatio(Application application, String serviceName,
                                                         float maxRetryRatio) {
        String name = wrapApplicationInfoForName(application, serviceName);
        QpsCounter secondRequestQpsCounter = SECOND_REQUEST_QPS_COUNTERS.get(name);
        if (secondRequestQpsCounter == null) {
            secondRequestQpsCounter = new QpsCounter();
            QpsCounter existQpsCounter = SECOND_REQUEST_QPS_COUNTERS.putIfAbsent(name, secondRequestQpsCounter);
            if (existQpsCounter != null) {
                secondRequestQpsCounter = existQpsCounter;
            }
        }
        long secondRequestInstantQps = secondRequestQpsCounter.getInstantQps(System.currentTimeMillis());
        boolean inScope = false;
        // at least one second request can be sent
        if (secondRequestInstantQps == 0) {
            inScope = true;
        } else {
            Double firstRequestQps = FIRST_REQUEST_QPS_MAP.get(name);
            if (firstRequestQps != null) {
                inScope = secondRequestInstantQps + 1 <= firstRequestQps * maxRetryRatio;
            }
        }
        if (inScope) {
            secondRequestQpsCounter.incr(System.currentTimeMillis(), 1L);
        }
        return inScope;
    }

    public static String getMetricKey(Referer referer, String method) {
        StringBuilder sb = new StringBuilder();
        sb.append(referer.getServiceUrl().getIdentity()).append("|").append(method);
        return sb.toString();
    }

    private static void daemonStats() {
        long startTime = System.currentTimeMillis();
        // update qps
        for (Map.Entry<String, QpsCounter> entry : FIRST_REQUEST_QPS_COUNTERS.entrySet()) {
            double qps = entry.getValue().getQps(startTime);
            FIRST_REQUEST_QPS_MAP.put(entry.getKey(), qps);
            LoggerUtil.info(String.format("BackupRequestStatsUtil daemon update qps: serviceName=%s, qps=%s",
                    entry.getKey(), qps));
        }
        // update pXXX waterline
        for (String serviceName : StatsUtil.accessStatistics.keySet()) {
            Double waterline = getWaterline(serviceName);
            if (waterline != null) {
                Snapshot snapshot = InternalMetricsFactory.getRegistryInstance(serviceName)
                        .histogram(StatsUtil.getHistogramName()).getSnapshot();
                double waterlineTime = snapshot.getValue(waterline);
                int delayTime = (int) Math.ceil(waterlineTime);
                SECOND_REQUEST_DELAY_TIMES.put(serviceName, delayTime);
                LoggerUtil.info(String.format("BackupRequestStatsUtil daemon update secodeRequest dealy time: "
                        + "serviceName=%s, waterline=%s, delayTime=%s", serviceName, waterline, delayTime));
            }
        }
        long nowTs = System.currentTimeMillis();
        LoggerUtil.info(String.format("BackupRequestStatsUtil daemon stats finish time-cost(ms) = %s "
                , nowTs - startTime));
        StatsUtil.accessStatistic("SERVICE",  null, nowTs,
                nowTs - startTime, 0, StatsUtil.AccessStatus.NORMAL);
    }

    private static Double getWaterline(String key) {
        Double waterline = SERVICE_BACKUP_REQUEST_TIME_WATERLINE.get(key);
        if (waterline == null) {
            for (String interfaceName : SERVICE_INTERFACE_NAMES) {
                if (StringUtils.startsWith(key, interfaceName)) {
                    waterline = DEFAULT_SECOND_DELAY_WATERLINE / 100.0;
                    SERVICE_BACKUP_REQUEST_TIME_WATERLINE.put(interfaceName, waterline);
                    return waterline;
                }
            }
        }
        return waterline;
    }

    private static String wrapApplicationInfoForName(Application application, String name) {
        if (application == null) {
            application = new Application(URLParamType.application.getValue(), URLParamType.module.getValue());
        }

        return String.format("%s|%s|%s|%s", application.getApplication(), application.getModule(), "SERVICE", name);
    }

    private final static int DEFAULT_SECOND_DELAY_WATERLINE = 90;
    private final static ConcurrentHashMap<String, Integer> SECOND_REQUEST_DELAY_TIMES =
            new ConcurrentHashMap<String, Integer>();
    private final static List<String> SERVICE_INTERFACE_NAMES = Lists.newArrayList();
    private final static ConcurrentHashMap<String, Double> SERVICE_BACKUP_REQUEST_TIME_WATERLINE =
            new ConcurrentHashMap<String, Double>();
    private final static ConcurrentHashMap<String, QpsCounter> FIRST_REQUEST_QPS_COUNTERS =
            new ConcurrentHashMap<String, QpsCounter>();
    private final static ConcurrentHashMap<String, QpsCounter> SECOND_REQUEST_QPS_COUNTERS =
            new ConcurrentHashMap<String, QpsCounter>();
    private final static ConcurrentHashMap<String, Double> FIRST_REQUEST_QPS_MAP =
            new ConcurrentHashMap<String, Double>();

    private final static int SCHEDULE_INTERVAL = 2;   // seconds
    private final static ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    static {
        EXECUTOR_SERVICE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                daemonStats();
            }
        }, SCHEDULE_INTERVAL, SCHEDULE_INTERVAL, TimeUnit.SECONDS);
    }
}
