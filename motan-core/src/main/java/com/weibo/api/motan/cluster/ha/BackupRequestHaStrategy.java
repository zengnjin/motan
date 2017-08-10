/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.cluster.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Histogram;
import com.weibo.api.motan.cluster.loadbalance.AutoDecrWeightLoadBalance;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.util.*;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.weibo.api.motan.cluster.LoadBalance;
import com.weibo.api.motan.core.extension.ExtensionLoader;
import com.weibo.api.motan.core.extension.SpiMeta;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.Application;
import com.weibo.api.motan.rpc.ApplicationInfo;
import com.weibo.api.motan.rpc.Referer;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.rpc.URL;
import com.weibo.api.motan.switcher.LocalSwitcherService;
import com.weibo.api.motan.switcher.Switcher;
import com.weibo.api.motan.switcher.SwitcherService;

/**
 * Backup request ha strategy <br />
 * After a delay, the second request is made, and eventually the faster one is returned
 *
 * 
 */
@SpiMeta(name = BackupRequestHaStrategy.NAME)
public class BackupRequestHaStrategy<T> extends AbstractHaStrategy<T> {
    public static final String NAME = "backupRequest";
    public static final String MASTER_SWITCHER_NAME = "feature.motan.backuprequest.enable";
    protected ThreadLocal<List<Referer<T>>> referersHolder = new ThreadLocal<List<Referer<T>>>() {
        protected List<Referer<T>> initialValue() {
            return new ArrayList<Referer<T>>();
        }
    };
    private static Switcher backupRequestEnableSwitcher;
    private static SwitcherService switcherService;
    private String urlSwitcherName = null;
    private Application application = null;

    static {
        String switchName = URLParamType.switcherService.getValue();
        switcherService = ExtensionLoader.getExtensionLoader(SwitcherService.class).getExtension(
        		switchName);
        switcherService.initSwitcher(MASTER_SWITCHER_NAME, true);
        backupRequestEnableSwitcher = switcherService.getSwitcher(MASTER_SWITCHER_NAME);
    }

    @Override
    public Response call(final Request request, LoadBalance<T> loadBalance) {
        Referer<T> referer = loadBalance.select(request);
        if (referer == null) {
            throw new MotanServiceException(
                    String.format("BackupRequestHaStrategy No referers for request=%s, loadbalance=%s", request,
                            loadBalance));
        }

        if (backupRequestEnableSwitcher == null || !backupRequestEnableSwitcher.isOn()) {
            return referer.call(request);
        }

        int queueSize = WORK_QUEUE.size();
        if (queueSize > CORE_THREAD_SIZE) {
            LoggerUtil.warn(String.format("BackupRequestHaStrategy self-protect undo backup for: "
                    + "queueSize=%s, request=%s", queueSize, MotanFrameworkUtil.toString(request)));
            return referer.call(request);
        }

        URL refUrl = referer.getUrl();
        Switcher urlSwitcher = switcherService.getSwitcher(this.urlSwitcherName);
        if (urlSwitcher == null || !urlSwitcher.isOn()) {
            return referer.call(request);
        }
        int tryCount = refUrl.getMethodParameter(request.getMethodName(), request.getParamtersDesc(),
                URLParamType.retries.getName(), URLParamType.retries.getIntValue());
        if (tryCount <= 0) {
            return referer.call(request);
        }


        String serviceName = MotanFrameworkUtil.getFullMethodString(request);
        int requestTimeout = refUrl.getMethodParameter(request.getMethodName(), request.getParamtersDesc(),
                URLParamType.requestTimeout.getName(), URLParamType.requestTimeout.getIntValue());
        int defaultDelayTime = (int) Math.ceil(requestTimeout * DEFAULT_BACKUP_REQUEST_RELAY_RATIO);
        int delayTime = BackupRequestStatsUtil.getBackupRequestDelayTime(this.application, serviceName,
                defaultDelayTime);
        int backupRequstDelayTime = refUrl.getMethodParameter(request.getMethodName(), request.getParamtersDesc(),
                URLParamType.backupRequstDelayTime.getName(), URLParamType.backupRequstDelayTime.getIntValue());
        if(backupRequstDelayTime > 0) {
            delayTime = backupRequstDelayTime;
        }
        String maxRetryRatioStr = refUrl.getMethodParameter(request.getMethodName(), request.getParamtersDesc(),
                URLParamType.backupRequstMaxRetryRatio.getName(), URLParamType.backupRequstMaxRetryRatio.getValue());
        long startTime = System.currentTimeMillis();
        StatsUtil.accessStatistic("motan-backup-delay" + serviceName, this.application, startTime,
                delayTime, 0, StatsUtil.AccessStatus.NORMAL);
        try {
            return backupRequestCall(request, loadBalance, referer, requestTimeout, delayTime, Float.parseFloat(maxRetryRatioStr), tryCount);
        } finally {
            long nowTs = System.currentTimeMillis();
            StatsUtil.accessStatistic("motan-backupha" + serviceName, this.application, nowTs, nowTs - startTime, 0,
                    StatsUtil.AccessStatus.NORMAL);
            StatsUtil.accessStatistic("motan-backuptc" + serviceName, this.application, nowTs, TOTAL_COUNTER.get(), 0,
                    StatsUtil.AccessStatus.NORMAL);
        }
    }

    private Response backupRequestCall(final Request request, LoadBalance<T> loadBalance, Referer<T> referer,
                                       int requestTimeout, final long backupRequestDelayTime, float maxRetryRatio, int retries) {
        final BlockingQueue<ValueHolderWithRefer<Response>> responseQueue =
                new ArrayBlockingQueue<ValueHolderWithRefer<Response>>(retries + 1);
        final BlockingQueue<ValueHolderWithRefer<RuntimeException>> exceptionQueue =
                new ArrayBlockingQueue<ValueHolderWithRefer<RuntimeException>>(retries + 1);
        List<ValueHolderWithRefer<ListenableFuture>> futureHolders =
                Lists.newArrayListWithCapacity(retries + 1);

        Response response = null;
        RuntimeException finalException = null;
        String serviceName = MotanFrameworkUtil.getFullMethodString(request);
        BackupRequestStatsUtil.incrFirstRequstCount(this.application, serviceName);
        for (int i = 0 ; i <= retries; i++) {
            Referer useRefer =  null;
            if(i == 0) {
                useRefer = referer;
            } else {
                useRefer = loadBalance.select(request);
                if(useRefer == null) {
                    break;
                }
            }
            submitRequest(i, request, useRefer, responseQueue, exceptionQueue, futureHolders, retries);
            response = getResponseWithTimeout(backupRequestDelayTime, responseQueue);
            finalException = checkException(request, exceptionQueue, i);
            // send backup request
            if (response != null || ! BackupRequestStatsUtil.incrAndCheckSecondRequestRatio(this.application, serviceName, maxRetryRatio)) {
                break;
            }
        }
        // get faster response
        if(response == null) {
            response = getResponseWithTimeout(requestTimeout, responseQueue);
        }

        // cancel undone future
        for (ValueHolderWithRefer<ListenableFuture> futureHolder : futureHolders) {
            if (!futureHolder.value.isDone()) {
                try {
                    boolean cancelResult = futureHolder.value.cancel(true);
                    if (!cancelResult) {
                        LoggerUtil
                                .warn(String.format("BackupRequestHaStrategy cancel for: seq=%s, url=%s, request = %s, "
                                        + "cancelResult=%s", futureHolder.seq, futureHolder.refer.getUrl()
                                        .getServerPortStr(), MotanFrameworkUtil.toString(request), cancelResult));
                    }
                } catch (Exception e) {
                    // ignore cancel execption, and do nothing
                }
            }
        }
        if (response != null) {
            return response;
        }
        if (finalException == null) {
            finalException = new MotanServiceException("BackupRequest timeout " + serviceName);
        }
        throw finalException;
    }

    private RuntimeException checkException(Request request,
                                            BlockingQueue<ValueHolderWithRefer<RuntimeException>> exceptionQueue, int retries) {
        // if no response, handle exception
        RuntimeException finalException = null;
        for (int i = 0; i <= retries; i++) {
            ValueHolderWithRefer<RuntimeException> exceptionHolder = exceptionQueue.poll();
            if (exceptionHolder != null) {
                LoggerUtil.warn(String.format("BackupRequestHaStrategy call false for: seq=%s, url=%s, request=%s, "
                                + "exception=%s", exceptionHolder.seq, exceptionHolder.refer.getUrl()
                                .getServerPortStr(),
                        MotanFrameworkUtil.toString(request), exceptionHolder.value.getMessage()));
                // firstly throw MotanBizException
                if (ExceptionUtil.isBizException(exceptionHolder.value) && !ExceptionUtil
                        .isBizException(finalException)) {
                    throw exceptionHolder.value;
                }
                if (finalException == null) {
                    finalException = exceptionHolder.value;
                }
            }
        }
        return finalException;
    }

    private Response getResponseWithTimeout(long waitTime, BlockingQueue<ValueHolderWithRefer<Response>> responseQueue) {
        ValueHolderWithRefer<Response> responseHolder = null;
        try {
            responseHolder =
                    responseQueue.poll(waitTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        if (responseHolder != null && responseHolder.value != null) {
            return responseHolder.value;
        }
        return null;
    }

    private void submitRequest(final int seq, final Request request, final Referer<T> refer,
                               final BlockingQueue<ValueHolderWithRefer<Response>> responseQueue,
                               final BlockingQueue<ValueHolderWithRefer<RuntimeException>> exceptionQueue,
                               List<ValueHolderWithRefer<ListenableFuture>> futureHolders, final int retries) {
        request.setRetries(seq);
        //final RequestTraceContext context = RequestTraceContext.get();
        final long startTime = System.currentTimeMillis();
        final Application selfApplication = this.application;
        ListenableFuture future = SERVICE.submit(new Callable<Response>() {
            @Override
            public Response call() throws Exception {
                try {
                    TOTAL_COUNTER.incrementAndGet();
                    //RequestTraceContext.spawn(context);
                    long nowTs = System.currentTimeMillis();
                    StatsUtil.accessStatistic("motan-backuprequest-" + seq + MotanFrameworkUtil.getFullMethodString
                            (request), selfApplication, nowTs, nowTs - startTime, 0, StatsUtil.AccessStatus.NORMAL);
                    return refer.call(request);
                } finally {
                    TOTAL_COUNTER.decrementAndGet();
                    //RequestTraceContext.clear();
                }
            }
        });
        final Histogram histogram = InternalMetricsFactory.getRegistryInstance(AutoDecrWeightLoadBalance.METRIC_RPC_AVG_KEY)
                .histogram(BackupRequestStatsUtil.getMetricKey(refer, MotanFrameworkUtil.getFullMethodString(request)));
        Futures.addCallback(future, new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response response) {

                long nowTs = System.currentTimeMillis();
                StatsUtil.accessStatistic("motan-backupresponse-" + seq + MotanFrameworkUtil.getFullMethodString
                        (request), selfApplication, nowTs, nowTs - startTime, response.getProcessTime(), StatsUtil
                        .AccessStatus.NORMAL);
                histogram.update(nowTs - startTime);
                responseQueue.offer(new ValueHolderWithRefer<Response>(seq, refer, response));
            }

            @Override
            public void onFailure(Throwable t) {
                StatsUtil.AccessStatus accessStatus = StatsUtil.AccessStatus.OTHER_EXCEPTION;
                if (ExceptionUtil.isBizException(t)) {
                    accessStatus = StatsUtil.AccessStatus.BIZ_EXCEPTION;
                }
                long nowTs = System.currentTimeMillis();
                StatsUtil.accessStatistic("motan-backupresponse-" + seq + MotanFrameworkUtil
                        .getFullMethodString(request), selfApplication, nowTs, nowTs - startTime, 0, accessStatus);
                histogram.update(nowTs - startTime);
                if (t instanceof RuntimeException) {
                    exceptionQueue
                            .offer(new ValueHolderWithRefer<RuntimeException>(seq, refer, (RuntimeException) t));
                } else {
                    MotanServiceException motanServiceException = new MotanServiceException(t);
                    exceptionQueue.offer(new ValueHolderWithRefer<RuntimeException>(seq, refer,
                            motanServiceException));
                }
                // failfast when throw MotanBizException
                if (ExceptionUtil.isBizException(t)) {
                    responseQueue.offer(new ValueHolderWithRefer<Response>(seq, refer, null));
                }
                // failfast when all request throw exception
                if (exceptionQueue.size() == retries + 1) {
                    responseQueue.offer(new ValueHolderWithRefer<Response>(seq, refer, null));
                }
            }
        });
        futureHolders.add(new ValueHolderWithRefer<ListenableFuture>(seq, refer, future));
    }

    protected List<Referer<T>> selectReferers(Request request, LoadBalance<T> loadBalance) {
        List<Referer<T>> referers = referersHolder.get();
        referers.clear();
        loadBalance.selectToHolder(request, referers);
        return referers;
    }

    class ValueHolderWithRefer<T> {
        int seq;
        Referer refer;
        T value;

        ValueHolderWithRefer(int seq, Referer refer, T value) {
            this.seq = seq;
            this.refer = refer;
            this.value = value;
        }
    }

    @Override
    public void setUrl(URL url) {
        this.url = url;
        this.application = ApplicationInfo.getApplication(url);
        String backupRequstSwitcherNameStr = url.getParameter("backupRequstSwitcherName");
        if (StringUtils.isNotBlank(backupRequstSwitcherNameStr)) {
            String[] eles = StringUtils.split(backupRequstSwitcherNameStr, ":");
            this.urlSwitcherName = MASTER_SWITCHER_NAME + "." + eles[0];
            boolean urlSwitcherValue = false;
            if (eles.length == 2) {
                urlSwitcherValue = Boolean.valueOf(eles[1]);
            }
            switcherService.initSwitcher(this.urlSwitcherName, urlSwitcherValue);
        }
        BackupRequestStatsUtil.registerServiceInterface(this.application, url.getPath());
    }

    private static final double DEFAULT_BACKUP_REQUEST_RELAY_RATIO = 0.4;
    private static final AtomicInteger TOTAL_COUNTER = new AtomicInteger(0);
    private static final int CORE_THREAD_SIZE = 300;
    private static final BlockingQueue<Runnable> WORK_QUEUE = new LinkedBlockingQueue<Runnable>();
    private static final AtomicInteger THREAD_SEQ = new AtomicInteger(0);
    private static final ListeningExecutorService SERVICE = MoreExecutors.listeningDecorator(
            new ThreadPoolExecutor(CORE_THREAD_SIZE, CORE_THREAD_SIZE, 60, TimeUnit.SECONDS, WORK_QUEUE,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "ThreadBackupRequest-" + THREAD_SEQ.getAndIncrement());
                        }
                    }));
}
