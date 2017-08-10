package com.weibo.api.motan.cluster.loadbalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Snapshot;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.core.extension.ExtensionLoader;
import com.weibo.api.motan.core.extension.SpiMeta;
import com.weibo.api.motan.rpc.Referer;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.switcher.Switcher;
import com.weibo.api.motan.switcher.SwitcherService;
import com.weibo.api.motan.util.BackupRequestStatsUtil;
import com.weibo.api.motan.util.InternalMetricsFactory;
import com.weibo.api.motan.util.LoggerUtil;
import com.weibo.api.motan.util.MotanFrameworkUtil;

/**
 * Created by zhenjia1 on 17/1/13.
 */
@SpiMeta(name = "autoDecrWeight")
public class AutoDecrWeightLoadBalance<T> extends AbstractLoadBalance<T> {

	private static SwitcherService switcherService;
    private static Switcher autoDecrWeightLBEnableSwitcher = null;
    public static final String AUTODECR_SWITCHER_NAME = "feature.motan.autodecrweightlb.enable";

    // 正常的权重
    private final static int NORMAL_WEIGHT = 5;
    // 较低的权重
    private final static int LOW_WEIGHT = 3;
    // 低权重机器的百分比
    private final static double PERCENT_LOW = 0.2;
    // 最大referer数量
    private final static int MAX_REFERER_COUNT = 10;
    // metric key
    public final static String METRIC_RPC_AVG_KEY = "rpc_loadbalance_metric_key";
    // global position pointer
    private AtomicInteger INDEX = new AtomicInteger(0);
    // 轮次
    private AtomicInteger ROUND = new AtomicInteger(0);

    private ConcurrentHashMap<String, ConcurrentHashMap<Referer, RefererWeightPair>> methodWeightMap = new
            ConcurrentHashMap<String, ConcurrentHashMap<Referer, RefererWeightPair>>();

    private static ConcurrentHashMap<AutoDecrWeightLoadBalance, AtomicInteger> loadBalancesMap = new
            ConcurrentHashMap<AutoDecrWeightLoadBalance, AtomicInteger>();

    private final static int SCHEDULE_INTERVAL = 60; // seconds

    private final static ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    static {
    	String switchName = URLParamType.switcherService.getValue();
        switcherService = ExtensionLoader.getExtensionLoader(SwitcherService.class).getExtension(
        		switchName);
        switcherService.initSwitcher(AUTODECR_SWITCHER_NAME, true);
        autoDecrWeightLBEnableSwitcher = switcherService.getSwitcher(AUTODECR_SWITCHER_NAME);
        EXECUTOR_SERVICE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (autoDecrWeightLBEnableSwitcher != null && autoDecrWeightLBEnableSwitcher.isOn()) {
                        daemonUpdateWeight();
                    }
                } catch (Exception e) {
                    LoggerUtil.error("AutoDecrWeightLoadBalance.daemonUpdateWeight error: {}", e);
                }

            }
        }, SCHEDULE_INTERVAL, SCHEDULE_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void onRefresh(List<Referer<T>> referers) {
        super.onRefresh(referers);
        if (autoDecrWeightLBEnableSwitcher != null && autoDecrWeightLBEnableSwitcher.isOn()) {
            AtomicInteger refreshCount = loadBalancesMap.get(this);
            if (refreshCount == null) {
                refreshCount = new AtomicInteger(0);
                AtomicInteger count = loadBalancesMap.putIfAbsent(this, refreshCount);
                if (count != null) {
                    refreshCount = count;
                }
            }
            LoggerUtil.info("AutoDecrWeightLoadBalance loadBalances: size={}, refresh count={}", loadBalancesMap.size(),
                    refreshCount.incrementAndGet());

            for (String serviceName : methodWeightMap.keySet()) {
                ConcurrentHashMap<Referer, RefererWeightPair> referMap = new ConcurrentHashMap<Referer,
                        RefererWeightPair>();
                for (Referer<T> referer : referers) {
                    referMap.put(referer, new RefererWeightPair(referer, NORMAL_WEIGHT));
                }
                // reset
                methodWeightMap.put(serviceName, referMap);
            }
        }
    }

    @Override
    protected Referer<T> doSelect(Request request) {

        // call roundrobin 临时添加RR代码
        if (autoDecrWeightLBEnableSwitcher == null || !autoDecrWeightLBEnableSwitcher.isOn()) {
            return rrDoSelect(request);
        }

        List<Referer<T>> referers = getReferers();
        String serviceMethodKey = getServiceKey(request);
        ConcurrentHashMap<Referer, RefererWeightPair> refererWeightMapShot = methodWeightMap.get(serviceMethodKey);
        if (refererWeightMapShot == null) {
            refererWeightMapShot = new ConcurrentHashMap<Referer, RefererWeightPair>();
            ConcurrentHashMap<Referer, RefererWeightPair> referMap =
                    methodWeightMap.putIfAbsent(serviceMethodKey, refererWeightMapShot);
            if (referMap != null) {
                refererWeightMapShot = referMap;
            }
        }
        int index = 0;
        int round = 0;
        int weight = 0;
        do {
            index = getNextIndex();
            round = ROUND.get();
            if (index % referers.size() == 0) {
                round = getNextRound();
            }
            Referer<T> refer = referers.get(index % referers.size());
            RefererWeightPair<T> pair = refererWeightMapShot.get(refer);
            if (pair == null) {
                pair = new RefererWeightPair<T>(refer, NORMAL_WEIGHT);
                RefererWeightPair<T> rwp = refererWeightMapShot.putIfAbsent(refer, pair);
                if (rwp != null) {
                    pair = rwp;
                }
            }
            weight = pair.getWeight();

        } while (weight < (round % NORMAL_WEIGHT) + 1);

        for (int i = 0; i < referers.size(); i++) {
            Referer<T> referer = referers.get((i + index) % referers.size());
            if (referer.isAvailable()) {
                RefererWeightPair<T> pair = refererWeightMapShot.get(referer);
                if (pair != null) {
                    pair.getCallCount().incrementAndGet();
                }
                return referer;
            }
        }
        return null;
    }

    @Override
    protected void doSelectToHolder(Request request, List<Referer<T>> refersHolder) {
        // call roundrobin 临时添加RR代码
        if (autoDecrWeightLBEnableSwitcher == null || !autoDecrWeightLBEnableSwitcher.isOn()) {
            rrDoSelectToHolder(request, refersHolder);
            return;
        }

        for (int i = 0; i < MAX_REFERER_COUNT; i++) {
            Referer<T> refer = doSelect(request);
            if (refer == null) {
                break;
            }
            refersHolder.add(refer);
        }

    }

    private static void daemonUpdateWeight() {
        for (AutoDecrWeightLoadBalance lb : loadBalancesMap.keySet()) {
            for (Object serviceName : lb.methodWeightMap.keySet()) {
                Map<Referer, RefererWeightPair> refererWeightMap =
                        (Map<Referer, RefererWeightPair>) lb.methodWeightMap.get(serviceName);
                if (refererWeightMap == null) {
                    continue;
                }
                String serviceMethoName = (String) serviceName;
                String[] serviceStrArray = serviceMethoName.split("\\|");
                String method = null;
                if (serviceStrArray.length == 5) {
                    method = serviceStrArray[4];
                } else {
                    LoggerUtil.warn("AutoDecrWeightLoadBalance: {} is invalid.", serviceName);
                    continue;
                }

                List<RefererWeightPair> refererList = new ArrayList<RefererWeightPair>(refererWeightMap.values());
                for (RefererWeightPair pair : refererList) {
                    Snapshot snapshot = InternalMetricsFactory.getRegistryInstance(METRIC_RPC_AVG_KEY)
                            .histogram(BackupRequestStatsUtil.getMetricKey(pair.getReferer(), method)).getSnapshot();
                    pair.setValue(snapshot.getMean());
                }
                // 倒序
                Collections.sort(refererList, new Comparator<RefererWeightPair>() {
                    @Override
                    public int compare(RefererWeightPair o1, RefererWeightPair o2) {
                        if (o1.getValue() > o2.getValue()) {
                            return -1;
                        } else if (o1.getValue() < o2.getValue()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                });

                int count = (int) Math.floor(refererList.size() * PERCENT_LOW);
                for (RefererWeightPair pair : refererList) {
                    if (count > 0) {
                        pair.setWeight(LOW_WEIGHT);
                    } else {
                        pair.setWeight(NORMAL_WEIGHT);
                    }
                    count--;
                }
                // log
                StringBuilder sb = new StringBuilder();
                sb.append("AutoDecrWeightLoadBalance => ").append(serviceMethoName).append("#");
                for (RefererWeightPair pair : refererList) {
                    sb.append(pair.getReferer().getServiceUrl().getServerPortStr()).append("|").append(pair.getValue())
                            .append("|")
                            .append(pair.getWeight()).append("|").append(pair.getCallCount()).append("#");
                }
                LoggerUtil.info(sb.toString());
            }
        }

    }

    private String getServiceKey(Request request) {
        String separator = "|";
        StringBuilder sb = new StringBuilder();
        sb.append(request.getAttachments().get(URLParamType.application.getName())).append(separator)
                .append(request.getAttachments().get(URLParamType.module.getName())).append(separator)
                .append(request.getAttachments().get(URLParamType.clientGroup.getName())).append(separator)
                .append(request.getAttachments().get(URLParamType.version.getName())).append(separator)
                .append(MotanFrameworkUtil.getFullMethodString(request));
        return sb.toString();

    }

    // get positive int
    private int getNextIndex() {
        return 0x7fffffff & INDEX.incrementAndGet();
    }

    // get positive int
    private int getNextRound() {
        return 0x7fffffff & ROUND.incrementAndGet();
    }

    private AtomicInteger idx = new AtomicInteger(0);

    private Referer<T> rrDoSelect(Request request) {
        List<Referer<T>> referers = getReferers();

        int index = getNext();
        for (int i = 0; i < referers.size(); i++) {
            Referer<T> ref = referers.get((i + index) % referers.size());
            if (ref.isAvailable()) {
                return ref;
            }
        }
        return null;
    }

    private void rrDoSelectToHolder(Request request, List<Referer<T>> refersHolder) {
        List<Referer<T>> referers = getReferers();

        int index = getNext();
        for (int i = 0, count = 0; i < referers.size() && count < MAX_REFERER_COUNT; i++) {
            Referer<T> referer = referers.get((i + index) % referers.size());
            if (referer.isAvailable()) {
                refersHolder.add(referer);
                count++;
            }
        }
    }

    // get positive int
    private int getNext() {
        return 0x7fffffff & idx.incrementAndGet();
    }

    private static class RefererWeightPair<T> {
        private Referer<T> referer;
        private volatile int weight = NORMAL_WEIGHT;
        private double value;
        private AtomicLong callCount = new AtomicLong(0);

        public RefererWeightPair(Referer<T> referer, int weight) {
            this.referer = referer;
            this.weight = weight;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        public Referer<T> getReferer() {
            return referer;
        }

        public void setReferer(Referer<T> referer) {
            this.referer = referer;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public AtomicLong getCallCount() {
            return callCount;
        }

        public void setCallCount(AtomicLong callCount) {
            this.callCount = callCount;
        }
    }

}
