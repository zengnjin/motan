/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.util;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import com.weibo.api.motan.util.atomic.LongAdder;

/**
 * 采用指数移动平均值算法计算最近一段时间（sampleSeconds,默认10s）的流量来作为当前时间点qps，具体思路参考unix load average估算方法
 * <br/>有较好的时间和空间性能：采用无锁的LongAddr，借鉴RingBuffer数据结构
 *
 * @author leijian
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How
 * It Works</a>
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not
 * Your Average Average</a>
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class QpsCounter {
    private final static int DEFAULT_SAMPLE_SECONDS = 10;
    private final static double DEFAULT_DECAY_RATIO = 0.8;
    private int sampleSeconds;
    private int storeSampleNum;
    private List<LongAdder> counters = Lists.newArrayList();
    private AtomicLong lastRestIndex = new AtomicLong(System.currentTimeMillis() / 1000);
    private ReentrantLock lock = new ReentrantLock();

    public QpsCounter() {
        this(DEFAULT_SAMPLE_SECONDS);
    }

    public QpsCounter(int sampleSeconds) {
        this.sampleSeconds = sampleSeconds;
        this.storeSampleNum = sampleSeconds * 8;
        for (int i = 0; i < storeSampleNum; i++) {
            counters.add(new LongAdder());
        }
    }

    public void incr(long timeInMills, long incrValue) {
        resetIfNecessary();
        long timeInSeconds = timeInMills / 1000;
        int index = (int) (timeInSeconds % storeSampleNum);
        counters.get(index).add(incrValue);
    }

    public double getQps(long timeInMillis) {
        return getQps(timeInMillis, DEFAULT_DECAY_RATIO);
    }

    /**
     * 获取timeInMillis过去10s内的综合qps
     *
     * @param timeInMillis 时间戳
     * @param decayRatio   计算时间戳时的衰退系数，取值范围[0,1]，值越大qps随着时间衰退的越快
     *
     * @return qps
     */
    public double getQps(long timeInMillis, double decayRatio) {
        resetIfNecessary();
        checkGetTime(timeInMillis);
        long timeInSeconds = timeInMillis / 1000;
        long from = timeInSeconds - sampleSeconds;
        long to = timeInSeconds - 1;
        double qps = 0;
        for (long i = from; i <= to; i++) {
            int index = (int) (i % storeSampleNum);
            long counterValue = counters.get(index).sum();
            qps = counterValue * decayRatio + qps * (1 - decayRatio);
        }
        return qps;
    }

    /**
     * 获取timeInMillis所在秒的瞬间qps
     *
     * @param timeInMillis 时间戳
     *
     * @return qps
     */
    public long getInstantQps(long timeInMillis) {
        resetIfNecessary();
        checkGetTime(timeInMillis);
        long timeInSeconds = timeInMillis / 1000;
        int index = (int) (timeInSeconds % storeSampleNum);
        return counters.get(index).sum();
    }

    /**
     * 获取指定的timeInMillis时刻，过去10s内的qps均值
     *
     * @param timeInMillis 时间戳
     *
     * @return 平均值
     */
    public double getMean(long timeInMillis) {
        long total = getTotal(timeInMillis);
        return 1.0 * total / sampleSeconds;
    }

    /**
     * 获取指定的timeInMillis时刻，过去10s内的qps总和
     *
     * @param timeInMillis 时间戳
     *
     * @return qps总和
     */
    public long getTotal(long timeInMillis) {
        resetIfNecessary();
        checkGetTime(timeInMillis);
        long timeInSeconds = timeInMillis / 1000;
        long from = timeInSeconds - sampleSeconds;
        long to = timeInSeconds - 1;
        long qps = 0;
        for (long i = from; i <= to; i++) {
            int index = (int) (i % storeSampleNum);
            long counterValue = counters.get(index).sum();
            qps += counterValue;
        }
        return qps;
    }

    public void resetIfNecessary() {
        long nowSecond = System.currentTimeMillis() / 1000;
        long oldLastResetIndexValue = lastRestIndex.get();
        if (nowSecond - oldLastResetIndexValue > sampleSeconds * 3) {
            long from = oldLastResetIndexValue + 1;
            long to = nowSecond - 2 * sampleSeconds - 1;
            long delta = to - from;
            if (delta > 0) {
                // 当reset动作，回环到当前点nowSecond时，需要加锁，避免数据冲突
                if (nowSecond - from >= storeSampleNum - sampleSeconds) {
                    try {
                        lock.lock();
                        reset(oldLastResetIndexValue, from, to);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    reset(oldLastResetIndexValue, from, to);
                }
            }
        }
    }

    private void reset(long oldLastResetIndexValue, long from, long to) {
        if (lastRestIndex.compareAndSet(oldLastResetIndexValue, to)) {
            long realTo = Math.min(to, from + this.storeSampleNum);
            for (long i = from; i <= realTo; i++) {
                int index = (int) (i % storeSampleNum);
                counters.get(index).reset();
            }
        }
    }

    /**
     * 超过10s时，返回的结果可能会不准确
     *
     * @param timeInMillis 获取数据的时间点
     */
    private void checkGetTime(long timeInMillis) {
        long delta = System.currentTimeMillis() - timeInMillis;
        String message = String.format("QpsCounter timeInMillis is out of range, and gotted value may be lower than "
                + "the actual value: timeInMillis=%s, delta=%s", timeInMillis, delta);
        if (delta < 0 || delta > (sampleSeconds - 3) * 1000) {
            LoggerUtil.warn(message);
        }
    }
}
