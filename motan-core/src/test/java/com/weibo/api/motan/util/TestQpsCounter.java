/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.util;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import com.codahale.metrics.Counter;

/**
 * @author leijian
 */
public class TestQpsCounter {
    public static Queue<String> q = new LinkedBlockingQueue<String>();

    public static Counter pendingJobs;

    public static Random random = new Random();

    public static void addJob(String job) {
        pendingJobs.inc();
        q.offer(job);
    }

    public static String takeJob() {
        pendingJobs.dec();
        return q.poll();
    }

    public static void main(String[] args) throws InterruptedException {
        final QpsCounter qpsCounter = new QpsCounter();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    long timestamp = System.currentTimeMillis();
                    System.out.println("metric=" + qpsCounter.getQps(timestamp) + "\t" + qpsCounter.getQps(timestamp,
                            0.9) + "\t" +
                            qpsCounter.getMean(timestamp));
                }
            }
        }).start();
        int i = 0;
        while (true) {
            int curValue = (int) (System.currentTimeMillis() / 1000 % 10);
            int t = 0;
            if (curValue >= 8) {
                t = curValue + (new Random().nextInt(20));
            } else {
                t = (new Random().nextInt(6));
            }

            System.out.println("qps=" + i++ + "\t" + t);
            qpsCounter.incr(System.currentTimeMillis(), t);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
