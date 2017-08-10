/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.util.atomic;

/**
 * leijian copy from guava19
 * Abstract interface for objects that can concurrently add longs.
 *
 * @author Louis Wasserman
 */
public interface LongAddable {
    void increment();

    void add(long x);

    long sum();
}
