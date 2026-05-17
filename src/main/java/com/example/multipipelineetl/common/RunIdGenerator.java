package com.example.multipipelineetl.common;

import java.util.concurrent.atomic.AtomicLong;

public final class RunIdGenerator {
    private static final AtomicLong COUNTER = new AtomicLong(System.currentTimeMillis());

    private RunIdGenerator() {
    }

    public static long nextRunId() {
        return COUNTER.incrementAndGet();
    }
}

