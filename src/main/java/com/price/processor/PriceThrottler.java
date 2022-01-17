package com.price.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public final class PriceThrottler implements PriceProcessor {

    private final static Duration SLOW_SUBSCRIBER_THRESHOLD = Duration.ofMillis(100);

    private final List<PriceProcessor> fastSubscribers;
    private final List<PriceProcessor> slowSubscribers;
    private final ExecutorService lightweightExecutor;
    private final ExecutorService heavyExecutor;
    private final AtomicInteger version;
    private final ConcurrentMap<CurrencyPair, Integer> registry;

    public PriceThrottler() {
        this.fastSubscribers = new CopyOnWriteArrayList<>();
        this.slowSubscribers = new CopyOnWriteArrayList<>();
        this.lightweightExecutor = Executors.newFixedThreadPool(2);
        this.heavyExecutor = Executors.newFixedThreadPool(4);
        this.version = new AtomicInteger();
        this.registry = new ConcurrentHashMap<>();
    }

    public void shutdown() {
        lightweightExecutor.shutdown();
        heavyExecutor.shutdown();
    }

    @Override
    public void onPrice(CurrencyPair ccyPair, double rate) {
        // Fast and slow subscribers are being handled by two different thread pools
        // That way the fast subscribers are not affected by the slow ones
        fastSubscribers.forEach(subscriber ->
            lightweightExecutor.submit(() -> subscriber.onPrice(ccyPair, rate)));

        final int version = this.version.incrementAndGet();
        registry.put(ccyPair, version);

        slowSubscribers.forEach(subscriber ->
            heavyExecutor.submit(() -> {
                if (registry.get(ccyPair) != version)
                    // New rate was submitted while this task was waiting for computation resources
                    // Therefore this task is redundant and we skip it. See requirement #5
                    return;

                final var start = Instant.now();
                subscriber.onPrice(ccyPair, rate);
                final var end = Instant.now();

                // If the subscriber is fast, we move it to the unmanaged pool of fast subscribers
                // Here we have an assumption that subscribers' performance is consistent
                if (Duration.between(start, end).compareTo(SLOW_SUBSCRIBER_THRESHOLD) <= 0) {
                    slowSubscribers.remove(subscriber);
                    fastSubscribers.add(subscriber);
                }
            }));
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        slowSubscribers.add(priceProcessor);
    }

    @Override
    public void unsubscribe(PriceProcessor priceProcessor) {
        fastSubscribers.remove(priceProcessor);
        slowSubscribers.remove(priceProcessor);
    }

    // Visible for testing only
    boolean isFast(PriceProcessor subscriber) {
        return fastSubscribers.contains(subscriber);
    }

    // Visible for testing only
    boolean isSlow(PriceProcessor subscriber) {
        return slowSubscribers.contains(subscriber);
    }

}
