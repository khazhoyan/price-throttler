package com.price.processor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Currency;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class PriceThrottlerTest {
    
    private final PriceThrottler throttler = new PriceThrottler();

    private final TestProcessor subscriberSlow = new TestProcessor(1000);
    private final TestProcessor subscriberFast = new TestProcessor(10);

    private final CurrencyPair usdEur = new CurrencyPair(Currency.getInstance("USD"), Currency.getInstance("EUR"));

    @AfterEach
    void cleanup() {
        throttler.shutdown();
    }

    @Test
    void should_discard_slow_tasks_and_succeed_in_less_time_than_slow_tasks_combined() throws Exception {
        // given
        throttler.subscribe(subscriberSlow);
        int nRequests = 1000;

        // when
        for (int i = 0; i < nRequests; i++) throttler.onPrice(usdEur, i);

        // then
        var start = Instant.now();
        waitUntil(() -> !subscriberSlow.result.isEmpty() && subscriberSlow.result.get(subscriberSlow.result.size() - 1) == 999.0d);
        var duration = Duration.between(start, Instant.now());

        var durationIfNotSkippingTasks = Duration.ofMillis(subscriberSlow.millisToRun * nRequests / 4);
        assertTrue(duration.compareTo(durationIfNotSkippingTasks) < 0,
                "should succeed more quickly than 1000 slow tasks running in parallel on 4 cores");
        assertTrue(subscriberSlow.result.size() < 10,
                "should discard most of the slow tasks");
    }

    @Test
    void should_isolate_fast_subscribers_from_slow() throws Exception {
        // given
        throttler.subscribe(subscriberFast);
        throttler.onPrice(usdEur, 1.0d);
        waitUntil(() -> throttler.isFast(subscriberFast));  // warmup

        var superSlow = new TestProcessor(10_000);
        throttler.subscribe(superSlow);
        int nRequests = 1000;

        // when
        for (int i = 0; i < nRequests; i++) throttler.onPrice(usdEur, i);

        // then
        var start = Instant.now();
        waitUntil(() -> !subscriberFast.result.isEmpty() && subscriberFast.result.get(subscriberFast.result.size() - 1) == 999.0d);
        var duration = Duration.between(start, Instant.now());

        assertTrue(duration.compareTo(Duration.ofMillis(superSlow.millisToRun)) < 0,
                "should process 1000 fast tasks more quickly than 1 slow");
    }

    @Test
    void should_mark_fast_subscriber_as_fast() throws Exception {
        // given
        throttler.subscribe(subscriberFast);

        // when
        throttler.onPrice(usdEur, 1.0d);

        // then
        waitUntil(() -> subscriberFast.result.contains(1.0d));
        assertTrue(throttler.isFast(subscriberFast));
        assertFalse(throttler.isSlow(subscriberFast));
    }

    @Test
    void should_not_mark_slow_subscriber_as_fast() throws Exception {
        // given
        throttler.subscribe(subscriberSlow);

        // when
        throttler.onPrice(usdEur, 1.0d);

        // then
        waitUntil(() -> subscriberSlow.result.contains(1.0d));
        assertFalse(throttler.isFast(subscriberSlow));
        assertTrue(throttler.isSlow(subscriberSlow));
    }

    private static void waitUntil(Supplier<Boolean> probe) throws InterruptedException {
        int attempts = 0;
        while (attempts < 200) {
            if (probe.get()) return;
            Thread.sleep(50);
            attempts++;
        }
        fail("Condition does not hold");
    }

    private static class TestProcessor implements PriceProcessor {
        final long millisToRun;
        final List<Double> result;

        public TestProcessor(long millisToRun) {
            this.millisToRun = millisToRun;
            this.result = new CopyOnWriteArrayList<>();
        }

        @Override
        public void onPrice(CurrencyPair ccyPair, double rate) {
            try {
                Thread.sleep(millisToRun);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            result.add(rate);
        }

        @Override
        public void subscribe(PriceProcessor priceProcessor) {}

        @Override
        public void unsubscribe(PriceProcessor priceProcessor) {}
    }
}
