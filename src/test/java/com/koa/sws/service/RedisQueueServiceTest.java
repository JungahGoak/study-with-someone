package com.koa.sws.service;

import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class RedisQueueServiceTest {

    @Autowired
    private RedisQueueService queueService;

    @Autowired
    private RedissonClient redissonClient;

    @Test
    void distributedLock_shouldHandleHighConcurrencyPop() throws Exception {

        // 큐 초기화
        redissonClient.getKeys().delete("sws:publishQueue");

        int TOTAL = 1000;
        for (int i = 1; i <= TOTAL; i++) {
            queueService.addToPublishQueue("USER-" + i);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(TOTAL);
        CountDownLatch latch = new CountDownLatch(TOTAL);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < TOTAL; i++) {
            futures.add(executorService.submit(() -> {
                latch.countDown(); // 동시 출발
                latch.await();
                return queueService.popFromPublishQueue();
            }));
        }

        Set<String> results = new HashSet<>();
        for (Future<String> f : futures) {
            String result = f.get();
            if (result != null) {
                results.add(result);
            }
        }

        assertThat(results.size()).isEqualTo(TOTAL);
        assertThat(queueService.getPublishQueueSize()).isZero();
    }
}