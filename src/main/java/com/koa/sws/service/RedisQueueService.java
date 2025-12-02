package com.koa.sws.service;

import com.koa.sws.aop.DistributedLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisQueueService {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String PUBLISH_QUEUE = "sws:publishQueue";
    private static final String SUBSCRIBE_QUEUE = "sws:subscribeQueue";

    public void addToPublishQueue(String peerId) {
        log.debug("🔫 ADD Publisher queue: {}", peerId);
        enqueue(getPublishQueueName(), peerId);
    }

    public void addToSubscribeQueue(String peerId) {
        log.debug("🔫 ADD Subscriber queue: {}", peerId);
        enqueue(getSubscribeQueueName(), peerId);
    }

    @DistributedLock(key = "'publishQueue'")
    public String popFromPublishQueue() {
        String peerId = dequeue(getPublishQueueName());
        log.debug("🦷 POP Publisher queue: {}", peerId);
        return peerId;
    }

    @DistributedLock(key = "'subscribeQueue'")
    public String popFromSubscribeQueue() {
        String peerId = dequeue(getSubscribeQueueName());
        log.debug("🦷 POP Subscriber queue: {}", peerId);
        return peerId;
    }

    public Long getPublishQueueSize() {
        return getQueueSize(getPublishQueueName());
    }

    public Long getSubscribeQueueSize() {
        return getQueueSize(getSubscribeQueueName());
    }

    private Long getQueueSize(String queue) {
        return redisTemplate.opsForList().size(queue);
    }

    private void enqueue(String queue, String peerId) {
        redisTemplate.opsForList().rightPush(queue, peerId);
    }

    private String dequeue(String queue) {
        return redisTemplate.opsForList().leftPop(queue);
    }

    private String getPublishQueueName() {
        return PUBLISH_QUEUE;
    }

    private String getSubscribeQueueName() {
        return SUBSCRIBE_QUEUE;
    }
}