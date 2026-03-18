package com.koa.sws.service;

import com.koa.sws.aop.DistributedLock;
import com.koa.sws.constant.RedisKeyConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisQueueService {

    private final RedisTemplate<String, String> redisTemplate;

    public void addToPublishQueue(String peerId) {
        log.info("Adding to publisher queue - peerId: {}", peerId);
        enqueue(RedisKeyConstants.PUBLISH_QUEUE, peerId);
    }

    public void addToSubscribeQueue(String peerId) {
        log.info("Adding to subscriber queue - peerId: {}", peerId);
        enqueue(RedisKeyConstants.SUBSCRIBE_QUEUE, peerId);
    }

    @DistributedLock(key = "'publishQueue'")
    public String popFromPublishQueue() {
        String peerId = dequeue(RedisKeyConstants.PUBLISH_QUEUE);
        log.info("Popped from publisher queue - peerId: {}", peerId);
        return peerId;
    }

    @DistributedLock(key = "'subscribeQueue'")
    public String popFromSubscribeQueue() {
        String peerId = dequeue(RedisKeyConstants.SUBSCRIBE_QUEUE);
        log.info("Popped from subscriber queue - peerId: {}", peerId);
        return peerId;
    }

    public Long getPublishQueueSize() {
        return getQueueSize(RedisKeyConstants.PUBLISH_QUEUE);
    }

    public Long getSubscribeQueueSize() {
        return getQueueSize(RedisKeyConstants.SUBSCRIBE_QUEUE);
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
}