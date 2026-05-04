package com.koa.sws.service;

import com.koa.sws.constant.RedisKeyConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
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

    public void removeFromPublishQueue(String peerId) {
        log.info("Removing from publisher queue - peerId: {}", peerId);
        remove(RedisKeyConstants.PUBLISH_QUEUE, peerId);
    }

    public void removeFromSubscribeQueue(String peerId) {
        log.info("Removing from subscriber queue - peerId: {}", peerId);
        remove(RedisKeyConstants.SUBSCRIBE_QUEUE, peerId);
    }

    public String popFromPublishQueue() {
        String peerId = pop(RedisKeyConstants.PUBLISH_QUEUE);
        log.info("Popped from publisher queue - peerId: {}", peerId);
        return peerId;
    }

    public String popFromSubscribeQueue() {
        String peerId = pop(RedisKeyConstants.SUBSCRIBE_QUEUE);
        log.info("Popped from subscriber queue - peerId: {}", peerId);
        return peerId;
    }

    public Long getPublishQueueSize() {
        return getQueueSize(RedisKeyConstants.PUBLISH_QUEUE);
    }

    public Long getSubscribeQueueSize() {
        return getQueueSize(RedisKeyConstants.SUBSCRIBE_QUEUE);
    }

    private void enqueue(String queue, String peerId) {
        redisTemplate.opsForZSet().add(queue, peerId, System.currentTimeMillis());
    }

    private String pop(String queue) {
        ZSetOperations.TypedTuple<String> tuple = redisTemplate.opsForZSet().popMin(queue);
        return tuple != null ? tuple.getValue() : null;
    }

    private void remove(String queue, String peerId) {
        redisTemplate.opsForZSet().remove(queue, peerId);
    }

    private Long getQueueSize(String queue) {
        Long size = redisTemplate.opsForZSet().zCard(queue);
        return size != null ? size : 0L;
    }
}