package com.koa.sws.service;

import com.koa.sws.constant.RedisKeyConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisQueueService {

    private final RedisTemplate<String, String> redisTemplate;

    // peer 키가 존재할 때만 큐에 추가 — EXISTS + ZADD를 원자적으로 실행해 ghost 항목 방지
    // KEYS[1]=sws:peer:{peerId}, KEYS[2]=queue, ARGV[1]=score, ARGV[2]=peerId
    private static final RedisScript<Long> ENQUEUE_IF_PEER_EXISTS = RedisScript.of(
            """
            if redis.call('EXISTS', KEYS[1]) == 1 then
                redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
                return 1
            end
            return 0
            """,
            Long.class
    );

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
        String peerKey = RedisKeyConstants.PEER_PREFIX + peerId;
        Long result = redisTemplate.execute(
                ENQUEUE_IF_PEER_EXISTS,
                List.of(peerKey, queue),
                String.valueOf(System.currentTimeMillis()),
                peerId
        );
        if (result == null || result == 0L) {
            log.warn("Skipped enqueue - peer key not found: {}", peerId);
        }
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