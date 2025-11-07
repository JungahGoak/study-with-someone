package com.koa.sws.service;


import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RedisQueueService {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String PUBLISH_QUEUE = "sws:publishQueue";
    private static final String SUBSCRIBE_QUEUE = "sws:subscribeQueue";

    public void addToPublishQueue(String peerId) {
        enqueue(getPublishQueueName(), peerId);
    }

    public void addToSubscribeQueue(String peerId) {
        enqueue(getSubscribeQueueName(), peerId);
    }

    public String popFromPublishQueue() {
        return dequeue(getPublishQueueName());
    }

    public String popFromSubscribeQueue() {
        return dequeue(getSubscribeQueueName());
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