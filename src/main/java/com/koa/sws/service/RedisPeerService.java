package com.koa.sws.service;

import com.koa.sws.constant.RedisKeyConstants;
import com.koa.sws.model.PeerRelation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * peer의 존재 여부와 publisher/subscriber 관계를 Redis에서 관리
 *
 * Redis key 구조:
 *   sws:peer:{peerId}            → presence ("1"), TTL 24h
 *   sws:peer:{peerId}:publisher  → publisherId
 *   sws:peer:{peerId}:subscriber → subscriberId
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisPeerService {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String PEER_PREFIX = RedisKeyConstants.PEER_PREFIX;
    private static final String PUBLISHER_SUFFIX = RedisKeyConstants.PUBLISHER_SUFFIX;
    private static final String SUBSCRIBER_SUFFIX = RedisKeyConstants.SUBSCRIBER_SUFFIX;

    public void register(String peerId) {
        redisTemplate.opsForValue().set(PEER_PREFIX + peerId, "1", RedisKeyConstants.PEER_SESSION_TTL);
        log.debug("Registered peer in Redis: {}", peerId);
    }

    public void remove(String peerId) {
        redisTemplate.delete(PEER_PREFIX + peerId);
        redisTemplate.delete(PEER_PREFIX + peerId + PUBLISHER_SUFFIX);
        redisTemplate.delete(PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX);
        log.debug("Removed peer from Redis: {}", peerId);
    }

    public void updatePublisher(String peerId, String publisherId) {
        String key = PEER_PREFIX + peerId + PUBLISHER_SUFFIX;
        if (publisherId == null) {
            redisTemplate.delete(key);
        } else {
            redisTemplate.opsForValue().set(key, publisherId, RedisKeyConstants.PEER_SESSION_TTL);
        }
    }

    public void updateSubscriber(String peerId, String subscriberId) {
        String key = PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX;
        if (subscriberId == null) {
            redisTemplate.delete(key);
        } else {
            redisTemplate.opsForValue().set(key, subscriberId, RedisKeyConstants.PEER_SESSION_TTL);
        }
    }

    public PeerRelation getPeerRelation(String peerId) {
        if (!isPresent(peerId)) return null;

        PeerRelation relation = new PeerRelation(peerId);
        relation.setPublisher(redisTemplate.opsForValue().get(PEER_PREFIX + peerId + PUBLISHER_SUFFIX));
        relation.setSubscriber(redisTemplate.opsForValue().get(PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX));
        return relation;
    }

    public boolean isPresent(String peerId) {
        if (peerId == null) return false;
        return Boolean.TRUE.equals(redisTemplate.hasKey(PEER_PREFIX + peerId));
    }
}