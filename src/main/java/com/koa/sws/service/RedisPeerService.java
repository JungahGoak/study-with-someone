package com.koa.sws.service;

import com.koa.sws.model.PeerSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * peer의 존재 여부와 publisher/subscriber 관계를 공유
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

    private static final String PEER_PREFIX = "sws:peer:";
    private static final String PUBLISHER_SUFFIX = ":publisher";
    private static final String SUBSCRIBER_SUFFIX = ":subscriber";
    private static final Duration SESSION_TTL = Duration.ofHours(24);

    public void register(String peerId) {
        redisTemplate.opsForValue().set(PEER_PREFIX + peerId, "1", SESSION_TTL);
        log.debug("Registered peer presence in Redis: {}", peerId);
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
            redisTemplate.opsForValue().set(key, publisherId, SESSION_TTL);
        }
    }

    public void updateSubscriber(String peerId, String subscriberId) {
        String key = PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX;
        if (subscriberId == null) {
            redisTemplate.delete(key);
        } else {
            redisTemplate.opsForValue().set(key, subscriberId, SESSION_TTL);
        }
    }

    public PeerSession getPeerSession(String peerId) {
        if (!isPresent(peerId)) return null;

        PeerSession peerSession = new PeerSession(peerId);
        peerSession.setPublisher(redisTemplate.opsForValue().get(PEER_PREFIX + peerId + PUBLISHER_SUFFIX));
        peerSession.setSubscriber(redisTemplate.opsForValue().get(PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX));
        return peerSession;
    }

    public boolean isPresent(String peerId) {
        if (peerId == null) return false;
        return Boolean.TRUE.equals(redisTemplate.hasKey(PEER_PREFIX + peerId));
    }
}