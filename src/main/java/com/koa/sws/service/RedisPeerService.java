package com.koa.sws.service;

import com.koa.sws.constant.RedisKeyConstants;
import com.koa.sws.model.PeerRelation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

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

    // peer 키 3개 삭제 + 두 큐에서 제거 + 파트너의 stale 참조 삭제를 원자적으로 실행
    // KEYS[1]=peer, KEYS[2]=peer:publisher, KEYS[3]=peer:subscriber, KEYS[4]=publishQueue, KEYS[5]=subscribeQueue
    // ARGV[1]=peerId, ARGV[2]=publisher의 :subscriber 키(없으면 ""), ARGV[3]=subscriber의 :publisher 키(없으면 "")
    private static final RedisScript<Long> REMOVE_PEER_AND_DEQUEUE = RedisScript.of(
            """
            redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
            redis.call('ZREM', KEYS[4], ARGV[1])
            redis.call('ZREM', KEYS[5], ARGV[1])
            if ARGV[2] ~= '' then redis.call('DEL', ARGV[2]) end
            if ARGV[3] ~= '' then redis.call('DEL', ARGV[3]) end
            return 1
            """,
            Long.class
    );

    public void register(String peerId) {
        redisTemplate.opsForValue().set(PEER_PREFIX + peerId, "1", RedisKeyConstants.PEER_SESSION_TTL);
        log.debug("Registered peer in Redis: {}", peerId);
    }

    public void remove(String peerId, String publisherId, String subscriberId) {
        redisTemplate.execute(
                REMOVE_PEER_AND_DEQUEUE,
                List.of(
                        PEER_PREFIX + peerId,
                        PEER_PREFIX + peerId + PUBLISHER_SUFFIX,
                        PEER_PREFIX + peerId + SUBSCRIBER_SUFFIX,
                        RedisKeyConstants.PUBLISH_QUEUE,
                        RedisKeyConstants.SUBSCRIBE_QUEUE
                ),
                peerId,
                publisherId != null ? PEER_PREFIX + publisherId + SUBSCRIBER_SUFFIX : "",
                subscriberId != null ? PEER_PREFIX + subscriberId + PUBLISHER_SUFFIX : ""
        );
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