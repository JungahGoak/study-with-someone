package com.koa.sws.service;

import com.koa.sws.model.PeerSession;

import java.util.function.Function;

public enum QueueType {
    PUBLISHER(
            RedisQueueService::popFromPublishQueue,
            RedisQueueService::addToPublishQueue,
            RedisQueueService::getPublishQueueSize,
            PeerSession::getSubscriber
    ),
    SUBSCRIBER(
            RedisQueueService::popFromSubscribeQueue,
            RedisQueueService::addToSubscribeQueue,
            RedisQueueService::getSubscribeQueueSize,
            PeerSession::getPublisher
    );

    private final Function<RedisQueueService, String> popFunction;
    private final java.util.function.BiConsumer<RedisQueueService, String> addFunction;
    private final Function<RedisQueueService, Long> sizeFunction;
    private final Function<PeerSession, String> peerGetter;

    QueueType(Function<RedisQueueService, String> popFunction,
              java.util.function.BiConsumer<RedisQueueService, String> addFunction,
              Function<RedisQueueService, Long> sizeFunction,
              Function<PeerSession, String> peerGetter) {
        this.popFunction = popFunction;
        this.addFunction = addFunction;
        this.sizeFunction = sizeFunction;
        this.peerGetter = peerGetter;
    }

    public String pop(RedisQueueService queueService) {
        return popFunction.apply(queueService);
    }

    public void add(RedisQueueService queueService, String peerId) {
        addFunction.accept(queueService, peerId);
    }

    public Long getSize(RedisQueueService queueService) {
        return sizeFunction.apply(queueService);
    }

    public String getConnectedPeer(PeerSession peerSession) {
        return peerGetter.apply(peerSession);
    }
}