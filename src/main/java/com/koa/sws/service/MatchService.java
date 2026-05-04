package com.koa.sws.service;

import com.koa.sws.model.MessageType;
import com.koa.sws.model.PeerRelation;
import com.koa.sws.model.SignalMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
@Service
@RequiredArgsConstructor
public class MatchService {

    private final SessionService sessionService;
    private final RedisPeerService redisPeerService;
    private final RedisQueueService queueService;
    private final SignalMessageRelayService relayService;
    private final FindPeerService queueMatchingService;

    /**
     * 사용자 등록
     */
    public void registerPeer(WebSocketSession session) {
        log.info("Registering peer as publisher and subscriber - peerId: {}", session.getId());
        registerAsPublisher(session);
        registerAsSubscriber(session);
    }

    public void registerAsPublisher(WebSocketSession session) {
        String myId = session.getId();
        String subscriberId = queueMatchingService.findWaitingSubscriber(session);
        if (subscriberId == null) {
            queueService.addToPublishQueue(myId);
        } else {
            match(myId, subscriberId);
        }
    }

    public void registerAsSubscriber(WebSocketSession session) {
        String myId = session.getId();
        String publisherId = queueMatchingService.findWaitingPublisher(session);
        if (publisherId == null) {
            queueService.addToSubscribeQueue(myId);
        } else {
            match(publisherId, myId);
        }
    }

    /**
     * 사용자 해제
     */
    public void unregisterPeer(String peerId) {
        queueService.removeFromPublishQueue(peerId);
        queueService.removeFromSubscribeQueue(peerId);

        PeerRelation peerRelation = sessionService.remove(peerId);
        if (peerRelation == null) {
            log.warn("not found user: {}", peerId);
            return;
        }

        unregisterMyPublisher(peerRelation.getPublisher(), peerId);
        unregisterMySubscriber(peerRelation.getSubscriber(), peerId);
    }

    private void unregisterMyPublisher(String publisherId, String myId) {
        if (publisherId == null) {
            log.debug("No connected publisher for {}", myId);
            return;
        }

        if (!redisPeerService.isPresent(publisherId)) {
            log.warn("Publisher not found in Redis: {}", publisherId);
            return;
        }

        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        relayService.sendMessage(publisherSession, new SignalMessage(MessageType.LEAVE, publisherId, myId, "Subscriber has left session"));
        sessionService.updateSubscriber(publisherId, null);

        String subscriberId = queueMatchingService.findWaitingSubscriber(publisherSession);
        if (subscriberId != null) {
            match(publisherId, subscriberId);
        } else {
            queueService.addToPublishQueue(publisherId);
        }
    }

    private void unregisterMySubscriber(String subscriberId, String myId) {
        if (subscriberId == null) {
            log.debug("No connected subscriber for {}", myId);
            return;
        }

        if (!redisPeerService.isPresent(subscriberId)) {
            log.warn("Subscriber not found in Redis: {}", subscriberId);
            return;
        }

        WebSocketSession subscriberSession = sessionService.getSession(subscriberId);
        relayService.sendMessage(subscriberSession, new SignalMessage(MessageType.LEAVE, subscriberId, myId, "Publisher has left session"));
        sessionService.updatePublisher(subscriberId, null);

        String publisherId = queueMatchingService.findWaitingPublisher(subscriberSession);
        if (publisherId != null) {
            match(publisherId, subscriberId);
        } else {
            queueService.addToSubscribeQueue(subscriberId);
            log.info("-> after disconnected add subscriber queue: {}", subscriberId);
        }
    }

    private void match(String publisherId, String subscriberId) {
        if (!redisPeerService.isPresent(publisherId) || !redisPeerService.isPresent(subscriberId)) {
            log.info("Peer not active in Redis: {} <-> {}", publisherId, subscriberId);
            return;
        }

        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        WebSocketSession subscriberSession = sessionService.getSession(subscriberId);

        relayService.sendMessage(publisherSession, new SignalMessage(MessageType.PUBLISH, publisherId, subscriberId));
        relayService.sendMessage(subscriberSession, new SignalMessage(MessageType.SUBSCRIBE, subscriberId, publisherId));

        sessionService.updateSubscriber(publisherId, subscriberId);
        sessionService.updatePublisher(subscriberId, publisherId);
        log.info("Peers matched - publisher: {}, subscriber: {}", publisherId, subscriberId);
    }
}