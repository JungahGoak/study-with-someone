package com.koa.sws.service;

import com.koa.sws.model.MessageType;
import com.koa.sws.model.PeerSession;
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
    private final RedisQueueService queueService;
    private final SignalMessageRelayService relayService;
    private final FindPeerService queueMatchingService;

    /**
     * 사용자 등록
     * 1. as publisher
     * 2. as subscriber
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
        // 1. Remove peer session, websocket session
        PeerSession peerSession = sessionService.remove(peerId);
        if (peerSession == null) {
            log.warn("not found user: {}", peerId);
            return;
        }

        // 2. Notice to publisher, subscriber and Rematch
        unregisterMyPublisher(peerSession.getPublisher(), peerId);
        unregisterMySubscriber(peerSession.getSubscriber(), peerId);
    }
    
    private void unregisterMyPublisher(String publisherId, String myId) {
        if (publisherId == null) {
            log.debug("No connected publisher for {}", myId);
            return;
        }

        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        PeerSession publisherPeerSession = sessionService.getPeerSession(publisherId);
        if (!sessionService.isSessionValid(publisherSession) || publisherPeerSession == null) {
            log.warn("Publisher session not found: {}", publisherId);
            return;
        }

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

        WebSocketSession subscriberSession = sessionService.getSession(subscriberId);
        PeerSession subscriberPeerSession = sessionService.getPeerSession(subscriberId);

        if (!sessionService.isSessionValid(subscriberSession) || subscriberPeerSession == null) {
            log.warn("Subscriber session not found: {}", subscriberId);
            return;
        }

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
        // 1. find Session
        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        WebSocketSession subscriberSession = sessionService.getSession(subscriberId);
        if (!sessionService.isSessionValid(subscriberSession) || !sessionService.isSessionValid(publisherSession)) {
            log.info("Session not active: {} <-> {}", subscriberId, publisherId);
            return;
        }

        // 2. send Websocket Message
        relayService.sendMessage(publisherSession, new SignalMessage(MessageType.PUBLISH, publisherId, subscriberId));
        relayService.sendMessage(subscriberSession, new SignalMessage(MessageType.SUBSCRIBE, subscriberId, publisherId));

        // 3. session info update
        sessionService.updateSubscriber(publisherId, subscriberId);
        sessionService.updatePublisher(subscriberId, publisherId);
        log.info("Peers matched - publisher: {}, subscriber: {}", publisherId, subscriberId);
    }
}