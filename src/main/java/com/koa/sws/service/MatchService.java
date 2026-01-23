package com.koa.sws.service;

import com.koa.sws.model.MessageType;
import com.koa.sws.model.PeerSession;
import com.koa.sws.model.QueueType;
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

        String subscriberId = getWaitingSubscriber(session);
        if (subscriberId == null) {
            queueService.addToPublishQueue(myId);
        } else {
            match(myId, subscriberId);
        }
    }

    public void registerAsSubscriber(WebSocketSession session) {

        String myId = session.getId();

        String publisherId = getWaitingPublisher(session);
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
        unregisterMyPublisher(peerSession.getPublishTo(), peerId);
        unregisterMySubscriber(peerSession.getSubscribeFrom(), peerId);

        // 3. Remove mine in queue -> X (when pop, check the session avaible)
    }

    /**
     * 메시지 중계 (OFFER, ANSWER, ICE)
     * Delegates to SignalMessageRelayService
     */
    public void relaySignalMessage(SignalMessage message) {
        relayService.relaySignalMessage(message);
    }

    /**
     * WebSocket 메시지 전송
     * Delegates to SignalMessageRelayService
     */
    public void sendMessage(WebSocketSession session, SignalMessage message) {
        relayService.sendMessage(session, message);
    }

    //
    private void unregisterMyPublisher(String publisherId, String myId) {
        if (publisherId == null) {
            log.debug("No connected publisher for {}", myId);
            return;
        }

        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        PeerSession publisherPeerSession = sessionService.getPeerSession(publisherId);
        if (!isSessionValid(publisherSession) || publisherPeerSession == null) {
            log.warn("Publisher session not found: {}", publisherId);
            return;
        }

        sendMessage(publisherSession, new SignalMessage(MessageType.LEAVE, publisherId, myId, "Subscriber has left session"));
        sessionService.updateSubscriber(publisherId, null);

        String subscriberId = getWaitingSubscriber(publisherSession);
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

        if (!isSessionValid(subscriberSession) || subscriberPeerSession == null) {
            log.warn("Subscriber session not found: {}", subscriberId);
            return;
        }

        sendMessage(subscriberSession, new SignalMessage(MessageType.LEAVE, subscriberId, myId, "Subscriber has left session"));
        sessionService.updatePublisher(subscriberId, null);

        String publisherId = getWaitingPublisher(subscriberSession);
        if (publisherId != null) {
            match(publisherId, subscriberId);
        } else {
            queueService.addToSubscribeQueue(subscriberId);
            log.info("-> after disconnected add subscriber queue: {}", myId);
        }
    }

    private String getWaitingSubscriber(WebSocketSession session) {
        WebSocketSession subscriberSession = getWaitingPeer(session, QueueType.SUBSCRIBER);
        return subscriberSession != null ? subscriberSession.getId() : null;
    }

    private String getWaitingPublisher(WebSocketSession session) {
        WebSocketSession publisherSession = getWaitingPeer(session, QueueType.PUBLISHER);
        return publisherSession != null ? publisherSession.getId() : null;
    }

    /**
     * 공통 큐 처리 로직
     * @param session 현재 세션
     * @param queueType 찾을 피어의 큐 타입 (PUBLISHER 또는 SUBSCRIBER)
     * @return 매칭 가능한 피어의 세션, 없으면 null
     */
    private WebSocketSession getWaitingPeer(WebSocketSession session, QueueType queueType) {
        PeerSession myPeerSession = sessionService.getPeerSession(session.getId());
        String myId = session.getId();

        String candidateId = null;
        WebSocketSession candidateSession = null;
        String peerToRestore = null;
        boolean needRestoreMine = false;

        long maxRetries = queueType.getSize(queueService);
        long attempts = 0;

        while (queueType.getSize(queueService) > 0 && !isSessionValid(candidateSession) && attempts < maxRetries) {
            candidateId = queueType.pop(queueService);
            candidateSession = sessionService.getSession(candidateId);
            attempts++;

            // Validation 1: Cannot be connected to my existing peer (prevent circular dependency)
            String myConnectedPeer = queueType.getConnectedPeer(myPeerSession);
            if (myConnectedPeer != null && !isPeerAvailable(candidateId, myConnectedPeer)) {
                log.warn("Peer not available due to circular dependency - candidate: {}, myConnectedPeer: {}",
                        candidateId, myConnectedPeer);
                peerToRestore = candidateId;
                candidateId = null;
                candidateSession = null;
                continue;
            }

            // Validation 2: Cannot be myself (prevent self-matching)
            if (candidateId != null && candidateId.equals(myId)) {
                log.debug("Skipping self-match - peerId: {}", candidateId);
                needRestoreMine = true;
                candidateId = null;
                candidateSession = null;
                continue;
            }

            // If session is invalid, continue to next candidate
            if (!isSessionValid(candidateSession)) {
                log.debug("Invalid session found in queue - peerId: {}", candidateId);
                candidateId = null;
                candidateSession = null;
            }
        }

        // Restore peers back to queue if needed
        if (peerToRestore != null) {
            queueType.add(queueService, peerToRestore);
        }
        if (needRestoreMine) {
            queueType.add(queueService, myId);
        }

        if (candidateId == null || candidateSession == null) {
            log.debug("No waiting peer in {} queue", queueType);
            return null;
        }

        return candidateSession;
    }

    private void match(String publisherId, String subscriberId) {

        // 1. find Session
        WebSocketSession publisherSession = sessionService.getSession(publisherId);
        WebSocketSession subscriberSession = sessionService.getSession(subscriberId);
        if (!isSessionValid(subscriberSession) || !isSessionValid(publisherSession)) {
            log.info("Session not active: {} <-> {}", subscriberId, publisherId);
            return;
        }

        // 2. send Websocket Message
        sendMessage(publisherSession, new SignalMessage(MessageType.PUBLISH, publisherId, subscriberId));
        sendMessage(subscriberSession, new SignalMessage(MessageType.SUBSCRIBE, subscriberId, publisherId));

        // 3. session info update
        sessionService.updateSubscriber(publisherId, subscriberId);
        sessionService.updatePublisher(subscriberId, publisherId);
        log.info("Peers matched - publisher: {}, subscriber: {}", publisherId, subscriberId);
    }

    private boolean isPeerAvailable(String targetPeerId, String myPeerId) {
        return targetPeerId != null && myPeerId != null && !targetPeerId.equals(myPeerId);
    }

    private boolean isSessionValid(WebSocketSession session) {
        return session != null && session.isOpen();
    }
}