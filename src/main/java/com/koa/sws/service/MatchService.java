package com.koa.sws.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.model.MessageType;
import com.koa.sws.model.PeerSession;
import com.koa.sws.model.SignalMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

@Slf4j
@Service
@RequiredArgsConstructor
public class MatchService {

    private final SessionService sessionService;
    private final RedisQueueService queueService;
    private final ObjectMapper objectMapper;

    /**
     * 사용자 등록
     * 1. as publisher
     * 2. as subscriber
     */
    public void registerPeer(WebSocketSession session) {

        String myId = session.getId();

        // 1. register as publisher
        WebSocketSession subscriberSession = getWaitingSubscriber(session);
        if (subscriberSession != null) {
            match(myId, subscriberSession.getId());
        }

        // 2. register as subscriber
        String publisherId = getWaitingPublisher(session);
        if (publisherId == null || (subscriberSession != null && publisherId.equals(subscriberSession.getId()))) {
            queueService.addToSubscribeQueue(myId);
        } else {
            match(publisherId, myId);
        }

        if (subscriberSession == null) {
            queueService.addToPublishQueue(myId);
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
     * 메세지 중계 (OFFER, ANSWER, ICE)
     */
    public void relaySignalMessage(SignalMessage message) {

        String fromId = message.getMyId();     // 메시지 보낸 사람
        String toId = message.getTargetId();   // 전달해야 할 상대방

        if (toId == null) {
            log.warn("relay failed: targetId is null. fromId={}", fromId);
            return;
        }

        WebSocketSession targetSession = sessionService.getSession(toId);
        if (!isSessionValid(targetSession)) {
            log.warn("relay failed: target session invalid. targetId={}", toId);
            return;
        }

        // 그대로 상대에게 메시지 전달
        sendMessage(targetSession, message);
        log.info("🔁 Relayed {} from {} → {}", message.getType(), fromId, toId);
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

        WebSocketSession subscriberSession = getWaitingSubscriber(publisherSession);
        if (subscriberSession != null) {
            match(publisherId, subscriberSession.getId());
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

    private WebSocketSession getWaitingSubscriber(WebSocketSession session) {

        PeerSession peerSession = sessionService.getPeerSession(session.getId());

        String waitingSubscriberId = null;
        WebSocketSession subscriberSession = null;
        boolean check = false;
        while (queueService.getSubscribeQueueSize() > 0 && !isSessionValid(subscriberSession)) {
            waitingSubscriberId = queueService.popFromSubscribeQueue();
            subscriberSession = sessionService.getSession(waitingSubscriberId);

            if (!isPeerAvailable(waitingSubscriberId, peerSession.getPublishTo())) {
                log.warn("peer is not availble : {} <-> {}" , waitingSubscriberId, peerSession.getPublishTo());
                waitingSubscriberId = null;
                check = true;
            }
        }

        if (check) queueService.addToSubscribeQueue(peerSession.getPublishTo());

        if (waitingSubscriberId == null || subscriberSession == null) {
            log.info("❌ No waiting subscriber in subscribe queue");
            return null;
        }
        return subscriberSession;
    }

    private String getWaitingPublisher(WebSocketSession session) {

        PeerSession peerSession = sessionService.getPeerSession(session.getId());

        String waitingPublisherId = null;
        WebSocketSession publisherSession = null;
        boolean check = false;
        while (queueService.getPublishQueueSize() > 0 && !isSessionValid(publisherSession)) {
            waitingPublisherId = queueService.popFromPublishQueue();
            publisherSession = sessionService.getSession(waitingPublisherId);

            if (!isPeerAvailable(waitingPublisherId, peerSession.getSubscribeFrom())) {
                log.warn("peer is not availble : {} <-> {}" , waitingPublisherId, peerSession.getPublishTo());
                waitingPublisherId = null;
                check = true;
            }
        }

        if (check) queueService.addToPublishQueue(peerSession.getSubscribeFrom());

        if (waitingPublisherId == null || publisherSession == null) {
            log.info("❌ No publisher in publish queue");
            return null;
        }

        return waitingPublisherId;
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
        log.info("📤 Sent MATCH message) publisher: {}, subscriber: {}", publisherId, subscriberId);
    }

    public void sendMessage(WebSocketSession session, SignalMessage message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            log.error("Failed to send message type {}: {}", message.getType(), e.getMessage());
        }
    }

    private boolean isPeerAvailable(String targetPeerId, String myPeerId) {
        return targetPeerId != null && !targetPeerId.equals(myPeerId);
    }

    private boolean isSessionValid(WebSocketSession session) {
        return session != null && session.isOpen();
    }
}
