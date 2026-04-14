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
    private final RedisPeerService redisPeerService;
    private final RedisQueueService queueService;
    private final SignalRelayService relayService;

    /**
     * 사용자 등록
     */
    public void registerPeer(WebSocketSession session) {
        sessionService.register(session);

        log.debug("🔗 Register as Publisher");
        registerAsPublisher(session);

        log.debug("🔗 Register as Subscriber");
        registerAsSubscriber(session);
    }

    public void registerAsPublisher(WebSocketSession session) {
        String myId = session.getId();
        String subscriberId = getWaitingSubscriberId(myId);
        if (subscriberId == null) {
            queueService.addToPublishQueue(myId);
        } else {
            match(myId, subscriberId);
        }
    }

    public void registerAsSubscriber(WebSocketSession session) {
        String myId = session.getId();
        String publisherId = getWaitingPublisherId(myId);
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

    // --

    private void unregisterMyPublisher(String publisherId, String myId) {
        if (publisherId == null) {
            log.debug("No connected publisher for {}", myId);
            return;
        }

        if (!redisPeerService.isPresent(publisherId) || redisPeerService.getPeerSession(publisherId) == null) {
            log.warn("Publisher session not found: {}", publisherId);
            return;
        }

        relayService.sendToPeer(publisherId, new SignalMessage(MessageType.LEAVE, publisherId, myId, "Subscriber has left session"));
        redisPeerService.updateSubscriber(publisherId, null);

        String newSubscriberId = getWaitingSubscriberId(publisherId);
        if (newSubscriberId != null) {
            match(publisherId, newSubscriberId);
        } else {
            queueService.addToPublishQueue(publisherId);
        }
    }

    private void unregisterMySubscriber(String subscriberId, String myId) {
        if (subscriberId == null) {
            log.debug("No connected subscriber for {}", myId);
            return;
        }

        if (!redisPeerService.isPresent(subscriberId) || redisPeerService.getPeerSession(subscriberId) == null) {
            log.warn("Subscriber session not found: {}", subscriberId);
            return;
        }

        relayService.sendToPeer(subscriberId, new SignalMessage(MessageType.LEAVE, subscriberId, myId, "Subscriber has left session"));
        redisPeerService.updatePublisher(subscriberId, null);

        String newPublisherId = getWaitingPublisherId(subscriberId);
        if (newPublisherId != null) {
            match(newPublisherId, subscriberId);
        } else {
            queueService.addToSubscribeQueue(subscriberId);
            log.info("-> after disconnected add subscriber queue: {}", myId);
        }
    }

    private String getWaitingSubscriberId(String peerId) {
        PeerSession peerSession = redisPeerService.getPeerSession(peerId);

        String foundSubscriberId = null;
        String subscriberToRestore = null;
        boolean shouldRestoreMine = false;

        while (queueService.getSubscribeQueueSize() > 0 && foundSubscriberId == null) {
            String candidateId = queueService.popFromSubscribeQueue();
            if (candidateId == null) break;

            if (!redisPeerService.isPresent(candidateId)) {
                continue;
            }

            if (peerSession != null && peerSession.getPublisher() != null
                    && !isPeerAvailable(candidateId, peerSession.getPublisher())) {
                log.warn("peer is not available : {} <-> {}", candidateId, peerSession.getPublisher());
                subscriberToRestore = candidateId;
                continue;
            }

            if (candidateId.equals(peerId)) {
                shouldRestoreMine = true;
                continue;
            }

            foundSubscriberId = candidateId;
        }

        if (subscriberToRestore != null) queueService.addToSubscribeQueue(subscriberToRestore);
        if (shouldRestoreMine) queueService.addToSubscribeQueue(peerId);

        if (foundSubscriberId == null) {
            log.info("❌ No waiting subscriber in subscribe queue");
        }
        return foundSubscriberId;
    }

    private String getWaitingPublisherId(String peerId) {
        PeerSession peerSession = redisPeerService.getPeerSession(peerId);

        String foundPublisherId = null;
        String publisherToRestore = null;
        boolean shouldRestoreMine = false;

        while (queueService.getPublishQueueSize() > 0 && foundPublisherId == null) {
            String candidateId = queueService.popFromPublishQueue();
            if (candidateId == null) break;

            if (!redisPeerService.isPresent(candidateId)) {
                continue;
            }

            if (peerSession != null && peerSession.getSubscriber() != null
                    && !isPeerAvailable(candidateId, peerSession.getSubscriber())) {
                log.warn("peer is not available : {} <-> {}", candidateId, peerSession.getSubscriber());
                publisherToRestore = candidateId;
                continue;
            }

            if (candidateId.equals(peerId)) {
                shouldRestoreMine = true;
                continue;
            }

            foundPublisherId = candidateId;
        }

        if (publisherToRestore != null) queueService.addToPublishQueue(publisherToRestore);
        if (shouldRestoreMine) queueService.addToPublishQueue(peerId);

        if (foundPublisherId == null) {
            log.info("❌ No publisher in publish queue");
        }
        return foundPublisherId;
    }

    private void match(String publisherId, String subscriberId) {
        if (!redisPeerService.isPresent(publisherId) || !redisPeerService.isPresent(subscriberId)) {
            log.info("Session not active: {} <-> {}", subscriberId, publisherId);
            return;
        }

        relayService.sendToPeer(publisherId, new SignalMessage(MessageType.PUBLISH, publisherId, subscriberId));
        relayService.sendToPeer(subscriberId, new SignalMessage(MessageType.SUBSCRIBE, subscriberId, publisherId));

        redisPeerService.updateSubscriber(publisherId, subscriberId);
        redisPeerService.updatePublisher(subscriberId, publisherId);
        log.info("📤 Sent MATCH message) publisher: {}, subscriber: {}", publisherId, subscriberId);
    }

    private boolean isPeerAvailable(String targetPeerId, String myPeerId) {
        return targetPeerId != null && myPeerId != null && !targetPeerId.equals(myPeerId);
    }
}