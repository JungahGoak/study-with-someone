package com.koa.sws.service;

import com.koa.sws.model.PeerSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;

/**
 * 대기열에서 매칭 가능한 피어를 탐색하는 서비스
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FindPeerService {

    private final SessionService sessionService;
    private final RedisQueueService queueService;

    public String findWaitingSubscriber(WebSocketSession session) {
        WebSocketSession subscriberSession = findWaitingPeer(session, QueueType.SUBSCRIBER);
        return subscriberSession != null ? subscriberSession.getId() : null;
    }

    public String findWaitingPublisher(WebSocketSession session) {
        WebSocketSession publisherSession = findWaitingPeer(session, QueueType.PUBLISHER);
        return publisherSession != null ? publisherSession.getId() : null;
    }

    /**
     * 대기열에서 유효한 매칭 후보를 탐색
     * @param session 현재 세션
     * @param queueType 찾을 피어의 큐 타입 (PUBLISHER 또는 SUBSCRIBER)
     * @return 매칭 가능한 피어의 세션, 없으면 null
     */
    private WebSocketSession findWaitingPeer(WebSocketSession session, QueueType queueType) {
        PeerSession myPeerSession = sessionService.getPeerSession(session.getId());
        String myId = session.getId();
        String myConnectedPeer = myPeerSession != null ? queueType.getConnectedPeer(myPeerSession) : null;

        String candidateId = null;
        WebSocketSession candidateSession = null;
        List<String> peersToRestore = new ArrayList<>();
        boolean needRestoreMine = false;

        long maxRetries = queueType.getSize(queueService);
        long attempts = 0;

        while (queueType.getSize(queueService) > 0 && !sessionService.isSessionValid(candidateSession) && attempts < maxRetries) {
            candidateId = queueType.pop(queueService);
            candidateSession = sessionService.getSession(candidateId);
            attempts++;

            // Validation 1: Cannot be connected to my existing peer (prevent circular dependency)
            if (myConnectedPeer != null && !isPeerAvailable(candidateId, myConnectedPeer)) {
                log.warn("Peer not available due to circular dependency - candidate: {}, myConnectedPeer: {}",
                        candidateId, myConnectedPeer);
                peersToRestore.add(candidateId);
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
            if (!sessionService.isSessionValid(candidateSession)) {
                log.debug("Invalid session found in queue - peerId: {}", candidateId);
                candidateId = null;
                candidateSession = null;
            }
        }

        // Restore peers back to queue if needed
        for (String peer : peersToRestore) {
            queueType.add(queueService, peer);
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

    private boolean isPeerAvailable(String targetPeerId, String myPeerId) {
        return targetPeerId != null && myPeerId != null && !targetPeerId.equals(myPeerId);
    }
}