package com.koa.sws.service;

import com.koa.sws.model.PeerRelation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class SessionService {

    private final Map<String, WebSocketSession> websocketSessions = new ConcurrentHashMap<>();
    private final Map<String, PeerRelation> peerRelations = new ConcurrentHashMap<>();

    public String register(WebSocketSession session) {
        websocketSessions.put(session.getId(), session);
        peerRelations.put(session.getId(), new PeerRelation(session.getId()));
        return session.getId();
    }

    public PeerRelation remove(String sessionId) {
        PeerRelation relation = peerRelations.remove(sessionId);
        websocketSessions.remove(sessionId);
        return relation;
    }

    public void updatePublisher(String peerId, String publisherId) {
        PeerRelation relation = peerRelations.get(peerId);
        if (relation == null) {
            log.warn("PeerRelation not found for peerId: {}", peerId);
            return;
        }
        relation.setPublisher(publisherId);
    }

    public void updateSubscriber(String peerId, String subscriberId) {
        PeerRelation relation = peerRelations.get(peerId);
        if (relation == null) {
            log.warn("PeerRelation not found for peerId: {}", peerId);
            return;
        }
        relation.setSubscriber(subscriberId);
    }

    public WebSocketSession getSession(String peerId) {
        if (peerId == null) return null;
        return websocketSessions.get(peerId);
    }

    public PeerRelation getPeerRelation(String peerId) {
        if (peerId == null) return null;
        return peerRelations.get(peerId);
    }

    public boolean isSessionValid(WebSocketSession session) {
        return session != null && session.isOpen();
    }

    public int getSessionCount() {
        return websocketSessions.size();
    }
}