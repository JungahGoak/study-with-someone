package com.koa.sws.service;

import com.koa.sws.model.PeerSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class SessionService {

    private final Map<String, WebSocketSession> websocketSessions = new ConcurrentHashMap<>();
    private final Map<String, PeerSession> peerSessions = new ConcurrentHashMap<>();

    public String register(WebSocketSession session) {
        websocketSessions.put(session.getId(), session);
        peerSessions.put(session.getId(), new PeerSession(session.getId()));
        return session.getId();
    }

    public PeerSession remove(String sessionId) {
        PeerSession peer = peerSessions.remove(sessionId);
        websocketSessions.remove(sessionId);

        return peer;
    }

    public void updatePublisher(String peerId, String publisherId) {
        PeerSession peerSession = peerSessions.get(peerId);
        peerSession.setPublisher(publisherId);
    }

    public void updateSubscriber(String peerId, String subscriberId) {
        PeerSession peerSession = peerSessions.get(peerId);
        peerSession.setSubscriber(subscriberId);
    }

    public WebSocketSession getSession(String peerId) {
        if (peerId == null) return null;
        return websocketSessions.get(peerId);
    }

    public PeerSession getPeerSession(String peerId) {
        if (peerId == null) return null;
        return peerSessions.get(peerId);
    }

    public Collection<WebSocketSession> getAllSessions() {
        return websocketSessions.values();
    }
}
