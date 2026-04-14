package com.koa.sws.service;

import com.koa.sws.model.PeerRelation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SessionService {

    private final Map<String, WebSocketSession> websocketSessions = new ConcurrentHashMap<>();
    private final RedisPeerService redisPeerService;

    public String register(WebSocketSession session) {
        String peerId = session.getId();
        websocketSessions.put(peerId, session);
        redisPeerService.register(peerId);
        return peerId;
    }

    public PeerRelation remove(String sessionId) {
        websocketSessions.remove(sessionId);
        PeerRelation relation = redisPeerService.getPeerRelation(sessionId);
        redisPeerService.remove(sessionId);
        return relation;
    }

    public void updatePublisher(String peerId, String publisherId) {
        redisPeerService.updatePublisher(peerId, publisherId);
    }

    public void updateSubscriber(String peerId, String subscriberId) {
        redisPeerService.updateSubscriber(peerId, subscriberId);
    }

    public WebSocketSession getSession(String peerId) {
        if (peerId == null) return null;
        return websocketSessions.get(peerId);
    }

    public PeerRelation getPeerRelation(String peerId) {
        if (peerId == null) return null;
        return redisPeerService.getPeerRelation(peerId);
    }

    public boolean isSessionValid(WebSocketSession session) {
        return session != null && session.isOpen();
    }

    public int getSessionCount() {
        return websocketSessions.size();
    }
}