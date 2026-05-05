package com.koa.sws.service;

import com.koa.sws.model.PeerRelation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SessionService {

    private static final int SEND_TIME_LIMIT_MS = 1_000;
    private static final int BUFFER_SIZE_LIMIT_BYTES = 16 * 1024;

    private final Map<String, WebSocketSession> websocketSessions = new ConcurrentHashMap<>();
    private final RedisPeerService redisPeerService;

    public String register(WebSocketSession session) {
        String peerId = session.getId();
        ConcurrentWebSocketSessionDecorator concurrentSession =
                new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, BUFFER_SIZE_LIMIT_BYTES);
        websocketSessions.put(peerId, concurrentSession);
        redisPeerService.register(peerId);
        return peerId;
    }

    public PeerRelation remove(String sessionId) {
        websocketSessions.remove(sessionId);
        PeerRelation relation = redisPeerService.getPeerRelation(sessionId);
        String publisherId = relation != null ? relation.getPublisher() : null;
        String subscriberId = relation != null ? relation.getSubscriber() : null;
        redisPeerService.remove(sessionId, publisherId, subscriberId);
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