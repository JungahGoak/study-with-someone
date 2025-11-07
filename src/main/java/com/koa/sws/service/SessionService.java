package com.koa.sws.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class SessionService {

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, String> sessionToPeerId = new ConcurrentHashMap<>();

    public String register(WebSocketSession session) {
        String peerId = UUID.randomUUID().toString();
        sessions.put(peerId, session);
        sessionToPeerId.put(session.getId(), peerId);
        return peerId;
    }

    public String remove(WebSocketSession session) {
        String peerId = sessionToPeerId.remove(session.getId());
        if (peerId != null) {
            sessions.remove(peerId);
        }
        return peerId;
    }

    public WebSocketSession getSession(String peerId) {
        return sessions.get(peerId);
    }

    public Collection<WebSocketSession> getAllSessions() {
        return sessions.values();
    }
}
