package com.koa.sws.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
public class SignalMessageRelayService {

    private final SessionService sessionService;
    private final ObjectMapper objectMapper;

    /**
     * 메시지 중계 (OFFER, ANSWER, ICE)
     */
    public void relaySignalMessage(SignalMessage message) {
        String fromId = message.getMyId();
        String toId = message.getTargetId();

        if (toId == null) {
            log.warn("Relay failed: targetId is null - fromId: {}", fromId);
            return;
        }

        WebSocketSession targetSession = sessionService.getSession(toId);
        if (!isSessionValid(targetSession)) {
            log.warn("Relay failed: target session invalid - targetId: {}", toId);
            return;
        }

        sendMessage(targetSession, message);
        log.info("Message relayed - type: {}, from: {} to: {}", message.getType(), fromId, toId);
    }

    /**
     * WebSocket 메시지 전송
     */
    public void sendMessage(WebSocketSession session, SignalMessage message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            session.sendMessage(new TextMessage(json));
        } catch (IOException e) {
            log.error("Failed to send message - type: {}, error: {}", message.getType(), e.getMessage());
        }
    }

    private boolean isSessionValid(WebSocketSession session) {
        return session != null && session.isOpen();
    }
}