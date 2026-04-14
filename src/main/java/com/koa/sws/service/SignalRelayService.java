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
public class SignalRelayService {

    private final SessionService sessionService;
    private final RedisPeerService redisPeerService;
    private final ObjectMapper objectMapper;

    /**
     * 메시지 중계 (OFFER, ANSWER, ICE)
     */
    public void relay(SignalMessage message) {
        String fromId = message.getMyId();
        String toId = message.getTargetId();

        if (toId == null) {
            log.warn("relay failed: targetId is null. fromId={}", fromId);
            return;
        }

        if (!redisPeerService.isPresent(toId)) {
            log.warn("relay failed: target not present. targetId={}", toId);
            return;
        }

        sendToPeer(toId, message);
        log.info("🔁 Relayed {} from {} → {}", message.getType(), fromId, toId);
    }

    public void sendToPeer(String peerId, SignalMessage message) {
        WebSocketSession session = sessionService.getSession(peerId);
        if (session != null && session.isOpen()) {
            sendDirect(session, message);
        } else {
            log.warn("sendToPeer failed: session not found or closed. peerId={}", peerId);
        }
    }

    /**
     * 로컬 WebSocket 세션에 직접 전송
     */
    public void sendDirect(WebSocketSession session, SignalMessage message) {
        try {
            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(message)));
        } catch (IOException e) {
            log.error("Failed to send message type {}: {}", message.getType(), e.getMessage());
        }
    }
}