package com.koa.sws.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.model.SignalMessage;
import com.koa.sws.service.SessionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
@Component
@RequiredArgsConstructor
public class SignalingWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final SessionService sessionService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String peerId = sessionService.register(session);
        log.info("⭐ CONNECTED: peerId={} sessionId={}", peerId, session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        SignalMessage msg = objectMapper.readValue(message.getPayload(), SignalMessage.class);
        log.info("📩 MESSAGE: type={} from={}", msg.getType(), msg.getPeerId());

    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String peerId = sessionService.remove(session);
        log.info("🔴 DISCONNECTED: peerId={} status={}", peerId, status);
    }
}
