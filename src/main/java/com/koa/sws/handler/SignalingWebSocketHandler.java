package com.koa.sws.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.model.MessageType;
import com.koa.sws.model.SignalMessage;
import com.koa.sws.service.MatchService;
import com.koa.sws.service.SignalRelayService;
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
    private final MatchService matchService;
    private final SignalRelayService relayService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("⭐ CONNECTED: peerId={}", session.getId());
        matchService.registerPeer(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            SignalMessage signalMessage = objectMapper.readValue(message.getPayload(), SignalMessage.class);
            signalMessage.setMyId(session.getId());

            switch (signalMessage.getType()) {
                case OFFER, ANSWER, ICE -> relayService.relay(signalMessage);
                default -> {
                    log.warn("Unknown message type: {}", signalMessage.getType());
                    relayService.sendDirect(session, new SignalMessage(MessageType.ERROR, session.getId(), null, "Unknown message type"));
                }
            }
        } catch (Exception e) {
            log.error("Error handling message", e);
            if (session.isOpen()) {
                relayService.sendDirect(session, new SignalMessage(MessageType.ERROR, session.getId(), null, "Failed message"));
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.debug("🔴 DISCONNECTED: peerId={} status={}", session.getId(), status);
        matchService.unregisterPeer(session.getId());
    }
}