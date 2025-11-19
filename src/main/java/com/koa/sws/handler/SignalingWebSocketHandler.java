package com.koa.sws.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.model.MessageType;
import com.koa.sws.model.SignalMessage;
import com.koa.sws.service.MatchService;
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
    private final MatchService matchService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String peerId = sessionService.register(session);
        log.info("⭐ CONNECTED: peerId={} sessionId={}", peerId, session.getId());

        matchService.registerPeer(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            SignalMessage signalMessage = objectMapper.readValue(message.getPayload(), SignalMessage.class);

            if (!session.isOpen()) {
                log.warn("Received message from unopen session: {}", session.getId());
                return;
            }

            signalMessage.setMyId(session.getId());

            switch (signalMessage.getType()) {
                case OFFER:
                case ANSWER:
                case ICE:
                    matchService.relaySignalMessage(signalMessage);
                    break;
                case LEAVE:
                    matchService.unregisterPeer(session.getId());
                    break;
                default:
                    log.warn("Unknown message type: {}", signalMessage.getType());
                    matchService.sendMessage(session, new SignalMessage(MessageType.ERROR, session.getId(), null, "Unknown message type"));
            }
        } catch (Exception e) {
            log.error("Error handling message", e);

            if (session.isOpen())
                matchService.sendMessage(session, new SignalMessage(MessageType.ERROR, session.getId(), null, "Failed message"));
        }

    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessionService.remove(session.getId());
        log.info("🔴 DISCONNECTED: peerId={} status={}", session.getId(), status);
    }
}
