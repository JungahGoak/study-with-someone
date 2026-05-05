package com.koa.sws.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.exception.InvalidMessageException;
import com.koa.sws.exception.SessionException;
import com.koa.sws.model.MessageType;
import com.koa.sws.model.SignalMessage;
import com.koa.sws.service.MatchService;
import com.koa.sws.service.SessionService;
import com.koa.sws.service.SignalMessageRelayService;
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
    private final SignalMessageRelayService relayService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String peerId = sessionService.register(session);
        log.info("WebSocket connection established - peerId: {}, sessionId: {}", peerId, session.getId());

        matchService.registerPeer(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            validateSession(session);
            SignalMessage signalMessage = parseAndValidateMessage(message);
            signalMessage.setMyId(session.getId());
            processMessage(session, signalMessage);
        } catch (InvalidMessageException e) {
            log.warn("Invalid message received - sessionId: {}, error: {}", session.getId(), e.getMessage());
            sendErrorResponse(session, "Invalid message format");
        } catch (SessionException e) {
            // 세션이 null이거나 닫힌 상태이므로 에러 응답 전송 불가
            log.warn("Session error - sessionId: {}, error: {}", session.getId(), e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error handling message - sessionId: {}", session.getId(), e);
            sendErrorResponse(session, "Internal server error");
        }
    }

    private void validateSession(WebSocketSession session) throws SessionException {
        if (session == null) {
            throw new SessionException("Session is null");
        }
        if (!session.isOpen()) {
            throw new SessionException("Session is closed: " + session.getId());
        }
    }

    private SignalMessage parseAndValidateMessage(TextMessage message) throws InvalidMessageException {
        if (message == null || message.getPayload() == null || message.getPayload().trim().isEmpty()) {
            throw new InvalidMessageException("Message payload cannot be null or empty");
        }

        try {
            SignalMessage signalMessage = objectMapper.readValue(message.getPayload(), SignalMessage.class);

            if (signalMessage == null || signalMessage.getType() == null) {
                throw new InvalidMessageException("Message type is required");
            }

            return signalMessage;
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Failed to parse message JSON", e);
        }
    }

    private void processMessage(WebSocketSession session, SignalMessage signalMessage) {
        switch (signalMessage.getType()) {
            case OFFER:
            case ANSWER:
            case ICE:
                relayService.relaySignalMessage(signalMessage);
                break;
            case JOIN:
                log.debug("JOIN message received - sessionId: {}", session.getId());
                break;
            default:
                log.warn("Unknown message type - sessionId: {}, type: {}", session.getId(), signalMessage.getType());
                sendErrorResponse(session, "Unknown message type");
        }
    }

    private void sendErrorResponse(WebSocketSession session, String errorMessage) {
        if (session != null && session.isOpen()) {
            relayService.sendMessage(session, new SignalMessage(MessageType.ERROR, session.getId(), null, errorMessage));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        try {
            matchService.unregisterPeer(session.getId());
        } finally {
            log.info("WebSocket connection closed - peerId: {}, status: {}, reason: {}",
                    session.getId(), status.getCode(), status.getReason());
        }

    }
}