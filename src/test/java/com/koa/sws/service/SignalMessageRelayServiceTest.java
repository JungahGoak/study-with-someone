package com.koa.sws.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.koa.sws.model.MessageType;
import com.koa.sws.model.SignalMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SignalMessageRelayServiceTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private WebSocketSession session;

    @InjectMocks
    private SignalMessageRelayService relayService;

    private SignalMessage testMessage;

    @BeforeEach
    void setUp() {
        testMessage = new SignalMessage(MessageType.OFFER, "sender-1", "receiver-1", "offer-data");
    }

    @Test
    @DisplayName("메시지 중계 성공 - 유효한 타겟 세션")
    void relaySignalMessage_Success() throws Exception {
        // given
        when(sessionService.getSession("receiver-1")).thenReturn(session);
        when(session.isOpen()).thenReturn(true);
        when(objectMapper.writeValueAsString(testMessage)).thenReturn("{\"type\":\"OFFER\"}");

        // when
        relayService.relaySignalMessage(testMessage);

        // then
        verify(sessionService).getSession("receiver-1");
        verify(session).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("메시지 중계 실패 - targetId가 null")
    void relaySignalMessage_FailWhenTargetIdIsNull() throws Exception {
        // given
        testMessage.setTargetId(null);

        // when
        relayService.relaySignalMessage(testMessage);

        // then
        verify(sessionService, never()).getSession(anyString());
        verify(session, never()).sendMessage(any());
    }

    @Test
    @DisplayName("메시지 중계 실패 - 타겟 세션이 null")
    void relaySignalMessage_FailWhenTargetSessionIsNull() throws Exception {
        // given
        when(sessionService.getSession("receiver-1")).thenReturn(null);

        // when
        relayService.relaySignalMessage(testMessage);

        // then
        verify(sessionService).getSession("receiver-1");
        verify(session, never()).sendMessage(any());
    }

    @Test
    @DisplayName("메시지 중계 실패 - 타겟 세션이 닫혀있음")
    void relaySignalMessage_FailWhenTargetSessionIsClosed() throws Exception {
        // given
        when(sessionService.getSession("receiver-1")).thenReturn(session);
        when(session.isOpen()).thenReturn(false);

        // when
        relayService.relaySignalMessage(testMessage);

        // then
        verify(sessionService).getSession("receiver-1");
        verify(session, never()).sendMessage(any());
    }

    @Test
    @DisplayName("메시지 전송 성공")
    void sendMessage_Success() throws Exception {
        // given
        when(objectMapper.writeValueAsString(testMessage)).thenReturn("{\"type\":\"OFFER\"}");

        // when
        relayService.sendMessage(session, testMessage);

        // then
        verify(objectMapper).writeValueAsString(testMessage);
        verify(session).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("메시지 전송 실패 - JSON 직렬화 오류")
    void sendMessage_FailWhenJsonError() throws Exception {
        // given
        when(objectMapper.writeValueAsString(testMessage))
                .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("JSON error"){});

        // when
        relayService.sendMessage(session, testMessage);

        // then
        verify(objectMapper).writeValueAsString(testMessage);
        verify(session, never()).sendMessage(any());
    }
}
