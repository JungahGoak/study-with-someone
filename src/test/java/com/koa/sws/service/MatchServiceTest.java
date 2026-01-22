package com.koa.sws.service;

import com.koa.sws.model.MessageType;
import com.koa.sws.model.PeerSession;
import com.koa.sws.model.SignalMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.socket.WebSocketSession;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MatchServiceTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private RedisQueueService queueService;

    @Mock
    private SignalMessageRelayService relayService;

    @Mock
    private WebSocketSession publisherSession;

    @Mock
    private WebSocketSession subscriberSession;

    @InjectMocks
    private MatchService matchService;

    private PeerSession publisherPeerSession;
    private PeerSession subscriberPeerSession;

    @BeforeEach
    void setUp() {
        publisherPeerSession = new PeerSession("publisher-1");
        subscriberPeerSession = new PeerSession("subscriber-1");

        lenient().when(publisherSession.getId()).thenReturn("publisher-1");
        lenient().when(subscriberSession.getId()).thenReturn("subscriber-1");
        lenient().when(publisherSession.isOpen()).thenReturn(true);
        lenient().when(subscriberSession.isOpen()).thenReturn(true);
    }

    @Test
    @DisplayName("메시지 중계 - relayService에 위임")
    void relaySignalMessage_ShouldDelegateToRelayService() {
        // given
        SignalMessage message = new SignalMessage(MessageType.OFFER, "sender", "receiver", "data");

        // when
        matchService.relaySignalMessage(message);

        // then
        verify(relayService).relaySignalMessage(message);
    }

    @Test
    @DisplayName("메시지 전송 - relayService에 위임")
    void sendMessage_ShouldDelegateToRelayService() {
        // given
        SignalMessage message = new SignalMessage(MessageType.PUBLISH, "pub", "sub");

        // when
        matchService.sendMessage(publisherSession, message);

        // then
        verify(relayService).sendMessage(publisherSession, message);
    }

    @Test
    @DisplayName("Publisher 등록 - 대기 중인 subscriber가 없으면 큐에 추가")
    void registerAsPublisher_ShouldAddToQueueWhenNoSubscriberIsWaiting() {
        // given
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(queueService.getSubscribeQueueSize()).thenReturn(0L);

        // when
        matchService.registerAsPublisher(publisherSession);

        // then
        verify(queueService).addToPublishQueue("publisher-1");
        verify(relayService, never()).sendMessage(any(), any());
    }

    @Test
    @DisplayName("Subscriber 등록 - 대기 중인 publisher가 없으면 큐에 추가")
    void registerAsSubscriber_ShouldAddToQueueWhenNoPublisherIsWaiting() {
        // given
        when(sessionService.getPeerSession("subscriber-1")).thenReturn(subscriberPeerSession);
        when(queueService.getPublishQueueSize()).thenReturn(0L);

        // when
        matchService.registerAsSubscriber(subscriberSession);

        // then
        verify(queueService).addToSubscribeQueue("subscriber-1");
        verify(relayService, never()).sendMessage(any(), any());
    }

    @Test
    @DisplayName("Publisher 등록 - 대기 중인 subscriber가 있으면 즉시 매칭 성공")
    void registerAsPublisher_ShouldMatchSuccessfully() {
        // given
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession); // match 메서드에서 필요

        // Queue에서 subscriber를 pop
        when(queueService.getSubscribeQueueSize()).thenReturn(1L, 0L);
        when(queueService.popFromSubscribeQueue()).thenReturn("subscriber-1");

        // when
        matchService.registerAsPublisher(publisherSession);

        // then
        verify(queueService).popFromSubscribeQueue();
        verify(sessionService).updateSubscriber("publisher-1", "subscriber-1");
        verify(sessionService).updatePublisher("subscriber-1", "publisher-1");
        verify(relayService, times(2)).sendMessage(any(WebSocketSession.class), any(SignalMessage.class));
    }

    @Test
    @DisplayName("Subscriber 등록 - 대기 중인 publisher가 있으면 즉시 매칭 성공")
    void registerAsSubscriber_ShouldMatchSuccessfully() {
        // given
        when(sessionService.getPeerSession("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession); // match 메서드에서 필요

        // Queue에서 publisher를 pop
        when(queueService.getPublishQueueSize()).thenReturn(1L, 0L);
        when(queueService.popFromPublishQueue()).thenReturn("publisher-1");

        // when
        matchService.registerAsSubscriber(subscriberSession);

        // then
        verify(queueService).popFromPublishQueue();
        verify(sessionService).updateSubscriber("publisher-1", "subscriber-1");
        verify(sessionService).updatePublisher("subscriber-1", "publisher-1");
        verify(relayService, times(2)).sendMessage(any(WebSocketSession.class), any(SignalMessage.class));
    }

    @Test
    @DisplayName("자기 자신 매칭 방지 - 큐에서 자신을 발견하면 스킵하고 다음 후보 찾기")
    void getWaitingPeer_ShouldSkipSelfMatch() {
        // given
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);

        // 큐에서 먼저 자기 자신이 나오고, 그 다음 유효한 subscriber가 나옴
        when(queueService.getSubscribeQueueSize()).thenReturn(2L, 1L, 0L);
        when(queueService.popFromSubscribeQueue())
                .thenReturn("publisher-1")  // 자기 자신 (스킵되어야 함)
                .thenReturn("subscriber-1"); // 유효한 subscriber

        // when
        matchService.registerAsPublisher(publisherSession);

        // then
        verify(queueService, times(2)).popFromSubscribeQueue();
        verify(queueService).addToSubscribeQueue("publisher-1"); // 자신을 큐에 복원
        verify(sessionService).updateSubscriber("publisher-1", "subscriber-1"); // 결국 subscriber-1과 매칭
    }


    @Test
    @DisplayName("피어 해제 - publisher에게 LEAVE 메시지 전송 및 재매칭")
    void unregisterPeer_ShouldNotifyPublisherAndRematch() {
        // given
        subscriberPeerSession.setPublisher("publisher-1");
        when(sessionService.remove("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(queueService.getSubscribeQueueSize()).thenReturn(0L);

        // when
        matchService.unregisterPeer("subscriber-1");

        // then
        verify(relayService).sendMessage(eq(publisherSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE &&
            msg.getMyId().equals("publisher-1") &&
            msg.getTargetId().equals("subscriber-1")
        ));
        verify(sessionService).updateSubscriber("publisher-1", null);
        verify(queueService).addToPublishQueue("publisher-1"); // 새 subscriber 대기를 위해 큐에 추가
    }

    @Test
    @DisplayName("피어 해제 - subscriber에게 LEAVE 메시지 전송 및 재매칭")
    void unregisterPeer_ShouldNotifySubscriberAndRematch() {
        // given
        publisherPeerSession.setSubscriber("subscriber-1");
        when(sessionService.remove("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.getPeerSession("subscriber-1")).thenReturn(subscriberPeerSession);
        when(queueService.getPublishQueueSize()).thenReturn(0L);

        // when
        matchService.unregisterPeer("publisher-1");

        // then
        verify(relayService).sendMessage(eq(subscriberSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE &&
            msg.getMyId().equals("subscriber-1") &&
            msg.getTargetId().equals("publisher-1")
        ));
        verify(sessionService).updatePublisher("subscriber-1", null);
        verify(queueService).addToSubscribeQueue("subscriber-1"); // 새 publisher 대기를 위해 큐에 추가
    }

    @Test
    @DisplayName("피어 해제 - 대기 중인 새로운 피어가 있으면 즉시 재매칭")
    void unregisterPeer_ShouldRematchWithWaitingPeer() {
        // given
        subscriberPeerSession.setPublisher("publisher-1");
        WebSocketSession newSubscriberSession = mock(WebSocketSession.class);
        when(newSubscriberSession.getId()).thenReturn("new-subscriber");
        when(newSubscriberSession.isOpen()).thenReturn(true);

        when(sessionService.remove("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.getSession("new-subscriber")).thenReturn(newSubscriberSession);

        // 큐에 새로운 subscriber가 대기 중
        when(queueService.getSubscribeQueueSize()).thenReturn(1L, 0L);
        when(queueService.popFromSubscribeQueue()).thenReturn("new-subscriber");

        // when
        matchService.unregisterPeer("subscriber-1");

        // then
        verify(relayService).sendMessage(eq(publisherSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE
        ));
        verify(sessionService).updateSubscriber("publisher-1", null);
        verify(queueService).popFromSubscribeQueue();
        verify(sessionService).updateSubscriber("publisher-1", "new-subscriber"); // 새 subscriber와 재매칭
        verify(queueService, never()).addToPublishQueue("publisher-1"); // 재매칭 성공했으므로 큐에 추가 안 함
    }

    @Test
    @DisplayName("피어 해제 - 존재하지 않는 피어 해제 시 경고 로그만 출력")
    void unregisterPeer_ShouldLogWarningWhenPeerNotFound() {
        // given
        when(sessionService.remove("non-existent-peer")).thenReturn(null);

        // when
        matchService.unregisterPeer("non-existent-peer");

        // then
        verify(sessionService).remove("non-existent-peer");
        verify(relayService, never()).sendMessage(any(), any());
        verify(queueService, never()).addToPublishQueue(anyString());
        verify(queueService, never()).addToSubscribeQueue(anyString());
    }

}
