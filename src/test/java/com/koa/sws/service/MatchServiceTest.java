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
    private FindPeerService findPeerService;

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
    @DisplayName("Publisher 등록 - 대기 중인 subscriber가 없으면 큐에 추가")
    void registerAsPublisher_ShouldAddToQueueWhenNoSubscriberIsWaiting() {
        // given
        when(findPeerService.findWaitingSubscriber(publisherSession)).thenReturn(null);

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
        when(findPeerService.findWaitingPublisher(subscriberSession)).thenReturn(null);

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
        when(findPeerService.findWaitingSubscriber(publisherSession)).thenReturn("subscriber-1");
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.isSessionValid(publisherSession)).thenReturn(true);
        when(sessionService.isSessionValid(subscriberSession)).thenReturn(true);

        // when
        matchService.registerAsPublisher(publisherSession);

        // then
        verify(sessionService).updateSubscriber("publisher-1", "subscriber-1");
        verify(sessionService).updatePublisher("subscriber-1", "publisher-1");
        verify(relayService, times(2)).sendMessage(any(WebSocketSession.class), any(SignalMessage.class));
    }

    @Test
    @DisplayName("Subscriber 등록 - 대기 중인 publisher가 있으면 즉시 매칭 성공")
    void registerAsSubscriber_ShouldMatchSuccessfully() {
        // given
        when(findPeerService.findWaitingPublisher(subscriberSession)).thenReturn("publisher-1");
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.isSessionValid(publisherSession)).thenReturn(true);
        when(sessionService.isSessionValid(subscriberSession)).thenReturn(true);

        // when
        matchService.registerAsSubscriber(subscriberSession);

        // then
        verify(sessionService).updateSubscriber("publisher-1", "subscriber-1");
        verify(sessionService).updatePublisher("subscriber-1", "publisher-1");
        verify(relayService, times(2)).sendMessage(any(WebSocketSession.class), any(SignalMessage.class));
    }

    @Test
    @DisplayName("피어 해제 - publisher에게 LEAVE 메시지 전송 및 재매칭")
    void unregisterPeer_ShouldNotifyPublisherAndRematch() {
        // given
        subscriberPeerSession.setPublisher("publisher-1");
        when(sessionService.remove("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.isSessionValid(publisherSession)).thenReturn(true);
        when(findPeerService.findWaitingSubscriber(publisherSession)).thenReturn(null);

        // when
        matchService.unregisterPeer("subscriber-1");

        // then
        verify(relayService).sendMessage(eq(publisherSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE &&
            msg.getMyId().equals("publisher-1") &&
            msg.getTargetId().equals("subscriber-1")
        ));
        verify(sessionService).updateSubscriber("publisher-1", null);
        verify(queueService).addToPublishQueue("publisher-1");
    }

    @Test
    @DisplayName("피어 해제 - subscriber에게 LEAVE 메시지 전송 및 재매칭")
    void unregisterPeer_ShouldNotifySubscriberAndRematch() {
        // given
        publisherPeerSession.setSubscriber("subscriber-1");
        when(sessionService.remove("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.getSession("subscriber-1")).thenReturn(subscriberSession);
        when(sessionService.getPeerSession("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.isSessionValid(subscriberSession)).thenReturn(true);
        when(findPeerService.findWaitingPublisher(subscriberSession)).thenReturn(null);

        // when
        matchService.unregisterPeer("publisher-1");

        // then
        verify(relayService).sendMessage(eq(subscriberSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE &&
            msg.getMyId().equals("subscriber-1") &&
            msg.getTargetId().equals("publisher-1")
        ));
        verify(sessionService).updatePublisher("subscriber-1", null);
        verify(queueService).addToSubscribeQueue("subscriber-1");
    }

    @Test
    @DisplayName("피어 해제 - 대기 중인 새로운 피어가 있으면 즉시 재매칭")
    void unregisterPeer_ShouldRematchWithWaitingPeer() {
        // given
        subscriberPeerSession.setPublisher("publisher-1");
        WebSocketSession newSubscriberSession = mock(WebSocketSession.class);
        lenient().when(newSubscriberSession.getId()).thenReturn("new-subscriber");

        when(sessionService.remove("subscriber-1")).thenReturn(subscriberPeerSession);
        when(sessionService.getSession("publisher-1")).thenReturn(publisherSession);
        when(sessionService.getPeerSession("publisher-1")).thenReturn(publisherPeerSession);
        when(sessionService.isSessionValid(publisherSession)).thenReturn(true);
        when(sessionService.isSessionValid(newSubscriberSession)).thenReturn(true);
        when(sessionService.getSession("new-subscriber")).thenReturn(newSubscriberSession);
        when(findPeerService.findWaitingSubscriber(publisherSession)).thenReturn("new-subscriber");

        // when
        matchService.unregisterPeer("subscriber-1");

        // then
        verify(relayService).sendMessage(eq(publisherSession), argThat(msg ->
            msg.getType() == MessageType.LEAVE
        ));
        verify(sessionService).updateSubscriber("publisher-1", null);
        verify(sessionService).updateSubscriber("publisher-1", "new-subscriber");
        verify(queueService, never()).addToPublishQueue("publisher-1");
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