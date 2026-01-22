package com.koa.sws.model;

import com.koa.sws.service.RedisQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueueTypeTest {

    @Mock
    private RedisQueueService queueService;

    private PeerSession peerSession;

    @BeforeEach
    void setUp() {
        peerSession = new PeerSession("peer-1");
    }

    @Test
    @DisplayName("PUBLISHER 타입 - pop 메서드 호출")
    void publisher_ShouldPopFromPublishQueue() {
        // given
        when(queueService.popFromPublishQueue()).thenReturn("publisher-1");

        // when
        String result = QueueType.PUBLISHER.pop(queueService);

        // then
        assertThat(result).isEqualTo("publisher-1");
        verify(queueService).popFromPublishQueue();
    }

    @Test
    @DisplayName("PUBLISHER 타입 - add 메서드 호출")
    void publisher_ShouldAddToPublishQueue() {
        // when
        QueueType.PUBLISHER.add(queueService, "publisher-1");

        // then
        verify(queueService).addToPublishQueue("publisher-1");
    }

    @Test
    @DisplayName("PUBLISHER 타입 - getSize 메서드 호출")
    void publisher_ShouldGetPublishQueueSize() {
        // given
        when(queueService.getPublishQueueSize()).thenReturn(5L);

        // when
        Long size = QueueType.PUBLISHER.getSize(queueService);

        // then
        assertThat(size).isEqualTo(5L);
        verify(queueService).getPublishQueueSize();
    }

    @Test
    @DisplayName("PUBLISHER 타입 - getConnectedPeer는 subscriber를 반환")
    void publisher_ShouldReturnSubscriberAsConnectedPeer() {
        // given
        peerSession.setSubscriber("subscriber-1");

        // when
        String connectedPeer = QueueType.PUBLISHER.getConnectedPeer(peerSession);

        // then
        assertThat(connectedPeer).isEqualTo("subscriber-1");
    }

    @Test
    @DisplayName("SUBSCRIBER 타입 - pop 메서드 호출")
    void subscriber_ShouldPopFromSubscribeQueue() {
        // given
        when(queueService.popFromSubscribeQueue()).thenReturn("subscriber-1");

        // when
        String result = QueueType.SUBSCRIBER.pop(queueService);

        // then
        assertThat(result).isEqualTo("subscriber-1");
        verify(queueService).popFromSubscribeQueue();
    }

    @Test
    @DisplayName("SUBSCRIBER 타입 - add 메서드 호출")
    void subscriber_ShouldAddToSubscribeQueue() {
        // when
        QueueType.SUBSCRIBER.add(queueService, "subscriber-1");

        // then
        verify(queueService).addToSubscribeQueue("subscriber-1");
    }

    @Test
    @DisplayName("SUBSCRIBER 타입 - getSize 메서드 호출")
    void subscriber_ShouldGetSubscribeQueueSize() {
        // given
        when(queueService.getSubscribeQueueSize()).thenReturn(10L);

        // when
        Long size = QueueType.SUBSCRIBER.getSize(queueService);

        // then
        assertThat(size).isEqualTo(10L);
        verify(queueService).getSubscribeQueueSize();
    }

    @Test
    @DisplayName("SUBSCRIBER 타입 - getConnectedPeer는 publisher를 반환")
    void subscriber_ShouldReturnPublisherAsConnectedPeer() {
        // given
        peerSession.setPublisher("publisher-1");

        // when
        String connectedPeer = QueueType.SUBSCRIBER.getConnectedPeer(peerSession);

        // then
        assertThat(connectedPeer).isEqualTo("publisher-1");
    }

    @Test
    @DisplayName("PUBLISHER와 SUBSCRIBER는 서로 다른 큐를 사용")
    void publisherAndSubscriber_ShouldUseDifferentQueues() {
        // when
        QueueType.PUBLISHER.add(queueService, "peer-1");
        QueueType.SUBSCRIBER.add(queueService, "peer-2");

        // then
        verify(queueService).addToPublishQueue("peer-1");
        verify(queueService).addToSubscribeQueue("peer-2");
        verify(queueService, never()).addToPublishQueue("peer-2");
        verify(queueService, never()).addToSubscribeQueue("peer-1");
    }
}
