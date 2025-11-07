package com.koa.sws.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
@Service
@RequiredArgsConstructor
public class MatchService {

    private final SessionService sessionService;
    private final RedisQueueService queueService;

    /**
     * 사용자 등록
     * 1. publish
     * 2. subscribe
     */
    public void registerPeer(String peerId, WebSocketSession session) {
    }
}
