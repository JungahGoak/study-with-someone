package com.koa.sws.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PeerSession {
    private final String peerId;
    private String subscriber;
    private String publisher;

    public PeerSession(String peerId) {
        this.peerId = peerId;
    }
}