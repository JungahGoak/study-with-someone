package com.koa.sws.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PeerRelation {
    private final String peerId;
    private String subscriber;
    private String publisher;

    public PeerRelation(String peerId) {
        this.peerId = peerId;
    }
}