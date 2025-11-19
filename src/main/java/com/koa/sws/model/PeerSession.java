package com.koa.sws.model;

import lombok.Data;

@Data
public class PeerSession {
    private String peerId;
    private String subscribeFrom;
    private String publishTo;

    public PeerSession(String peerId) {
        this.peerId = peerId;
    }

    public void setSubscriber(String subscriberId) {
        this.subscribeFrom = subscriberId;
    }

    public void setPublisher(String publisherId) {
        this.publishTo = publisherId;
    }
}
