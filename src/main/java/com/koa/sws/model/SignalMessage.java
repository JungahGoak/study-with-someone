package com.koa.sws.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SignalMessage {
    private MessageType type;   // "JOIN", "OFFER", "ANSWER"
    private String myId;
    private String targetId;
    private String data;

    public SignalMessage(MessageType messageType, String myId, String targetId) {
        this.type = messageType;
        this.myId = myId;
        this.targetId = targetId;
    }
}
