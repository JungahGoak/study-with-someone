package com.koa.sws.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SignalMessage {
    private String type;   // "JOIN", "OFFER", "ANSWER"
    private String peerId;
    private String data;
}
