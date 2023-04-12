package com.clouditora.mq.network;

import lombok.Data;

@Data
public class RequestHeader implements CommandHeader {
    private Integer count;
    private String messageTitle;
}
