package com.clouditora.mq.network;

import com.clouditora.mq.common.command.header.CommandHeader;
import lombok.Data;

@Data
public class RequestHeader implements CommandHeader {
    private Integer count;
    private String messageTitle;
}
