package com.clouditora.mq.common.command;

import com.clouditora.mq.common.util.JsonUtil;

public interface CommandJsonBody {
    default byte[] encode() {
        return JsonUtil.toBytes(this);
    }
}
