package com.clouditora.mq.common.network;

import com.clouditora.mq.common.util.JsonUtil;

public interface CommandJsonBody {
    default byte[] encode() {
        return JsonUtil.toJsonBytes(this);
    }
}
