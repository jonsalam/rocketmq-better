package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MagicCode {
    /**
     * Message's MAGIC CODE daa320a7 = 0xAABBCCDD ^ 1880681586 + 8
     * 标记某一段为消息, 即: [msgId, MESSAGE_MAGIC_CODE, 消息]
     *
     * @link org.apache.rocketmq.store.CommitLog#MESSAGE_MAGIC_CODE
     */
    MESSAGE(-626843481),

    /**
     * End of file empty MAGIC CODE cbd43194 = 0xBBCCDDEE ^ 1880681586 + 8
     * 标记某一段为空白, 即: [msgId, BLANK_MAGIC_CODE, 空白]
     * 当CommitLog无法容纳消息时，使用该类型结尾
     *
     * @link org.apache.rocketmq.store.CommitLog#BLANK_MAGIC_CODE
     */
    BLANK(-875286124),

    ERROR(0);

    private final int code;
}
