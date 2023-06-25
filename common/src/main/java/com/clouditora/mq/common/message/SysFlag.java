package com.clouditora.mq.common.message;

import lombok.AllArgsConstructor;

/**
 * @link org.apache.rocketmq.common.sysflag.PullSysFlag
 */
public class SysFlag {
    @AllArgsConstructor
    enum Flag {
        COMMIT_OFFSET(0x1),
        SUSPEND(0x1 << 1),
        /**
         * 订阅表达式
         */
        SUBSCRIPTION(0x1 << 2),
        CLASS_FILTER(0x1 << 3),
        LITE_PULL_MESSAGE(0x1 << 4),
        ;

        private final int bit;
    }

    public static boolean hasSubscriptionFlag(int flag) {
        return (flag & Flag.SUBSCRIPTION.bit) == Flag.SUBSCRIPTION.bit;
    }
}
