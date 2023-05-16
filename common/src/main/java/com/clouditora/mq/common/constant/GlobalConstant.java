package com.clouditora.mq.common.constant;

import java.lang.management.ManagementFactory;

/**
 * @link org.apache.rocketmq.common.MixAll
 */
public interface GlobalConstant {
    String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    String USER_HOME = "user.home";
    Long MASTER_ID = 0L;
    /**
     * @link org.apache.rocketmq.common.UtilAll#HOST_NAME
     */
    long PID = ManagementFactory.getRuntimeMXBean().getPid();

    interface SystemGroup {
        /**
         * 主要是给消费端用于重发消息
         *
         * @link org.apache.rocketmq.common.MixAll#CLIENT_INNER_PRODUCER_GROUP
         */
        String CLIENT_INNER_PRODUCER = "CLIENT_INNER_PRODUCER";
        String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";

        static String wrapRetry(String group) {
            return RETRY_GROUP_TOPIC_PREFIX + group;
        }
    }

}
