package com.clouditora.mq.common.constant;

import java.lang.management.ManagementFactory;

/**
 * @link org.apache.rocketmq.common.MixAll
 */
public class GlobalConstant {
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    public static final String USER_HOME = "user.home";
    public static final Long MASTER_ID = 0L;
    /**
     * format: pid@hostname
     *
     * @link org.apache.rocketmq.common.UtilAll#HOST_NAME
     */
    public static final String INSTANCE = ManagementFactory.getRuntimeMXBean().getName();
}
