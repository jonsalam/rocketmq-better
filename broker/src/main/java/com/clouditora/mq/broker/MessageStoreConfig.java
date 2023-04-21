package com.clouditora.mq.broker;

import com.clouditora.mq.common.constant.GlobalConstant;
import lombok.Data;

import java.io.File;

@Data
public class MessageStoreConfig {
    private String rocketmqHome = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String storePathRootDir = rocketmqHome + File.separator + "store";
}
