package com.clouditora.mq.nameserver;

import com.clouditora.mq.common.constant.GlobalConstant;
import lombok.Data;

@Data
public class NameserverConfig {
    private String home = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String kvConfigPath = "%s/namesrv/kvConfig.json".formatted(GlobalConstant.USER_HOME);
    private String configStorePath = "%s/namesrv/namesrv.properties".formatted(GlobalConstant.USER_HOME);
    private String productEnvName = "center";
    private boolean clusterTest = false;
    private boolean orderMessageEnable = false;
}
