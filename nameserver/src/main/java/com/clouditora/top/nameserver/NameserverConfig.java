package com.clouditora.top.nameserver;

import com.clouditora.mq.common.constant.GlobalConstant;
import lombok.Data;

import java.io.File;

@Data
public class NameserverConfig {
    private String rocketmqHome = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String kvConfigPath = "%s%snamesrv%skvConfig.json".formatted(GlobalConstant.USER_HOME, File.separator, File.separator);
    private String configStorePath = "%s%snamesrv%snamesrv.properties".formatted(GlobalConstant.USER_HOME, File.separator, File.separator);
    private String productEnvName = "center";
    private boolean clusterTest = false;
    private boolean orderMessageEnable = false;
}
