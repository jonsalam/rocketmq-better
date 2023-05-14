package com.clouditora.mq.broker;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.util.NetworkUtil;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.common.BrokerConfig
 */
@Data
public class BrokerConfig {
    private String rocketmqHome = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String namesrvAddr = System.getProperty(GlobalConstant.NAMESRV_ADDR_PROPERTY, System.getenv(GlobalConstant.NAMESRV_ADDR_ENV));
    private String brokerClusterName = "DefaultCluster";

    private Long brokerId = GlobalConstant.MASTER_ID;
    private String brokerIp = NetworkUtil.getLocalIp();
    private String brokerName = NetworkUtil.getLocalHostName();

    private int registerBrokerTimeoutMills = 6000;

    private MessageStoreConfig messageStoreConfig;

    public List<String> getNameserverEndpoints() {
        return Arrays.stream(Optional.ofNullable(namesrvAddr).orElse("localhost:9876").split(";")).collect(Collectors.toList());
    }
}
