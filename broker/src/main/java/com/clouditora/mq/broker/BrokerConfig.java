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
    private Integer brokerPort = 8888;
    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10, 000 and 60, 000 milliseconds.
     */
    private int registerNameServerPeriod = 30_000;
    private int registerBrokerTimeoutMills = 6000;

    private MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

    public List<String> getNameserverEndpoints() {
        return Arrays.stream(Optional.ofNullable(namesrvAddr).orElse("localhost:9876").split(";")).collect(Collectors.toList());
    }

    public int getRegisterNameServerPeriod() {
        return Math.max(10_000, Math.min(this.registerNameServerPeriod, 60_000));
    }

    public String getBrokerEndpoint() {
        return "%s:%s".formatted(getBrokerIp(), getBrokerPort());
    }
}
