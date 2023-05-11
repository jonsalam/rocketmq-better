package com.clouditora.mq.client.instance;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.util.NetworkUtil;
import com.clouditora.mq.network.protocol.LanguageCode;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.client.ClientConfig
 */
@Data
public class ClientConfig {
    public static final String DEFAULT_INSTANCE = "DEFAULT";
    private String namesrvAddr = System.getProperty(GlobalConstant.NAMESRV_ADDR_PROPERTY, System.getenv(GlobalConstant.NAMESRV_ADDR_ENV));
    private String clientIP = NetworkUtil.getLocalIp();
    private String instanceName = System.getProperty("rocketmq.client.name", DEFAULT_INSTANCE);
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private String namespace;
    private boolean namespaceInitialized = false;
    /**
     * Pulling topic information interval from the named server
     * typo: poll -> pull
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;
    private int mqClientApiTimeout = 3 * 1000;
    private LanguageCode language = LanguageCode.JAVA;

    public List<String> getNameserverEndpoints() {
        return Arrays.stream(Optional.ofNullable(namesrvAddr).orElse("localhost:9876").split(";")).collect(Collectors.toList());
    }

    /**
     * @link org.apache.rocketmq.client.ClientConfig#buildMQClientId
     */
    public String buildClientId() {
        return instanceName;
    }
}
