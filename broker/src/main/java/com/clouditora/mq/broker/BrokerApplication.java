package com.clouditora.mq.broker;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.ServerNetworkConfig;
import com.clouditora.mq.store.MessageStoreConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.broker.BrokerStartup
 */
@Slf4j
public class BrokerApplication {
    public static void main(String[] args) {
        BrokerConfig brokerConfig = getBrokerConfig();
        ServerNetworkConfig serverNetworkConfig = getServerNetworkConfig();
        ClientNetworkConfig clientNetworkConfig = getClientNetworkConfig();
        MessageStoreConfig messageStoreConfig = getMessageStoreConfig();
        BrokerController controller = buildController(brokerConfig, serverNetworkConfig, clientNetworkConfig, messageStoreConfig);
        addShutdownHook(controller);
        controller.startup();
        System.out.printf("broker startup success: %s@%s%n", brokerConfig.getBrokerName(), serverNetworkConfig.getListenPort());
    }

    private static BrokerConfig getBrokerConfig() {
        BrokerConfig brokerConfig = new BrokerConfig();
        if (brokerConfig.getRocketmqHome() == null) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", GlobalConstant.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        return brokerConfig;
    }

    private static ServerNetworkConfig getServerNetworkConfig() {
        ServerNetworkConfig config = new ServerNetworkConfig();
        config.setListenPort(10911);
        return config;
    }

    private static ClientNetworkConfig getClientNetworkConfig() {
        return new ClientNetworkConfig();
    }

    private static MessageStoreConfig getMessageStoreConfig() {
        return new MessageStoreConfig();
    }

    private static BrokerController buildController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig, MessageStoreConfig messageStoreConfig) {
        return new BrokerController(brokerConfig, serverNetworkConfig, clientNetworkConfig, messageStoreConfig);
    }

    private static void addShutdownHook(BrokerController controller) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("broker shutdown...");
            long startTime = System.currentTimeMillis();
            try {
                controller.shutdown();
            } catch (Exception e) {
                log.error("broker shutdown exception", e);
            }
            System.out.printf("broker shutdown elapsed %dmS%n", System.currentTimeMillis() - startTime);
        }));
    }
}
