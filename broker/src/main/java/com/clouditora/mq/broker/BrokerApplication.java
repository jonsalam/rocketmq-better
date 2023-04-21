package com.clouditora.mq.broker;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.broker.BrokerStartup
 */
@Slf4j
public class BrokerApplication {
    public static void main(String[] args) {
        BrokerConfig nameserverConfig = getBrokerConfig();
        ServerNetworkConfig serverNetworkConfig = getServerNetworkConfig();
        ClientNetworkConfig clientNetworkConfig = getClientNetworkConfig();
        BrokerController controller = buildController(nameserverConfig, serverNetworkConfig, clientNetworkConfig);
        addShutdownHook(controller);
        controller.startup();
        System.out.printf("broker startup success: %s%n", serverNetworkConfig.getListenPort());
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

    private static BrokerController buildController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        return new BrokerController(brokerConfig, serverNetworkConfig, clientNetworkConfig);
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
