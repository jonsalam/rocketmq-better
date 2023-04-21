package com.clouditora.mq.nameserver;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.namesrv.NamesrvStartup
 */
@Slf4j
public class NameServerApplication {
    public static void main(String[] args) {
        NameServerConfig nameserverConfig = getNameServerConfig();
        ServerNetworkConfig serverNetworkConfig = getNetworkConfig();
        NameServerController controller = buildController(nameserverConfig, serverNetworkConfig);
        addShutdownHook(controller);
        controller.startup();
        System.out.printf("name server startup success: %s%n", serverNetworkConfig.getListenPort());
    }

    private static NameServerConfig getNameServerConfig() {
        NameServerConfig nameserverConfig = new NameServerConfig();
        if (nameserverConfig.getRocketmqHome() == null) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", GlobalConstant.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        return nameserverConfig;
    }

    private static ServerNetworkConfig getNetworkConfig() {
        ServerNetworkConfig config = new ServerNetworkConfig();
        config.setListenPort(9876);
        return config;
    }

    private static NameServerController buildController(NameServerConfig nameserverConfig, ServerNetworkConfig serverNetworkConfig) {
        return new NameServerController(nameserverConfig, serverNetworkConfig);
    }

    private static void addShutdownHook(NameServerController controller) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("name server shutdown...");
            long startTime = System.currentTimeMillis();
            try {
                controller.shutdown();
            } catch (Exception e) {
                log.error("name server shutdown exception", e);
            }
            System.out.printf("name server shutdown elapsed %dmS%n", System.currentTimeMillis() - startTime);
        }));
    }
}
