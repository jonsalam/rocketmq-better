package com.clouditora.mq.nameserver;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.namesrv.NamesrvStartup
 */
@Slf4j
public class NameserverApplication {
    public static void main(String[] args) {
        NameserverConfig nameserverConfig = getNameserverConfig();
        ServerNetworkConfig serverNetworkConfig = getNetworkConfig();
        NameserverController controller = buildController(nameserverConfig, serverNetworkConfig);
        addShutdownHook(controller);
        controller.startup();
        System.out.printf("nameserver startup success: %s%n", serverNetworkConfig.getListenPort());
    }

    private static NameserverConfig getNameserverConfig() {
        NameserverConfig nameserverConfig = new NameserverConfig();
        if (nameserverConfig.getHome() == null) {
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

    private static NameserverController buildController(NameserverConfig nameserverConfig, ServerNetworkConfig serverNetworkConfig) {
        return new NameserverController(nameserverConfig, serverNetworkConfig);
    }

    private static void addShutdownHook(NameserverController controller) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("nameserver shutdown...");
            long startTime = System.currentTimeMillis();
            try {
                controller.shutdown();
            } catch (Exception e) {
                log.error("nameserver shutdown exception", e);
            }
            System.out.printf("nameserver shutdown elapsed %dmS%n", System.currentTimeMillis() - startTime);
        }));
    }
}
