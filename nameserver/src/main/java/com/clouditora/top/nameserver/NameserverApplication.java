package com.clouditora.top.nameserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

@Slf4j
public class NameserverApplication {
    public static void main(String[] args) throws Exception {
        NameserverController controller = createController();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown name server");
            long startTime = System.currentTimeMillis();
            try {
                controller.shutdown();
            } catch (Exception e) {
                log.info("shutdown name server exception", e);
            }
            log.info("shutdown name server elapsed {}mS", System.currentTimeMillis() - startTime);
        }));
        controller.startup();
        System.out.println("The Name Server boot success.");
    }

    private static NameserverController createController() throws Exception {
        NameserverConfig nameserverConfig = new NameserverConfig();
        if (nameserverConfig.getRocketmqHome() == null) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", GlobalConstant.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        ServerNetworkConfig networkConfig = new ServerNetworkConfig();

//        initLogger(nameserverConfig);

        return new NameserverController(nameserverConfig, networkConfig);
    }

    private static void initLogger(NameserverConfig nameserverConfig) throws JoranException {
        LoggerContext loggerFactory = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator joranConfigurator = new JoranConfigurator();
        joranConfigurator.setContext(loggerFactory);
        loggerFactory.reset();
        joranConfigurator.doConfigure(nameserverConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
    }
}
