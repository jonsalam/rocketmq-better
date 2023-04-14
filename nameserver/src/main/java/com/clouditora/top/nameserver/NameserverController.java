package com.clouditora.top.nameserver;

import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.Server;
import com.clouditora.mq.network.ServerNetworkConfig;
import com.clouditora.top.nameserver.listener.RouteInfoListener;
import com.clouditora.top.nameserver.processor.DefaultRequestProcessor;
import com.clouditora.top.nameserver.config.ConfigMapManager;
import com.clouditora.top.nameserver.route.RouteInfoManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @link org.apache.rocketmq.namesrv.NamesrvController
 */
public class NameserverController extends AbstractNothingService {
    private final NameserverConfig nameserverConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final ConfigMapManager configManager;
    private final RouteInfoManager routeInfoManager;
    private final ScheduledExecutorService scheduledExecutor;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingExecutor
     */
    private ExecutorService defaultExecutor;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingServer
     */
    private Server server;

    public NameserverController(NameserverConfig nameserverConfig, ServerNetworkConfig serverNetworkConfig) {
        this.nameserverConfig = nameserverConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.configManager = new ConfigMapManager(nameserverConfig);
        this.routeInfoManager = new RouteInfoManager();
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));

        this.defaultExecutor = ThreadUtil.newFixedThreadPool(serverNetworkConfig.getServerWorkerThreads(), getServiceName() + ":Laborer");
        RouteInfoListener routeInfoListener = new RouteInfoListener(routeInfoManager);
        this.server = new Server(serverNetworkConfig, routeInfoListener);
        this.server.setDefaultProcessor(new DefaultRequestProcessor(), this.defaultExecutor);
    }

    @Override
    public String getServiceName() {
        return "nameserver";
    }

    @Override
    public void startup() {
        this.configManager.startup();

    }

    @Override
    public void shutdown() {
        this.configManager.shutdown();
    }

}
