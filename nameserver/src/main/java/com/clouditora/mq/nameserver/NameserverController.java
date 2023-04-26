package com.clouditora.mq.nameserver;

import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.nameserver.listener.ChannelListener;
import com.clouditora.mq.nameserver.processor.DefaultRequestProcessor;
import com.clouditora.mq.nameserver.route.RouteInfoManager;
import com.clouditora.mq.network.Server;
import com.clouditora.mq.network.ServerNetworkConfig;

import java.util.concurrent.ExecutorService;

/**
 * @link org.apache.rocketmq.namesrv.NamesrvController
 */
public class NameserverController extends AbstractNothingService {
    private final NameserverConfig nameserverConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final RouteInfoManager routeInfoManager;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingExecutor
     */
    private final ExecutorService defaultExecutor;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingServer
     */
    private final Server server;

    public NameserverController(NameserverConfig nameserverConfig, ServerNetworkConfig serverNetworkConfig) {
        this.nameserverConfig = nameserverConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.routeInfoManager = new RouteInfoManager();

        this.defaultExecutor = ThreadUtil.newFixedThreadPool(serverNetworkConfig.getServerWorkerThreads(), getServiceName() + "#DefaultExecutor");
        ChannelListener channelListener = new ChannelListener(routeInfoManager);
        this.server = new Server(serverNetworkConfig, channelListener);
        this.server.setDefaultProcessor(new DefaultRequestProcessor(routeInfoManager), this.defaultExecutor);
    }

    @Override
    public String getServiceName() {
        return "Nameserver";
    }

    @Override
    public void startup() {
        this.routeInfoManager.startup();
        this.server.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.routeInfoManager.shutdown();
        this.server.shutdown();
        super.shutdown();
    }

}
