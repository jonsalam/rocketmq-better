package com.clouditora.mq.nameserver;

import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.nameserver.broker.BrokerChannelListener;
import com.clouditora.mq.nameserver.dispatcher.DefaultCommandDispatcher;
import com.clouditora.mq.nameserver.route.TopicRouteManager;
import com.clouditora.mq.network.ServerNetwork;
import com.clouditora.mq.network.ServerNetworkConfig;

import java.util.concurrent.ExecutorService;

/**
 * @link org.apache.rocketmq.namesrv.NamesrvController
 */
public class NameserverController extends AbstractNothingService {
    private final NameserverConfig nameserverConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final TopicRouteManager topicRouteManager;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingExecutor
     */
    private final ExecutorService defaultExecutor;
    /**
     * @link org.apache.rocketmq.namesrv.NamesrvController#remotingServer
     */
    private final ServerNetwork serverNetwork;

    public NameserverController(NameserverConfig nameserverConfig, ServerNetworkConfig serverNetworkConfig) {
        this.nameserverConfig = nameserverConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.topicRouteManager = new TopicRouteManager();

        this.defaultExecutor = ThreadUtil.newFixedThreadPool(serverNetworkConfig.getServerWorkerThreads(), getServiceName() + "#DefaultExecutor");
        BrokerChannelListener channelListener = new BrokerChannelListener(topicRouteManager);
        this.serverNetwork = new ServerNetwork(serverNetworkConfig, channelListener);
        this.serverNetwork.setDefaultDispatcher(new DefaultCommandDispatcher(topicRouteManager), this.defaultExecutor);
    }

    @Override
    public String getServiceName() {
        return "Nameserver";
    }

    @Override
    public void startup() {
        this.topicRouteManager.startup();
        this.serverNetwork.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.topicRouteManager.shutdown();
        this.serverNetwork.shutdown();
        super.shutdown();
    }

}
