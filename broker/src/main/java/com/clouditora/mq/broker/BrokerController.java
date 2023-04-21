package com.clouditora.mq.broker;

import com.clouditora.mq.broker.facade.NameServerRpcFacade;
import com.clouditora.mq.broker.listener.ChannelListener;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.Server;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BrokerController extends AbstractNothingService {
    private final BrokerConfig brokerConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final ClientNetworkConfig clientNetworkConfig;
    private final ChannelListener channelListener;
    private final NameServerRpcFacade nameServerRpcFacade;
    private Server server;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.clientNetworkConfig = clientNetworkConfig;
        this.channelListener = new ChannelListener();
        this.nameServerRpcFacade = new NameServerRpcFacade(clientNetworkConfig);
        this.server = new Server(this.serverNetworkConfig, this.channelListener);
    }

    @Override
    public String getServiceName() {
        return "Broker";
    }

    @Override
    public void startup() {
        this.server.startup();
        this.nameServerRpcFacade.startup();
        registerBroker();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.server.shutdown();
        this.nameServerRpcFacade.shutdown();
        super.shutdown();
    }

    private void registerBroker() {
        this.nameServerRpcFacade.registerBroker(
                this.brokerConfig.getBrokerClusterName(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerIP1() + ":" + serverNetworkConfig.getListenPort(),
                0L,
                this.brokerConfig.getRegisterBrokerTimeoutMills()
        );
    }
}
