package com.clouditora.mq.client.broker;

import com.clouditora.mq.network.Client;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
public class BrokerApi {
    private final ExecutorService executor;

    public BrokerApi(Client client, ExecutorService executor) {
        this.executor = executor;
    }

    public void unregisterConsumer(String group) {

    }
}
