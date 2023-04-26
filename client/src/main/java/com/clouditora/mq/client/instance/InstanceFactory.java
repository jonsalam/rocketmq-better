package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.ClientConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.client.impl.MQClientManager
 */
public class InstanceFactory {
    private static final ConcurrentMap<String, ClientInstance> CACHE = new ConcurrentHashMap<>();
    private static final InstanceFactory instance = new InstanceFactory();

    private InstanceFactory() {
    }

    /**
     * #link org.apache.rocketmq.client.impl.MQClientManager#getOrCreateMQClientInstance
     */
    public static ClientInstance create(ClientConfig config) {
        String clientId = config.buildClientId();
        return CACHE.computeIfAbsent(clientId, k -> new ClientInstance(config));
    }
}
