package com.clouditora.mq.nameserver.route;

import com.clouditora.mq.common.route.BrokerEndpoint;
import com.clouditora.mq.common.route.BrokerName;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.nameserver.broker.BrokerAlive;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager
 */
@Slf4j
public class RouteInfoManager extends AbstractScheduledService {
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    /**
     * brokerName: BrokerEndpoint
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#brokerAddrTable
     */
    private final Map<String, BrokerEndpoint> brokerEndpointMap;
    /**
     * clusterName: BrokerName
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#clusterAddrTable
     */
    private final Map<String, BrokerName> brokerNameMap;
    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#brokerLiveTable
     */
    private final HashMap<String, BrokerAlive> brokerAliveMap;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public RouteInfoManager() {
        this.brokerEndpointMap = new HashMap<>(128);
        this.brokerNameMap = new HashMap<>(32);
        this.brokerAliveMap = new HashMap<>(256);
    }

    @Override
    public String getServiceName() {
        return "RouteInfoManager";
    }

    @Override
    protected void init() throws Exception {
        register(TimeUnit.SECONDS, 5, 10, RouteInfoManager.this::evictInactiveBroker);
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#registerBroker
     */
    public void registerBroker(String cluster, String brokerNameStr, String endpointStr, long brokerId, Channel channel) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("register broker: cluster={}, broker={}, endpoint={}, id={}", cluster, brokerNameStr, endpointStr, brokerId);

                BrokerName brokerName = this.brokerNameMap.computeIfAbsent(cluster, k -> new BrokerName());
                brokerName.add(brokerNameStr);
                log.info("register cluster: {}", brokerName);

                BrokerEndpoint endpoint = this.brokerEndpointMap.computeIfAbsent(brokerNameStr, k -> new BrokerEndpoint(cluster, brokerNameStr));
                endpoint.put(brokerId, endpointStr);
                log.info("register broker: {}", endpoint);

                BrokerAlive brokerAlive = new BrokerAlive();
                brokerAlive.setEndpoint(endpointStr);
                brokerAlive.setChannel(channel);
                this.brokerAliveMap.put(endpointStr, brokerAlive);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register broker exception", e);
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#unregisterBroker
     */
    public void unregisterBroker(String cluster, String brokerNameStr, String endpointStr, long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("unregister broker: cluster={}, broker={}, id={}", cluster, brokerNameStr, brokerId);

                this.brokerAliveMap.remove(endpointStr);

                BrokerEndpoint endpoint = this.brokerEndpointMap.get(endpointStr);
                boolean isEmpty = endpoint.removeById(brokerId);
                if (isEmpty) {
                    this.brokerEndpointMap.remove(brokerNameStr);
                    BrokerName brokerName = this.brokerNameMap.get(cluster);
                    if (brokerName != null) {
                        isEmpty = brokerName.remove(brokerNameStr);
                        if (isEmpty) {
                            this.brokerNameMap.remove(cluster);
                            log.info("unregister cluster: {}", cluster);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregister broker exception", e);
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#onChannelDestroy
     */
    public void unregisterBroker(String address, Channel channel) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("close channel: address={}, channel={}", address, channel);

                String endpointStr = Optional.ofNullable(getEndpointByChannel(channel)).orElse(address);
                if (StringUtils.isBlank(endpointStr)) {
                    log.warn("close channel: broker endpoint is null");
                    return;
                }

                this.brokerAliveMap.remove(endpointStr);

                BrokerEndpoint endpoint = findEndpointByEndpointStr(endpointStr);
                if (endpoint != null) {
                    endpoint.removeByEndpoint(endpointStr);
                    if (endpoint.isEmpty()) {
                        Iterator<Map.Entry<String, BrokerName>> it = this.brokerNameMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<String, BrokerName> entry = it.next();
                            BrokerName brokerName = entry.getValue();
                            boolean removed = brokerName.remove(endpoint.getBrokerName());
                            if (removed && brokerName.isEmpty()) {
                                it.remove();
                                log.info("unregister cluster: {}", entry.getKey());
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("close channel exception", e);
        }
    }

    private BrokerEndpoint findEndpointByEndpointStr(String address) {
        for (Map.Entry<String, BrokerEndpoint> entry : this.brokerEndpointMap.entrySet()) {
            Long id = entry.getValue().findIdByEndpoint(address);
            if (id != null) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String getEndpointByChannel(Channel channel) {
        if (channel == null) {
            return null;
        }
        return this.brokerAliveMap.values().stream()
                .filter(e -> e.getChannel() == channel)
                .findFirst()
                .map(BrokerAlive::getEndpoint)
                .orElse(null);
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#scanNotActiveBroker
     */
    public void evictInactiveBroker() {
        Iterator<Map.Entry<String, BrokerAlive>> it = this.brokerAliveMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerAlive> next = it.next();
            String brokerAddress = next.getKey();
            BrokerAlive info = next.getValue();
            if (System.currentTimeMillis() - info.getUpdateTime() > BROKER_CHANNEL_EXPIRED_TIME) {
                Channel channel = info.getChannel();
                CoordinatorUtil.closeChannel(channel);
                it.remove();
                log.warn("{} {} expired", brokerAddress, channel);
                unregisterBroker(brokerAddress, channel);
            }
        }
    }

}
