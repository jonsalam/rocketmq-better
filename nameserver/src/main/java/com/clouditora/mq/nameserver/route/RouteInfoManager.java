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
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 120_000;
    /**
     * clusterName: BrokerName
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#clusterAddrTable
     */
    private final Map<String, BrokerName> brokerNameMap;
    /**
     * brokerName: BrokerEndpoint
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#brokerAddrTable
     */
    private final Map<String, BrokerEndpoint> brokerEndpointMap;
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
    public void startup() {
        scheduled(TimeUnit.SECONDS, 5, 10, RouteInfoManager.this::evictInactiveBroker);
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#registerBroker
     */
    public void registerBroker(String cluster, String name, String endpoint, long id, Channel channel) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("register broker: cluster={}, broker={}, endpoint={}, id={}", cluster, name, endpoint, id);

                BrokerName names = this.brokerNameMap.computeIfAbsent(cluster, k -> {
                    log.info("register cluster: {}", cluster);
                    return new BrokerName();
                });
                names.add(name);

                BrokerEndpoint endpoints = this.brokerEndpointMap.computeIfAbsent(name, k -> {
                    log.info("register broker: {}", name);
                    return new BrokerEndpoint(cluster, name);
                });
                endpoints.put(id, endpoint);

                BrokerAlive brokerAlive = new BrokerAlive();
                brokerAlive.setEndpoint(endpoint);
                brokerAlive.setChannel(channel);
                this.brokerAliveMap.put(endpoint, brokerAlive);
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
    public void unregisterBroker(String cluster, String name, String endpoint, long id) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("unregister broker: cluster={}, broker={}, id={}", cluster, name, id);

                this.brokerAliveMap.remove(endpoint);

                BrokerEndpoint endpoints = this.brokerEndpointMap.get(endpoint);
                boolean isEmpty = endpoints.removeById(id);
                if (isEmpty) {
                    this.brokerEndpointMap.remove(name);
                    BrokerName names = this.brokerNameMap.get(cluster);
                    if (names != null) {
                        isEmpty = names.remove(name);
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
    public void unregisterBroker(String endpoint, Channel channel) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("close channel: endpoint={}, channel={}", endpoint, channel);

                endpoint = Optional.ofNullable(getEndpointByChannel(channel)).orElse(endpoint);
                if (StringUtils.isBlank(endpoint)) {
                    log.warn("close channel: broker endpoint is null");
                    return;
                }

                this.brokerAliveMap.remove(endpoint);

                BrokerEndpoint endpoints = findEndpointByEndpointStr(endpoint);
                if (endpoints != null) {
                    endpoints.removeByEndpoint(endpoint);
                    if (endpoints.isEmpty()) {
                        Iterator<Map.Entry<String, BrokerName>> it = this.brokerNameMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<String, BrokerName> entry = it.next();
                            BrokerName brokerName = entry.getValue();
                            boolean removed = brokerName.remove(endpoints.getName());
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

    private BrokerEndpoint findEndpointByEndpointStr(String endpoint) {
        this.brokerEndpointMap.values().stream()
                .filter(e -> e.findIdByEndpoint(endpoint)!=null)
                .findFirst()
                .orElse(null);
        for (Map.Entry<String, BrokerEndpoint> entry : this.brokerEndpointMap.entrySet()) {
            Long id = entry.getValue().findIdByEndpoint(endpoint);
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
            BrokerAlive alive = next.getValue();
            if (alive.isInactive(BROKER_CHANNEL_EXPIRED_TIME)) {
                Channel channel = alive.getChannel();
                CoordinatorUtil.closeChannel(channel);
                it.remove();
                log.warn("broker {} expired", alive.getEndpoint());
                unregisterBroker(alive.getEndpoint(), channel);
            }
        }
    }

}
