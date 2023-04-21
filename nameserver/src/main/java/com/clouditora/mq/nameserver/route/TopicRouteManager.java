package com.clouditora.mq.nameserver.route;

import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.broker.BrokerNames;
import com.clouditora.mq.common.broker.BrokerQueue;
import com.clouditora.mq.common.broker.BrokerQueues;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.nameserver.broker.BrokerAlive;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager
 */
@Slf4j
public class TopicRouteManager extends AbstractScheduledService {
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 120_000;
    /**
     * clusterName: BrokerName
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#clusterAddrTable
     */
    private final Map<String, BrokerNames> clusterMap = new HashMap<>(128);
    /**
     * brokerName: BrokerEndpoint
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#brokerAddrTable
     */
    private final Map<String, BrokerEndpoints> brokerEndpointMap = new HashMap<>(32);
    /**
     * endpoint: BrokerAlive
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#brokerLiveTable
     */
    private final Map<String, BrokerAlive> brokerAliveMap = new HashMap<>(256);
    /**
     * topic: TopicRoute
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#topicQueueTable
     */
    private final Map<String, BrokerQueues> topicRouteMap = new HashMap<>(1024);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public String getServiceName() {
        return "RouteManager";
    }

    @Override
    public void startup() {
        scheduled(TimeUnit.SECONDS, 5, 10, TopicRouteManager.this::cleanExpiredBroker);
        scheduled(TimeUnit.SECONDS, 5, 30, TopicRouteManager.this::printRoute);
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#registerBroker
     */
    public void registerBroker(String cluster, String brokerName, String brokerEndpoint, long brokerId, ConcurrentMap<String, TopicQueue> topicMap, Channel channel) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("register broker: cluster={}, broker={}, endpoint={}, id={}", cluster, brokerName, brokerEndpoint, brokerId);

                BrokerNames names = this.clusterMap.computeIfAbsent(cluster, e -> {
                    log.info("register cluster: {}", cluster);
                    return new BrokerNames();
                });
                names.add(brokerName);

                BrokerEndpoints endpoints = this.brokerEndpointMap.computeIfAbsent(brokerName, e -> {
                    log.info("register broker: {}", brokerName);
                    return new BrokerEndpoints(cluster, brokerName);
                });
                endpoints.put(brokerId, brokerEndpoint);

                registerTopicRoute(brokerName, brokerId, topicMap);

                BrokerAlive brokerAlive = new BrokerAlive();
                brokerAlive.setEndpoint(brokerEndpoint);
                brokerAlive.setChannel(channel);
                this.brokerAliveMap.put(brokerEndpoint, brokerAlive);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register broker exception", e);
        }
    }

    /**
     * endpoint经过NetworkUtil#getLocalIp处理, 不如channel准确
     * 也可能处理的是client的channel
     *
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#onChannelDestroy
     */
    public void unregisterBroker(String endpoint, Channel channel) {
        endpoint = this.brokerAliveMap.values().stream()
                .filter(e -> e.getChannel() == channel)
                .findFirst()
                .map(BrokerAlive::getEndpoint)
                .orElse(endpoint);
        unregisterBroker(endpoint);
    }

    private void unregisterBroker(String endpoint) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                BrokerAlive alive = this.brokerAliveMap.remove(endpoint);
                BrokerEndpoints endpoints = findBrokerEndpoints(endpoint);
                if (endpoints != null) {
                    log.info("unregister broker: endpoint={}, alive={}", endpoint, alive);
                    endpoints.removeByEndpoint(endpoint);
                    if (endpoints.isEmpty()) {
                        removeBrokerEndpoint(endpoints.getBrokerName(), endpoints.getCluster());
                    }

                    unregisterTopicRoute(endpoints.getBrokerName());
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregister broker exception", e);
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#unregisterBroker
     */
    public void unregisterBroker(String cluster, String brokerName, String brokerEndpoint, long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                log.info("unregister broker: cluster={}, broker={}, id={}", cluster, brokerName, brokerId);

                this.brokerAliveMap.remove(brokerEndpoint);
                BrokerEndpoints endpoints = this.brokerEndpointMap.get(brokerName);
                if (endpoints == null) {
                    log.info("unregister broker: broker {} not exists", brokerName);
                } else {
                    endpoints.removeById(brokerId);
                    if (endpoints.isEmpty()) {
                        removeBrokerEndpoint(brokerName, cluster);
                    }

                    unregisterTopicRoute(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregister broker exception", e);
        }
    }

    private void removeBrokerEndpoint(String endpoints, String endpoints1) {
        this.brokerEndpointMap.remove(endpoints);
        BrokerNames names = this.clusterMap.get(endpoints1);
        if (names != null) {
            names.remove(endpoints);
            if (names.isEmpty()) {
                this.clusterMap.remove(endpoints1);
                log.info("unregister cluster: {}", endpoints1);
            }
        }
    }

    private BrokerEndpoints findBrokerEndpoints(String endpoint) {
        return this.brokerEndpointMap.values().stream()
                .filter(e -> e.containsEndpoint(endpoint))
                .findFirst()
                .orElse(null);
    }

    /**
     * 同一个brokerName只有mater才能更新topicRoute
     */
    private void registerTopicRoute(String brokerName, long brokerId, ConcurrentMap<String, TopicQueue> topicMap) {
        if (brokerId != GlobalConstant.MASTER_ID) {
            return;
        }
        topicMap.forEach((topic, config) -> {
            BrokerQueues brokerQueues = this.topicRouteMap.computeIfAbsent(topic, e -> {
                log.info("register topic {}: {}", topic, config);
                return new BrokerQueues();
            });

            BrokerQueue brokerQueue = new BrokerQueue();
            brokerQueue.setBrokerName(brokerName);
            brokerQueue.setWriteNum(config.getWriteQueueNum());
            brokerQueue.setReadNum(config.getWriteQueueNum());
            BrokerQueue prevQueue = brokerQueues.put(brokerName, brokerQueue);
            if (prevQueue != null && !prevQueue.equals(brokerQueue)) {
                log.info("change topic {}: prev={}, now={}", topic, prevQueue, brokerQueue);
            }
        });
    }

    private void unregisterTopicRoute(String brokerName) {
        Iterator<Map.Entry<String, BrokerQueues>> routeIterator = this.topicRouteMap.entrySet().iterator();
        while (routeIterator.hasNext()) {
            Map.Entry<String, BrokerQueues> routeEntry = routeIterator.next();
            String topic = routeEntry.getKey();
            BrokerQueues brokerQueues = routeEntry.getValue();
            boolean isEmpty = brokerQueues.removeByBrokerName(brokerName);
            if (isEmpty) {
                routeIterator.remove();
                log.info("unregister route: {}@{}", topic, brokerName);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#scanNotActiveBroker
     */
    public void cleanExpiredBroker() {
        Iterator<Map.Entry<String, BrokerAlive>> it = this.brokerAliveMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerAlive> next = it.next();
            BrokerAlive alive = next.getValue();
            if (alive.isInactive(BROKER_CHANNEL_EXPIRED_TIME)) {
                Channel channel = alive.getChannel();
                NetworkUtil.closeChannel(channel);
                it.remove();
                log.warn("broker {} expired", alive.getEndpoint());
                unregisterBroker(alive.getEndpoint(), channel);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.kvconfig.KVConfigManager#printAllPeriodically
     */
    public void printRoute() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");
                this.topicRouteMap.forEach((topic, config) -> {
                    config.getQueueMap().forEach((brokerName, queue) -> {
                        log.info("topic {}: brokerName={}, queue={}", topic, brokerName, queue);
                    });
                });
                log.info("--------------------------------------------------------");
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printRoute interrupted", e);
        }
    }
}
