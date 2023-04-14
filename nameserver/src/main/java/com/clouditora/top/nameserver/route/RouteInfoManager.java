package com.clouditora.top.nameserver.route;

import com.clouditora.mq.common.command.body.TopicRouteData;
import com.clouditora.mq.common.route.BrokerData;
import com.clouditora.mq.common.route.QueueData;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.network.util.CoordinatorUtil;
import com.clouditora.top.nameserver.broker.BrokerLiveInfo;
import com.clouditora.top.nameserver.listener.RouteInfoListener;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
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
     * topic: brokerName
     */
    private final Map<String, Map<String, QueueData>> topicQueueTable;
    /**
     * brokerName: BrokerData
     */
    private final Map<String, BrokerData> brokerAddrTable;
    /**
     * clusterName: [brokerName]
     */
    private final Map<String, Set<String>> clusterAddrTable;
    /**
     * brokerAddress: BrokerLiveInfo
     */
    private final Map<String, BrokerLiveInfo> brokerLiveTable;
    /**
     * brokerAddress: [Filter Server]
     */
    private final Map<String, List<String>> filterServerTable;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public RouteInfoManager() {
        super(TimeUnit.SECONDS, 5, 10);
        this.topicQueueTable = new HashMap<>(1024);
        this.brokerAddrTable = new HashMap<>(128);
        this.clusterAddrTable = new HashMap<>(32);
        this.brokerLiveTable = new HashMap<>(256);
        this.filterServerTable = new HashMap<>(256);
    }

    @Override
    public String getServiceName() {
        return "RouteInfoManager";
    }

    @Override
    protected void run() throws Exception {

    }

    public int scanNotActiveBroker() {
        int removeCount = 0;
        Iterator<Map.Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                Channel channel = next.getValue().getChannel();
                CoordinatorUtil.closeChannel(channel);
                it.remove();
                log.warn("channel {} expired: {}/{}ms", channel, next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                this.onChannelDestroy(next.getKey(), channel);
                removeCount++;
            }
        }

        return removeCount;
    }

    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddress = getBrokerAddress(channel);
        if (brokerAddress == null) {
            brokerAddress = remoteAddr;
        } else {
            log.info("destroy channel: {}", brokerAddress);
        }

        if (StringUtils.isBlank(brokerAddress)) {
            log.warn("destroy channel: brokerAddress is null");
            return;
        }
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.brokerLiveTable.remove(brokerAddress);
                this.filterServerTable.remove(brokerAddress);

                String brokerNameFound = null;
                boolean removeBrokerName = false;
                Iterator<Map.Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                while (itBrokerAddrTable.hasNext() && (brokerNameFound == null)) {
                    BrokerData brokerData = itBrokerAddrTable.next().getValue();
                    Iterator<Map.Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<Long, String> entry = it.next();
                        Long brokerId = entry.getKey();
                        if (entry.getValue().equals(brokerAddress)) {
                            brokerNameFound = brokerData.getBrokerName();
                            it.remove();
                            log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed", brokerId, brokerAddress);
                            break;
                        }
                    }

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        removeBrokerName = true;
                        itBrokerAddrTable.remove();
                        log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed", brokerData.getBrokerName());
                    }
                }

                if (brokerNameFound != null && removeBrokerName) {
                    Iterator<Map.Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Set<String>> entry = it.next();
                        String clusterName = entry.getKey();
                        Set<String> brokerNames = entry.getValue();
                        boolean removed = brokerNames.remove(brokerNameFound);
                        if (removed) {
                            log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed", brokerNameFound, clusterName);

                            if (brokerNames.isEmpty()) {
                                log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster", clusterName);
                                it.remove();
                            }
                            break;
                        }
                    }
                }

                if (removeBrokerName) {
                    String finalBrokerNameFound = brokerNameFound;
                    Set<String> needRemoveTopic = new HashSet<>();

                    topicQueueTable.forEach((topic, queueDataMap) -> {
                        QueueData old = queueDataMap.remove(finalBrokerNameFound);
                        log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, old);

                        if (queueDataMap.size() == 0) {
                            log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                            needRemoveTopic.add(topic);
                        }
                    });
                    needRemoveTopic.forEach(topicQueueTable::remove);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("onChannelDestroy Exception", e);
        }
    }

    private String getBrokerAddress(Channel channel) {
        if (channel == null) {
            return null;
        }
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                return brokerLiveTable.values().stream()
                        .filter(e -> e.getChannel() == channel)
                        .findFirst()
                        .map(BrokerLiveInfo::getBrokerAddress)
                        .orElse(null);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("channel destroy exception", e);
        }
        return null;
    }

    /**
     * @link org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#pickupTopicRouteData
     */
    public TopicRouteData getTopicRouteData(String topic) {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                if (queueDataMap != null) {
                    TopicRouteData topicRouteData = new TopicRouteData();
                    topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                    List<BrokerData> brokerDataList = new LinkedList<>();
                    topicRouteData.setBrokerDatas(brokerDataList);
                    Map<String, List<String>> filterServerMap = new HashMap<>();
                    topicRouteData.setFilterServerTable(filterServerMap);

                    for (String brokerName : queueDataMap.keySet()) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (brokerData == null) {
                            continue;
                        }
                        brokerDataList.add(brokerData);
                        log.debug("[nameserver] brokerData={}", brokerData);
                        for (String brokerAddr : brokerData.getBrokerAddrs().values()) {
                            List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                            // only add filter server list when not null
                            if (filterServerList != null) {
                                filterServerMap.put(brokerAddr, filterServerList);
                                log.debug("[nameserver] filterServerList={}", filterServerList);
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(brokerDataList)) {
                        log.debug("[nameserver] topicRouteData={}", topicRouteData);
                        return topicRouteData;
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getTopicRouteData exception", e);
        }
        return null;
    }
}
