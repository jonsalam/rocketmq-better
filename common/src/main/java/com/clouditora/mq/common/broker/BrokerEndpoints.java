package com.clouditora.mq.common.broker;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 同一个brokerName可以对应多个brokerId, 组成集群, 其中brokerId=0的是主, 其他的是从
 *
 * @link org.apache.rocketmq.common.protocol.route.BrokerData
 */
@Slf4j
@Getter
public class BrokerEndpoints implements Comparable<BrokerEndpoints> {
    private final String cluster;
    private final String brokerName;
    /**
     * id: endpoint
     */
    @JSONField(name = "brokerAddrs")
    private final HashMap<Long, String> endpointMap = new HashMap<>();

    public BrokerEndpoints(String cluster, String brokerName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.route.BrokerData#compareTo
     */
    @Override
    public int compareTo(BrokerEndpoints o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String get(Long brokerId) {
        return this.endpointMap.get(brokerId);
    }

    public void put(Long id, String endpoint) {
        Long prevId = removeByEndpoint(endpoint);
        if (prevId != null && !prevId.equals(id)) {
            log.info("remove same broker endpoint: endpoint={}, id={}", endpoint, id);
        }
        this.endpointMap.put(id, endpoint);
    }

    public void removeById(long id) {
        String prev = this.endpointMap.remove(id);
        log.debug("remove broker endpoint: id={}, prev={}", id, prev);
    }

    public boolean isEmpty() {
        return this.endpointMap.isEmpty();
    }

    public boolean containsEndpoint(String endpoint) {
        for (Map.Entry<Long, String> entry : this.endpointMap.entrySet()) {
            if (endpoint.equals(entry.getValue())) {
                return true;
            }
        }
        return false;
    }

    public Long removeByEndpoint(String endpoint) {
        Iterator<Map.Entry<Long, String>> iterator = this.endpointMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (endpoint.equals(entry.getValue())) {
                // remove same endpoint: maybe switch slave to master
                iterator.remove();
                return entry.getKey();
            }
        }
        return null;
    }
}
