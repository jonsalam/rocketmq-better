package com.clouditora.mq.common.broker;

import lombok.Data;
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
@Data
public class BrokerEndpoints implements Comparable<BrokerEndpoints> {
    private String cluster;
    private String brokerName;
    /**
     * id: endpoint
     */
    private HashMap<Long, String> endpointMap;

    public BrokerEndpoints(String cluster, String brokerName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.endpointMap = new HashMap<>();
    }

    /**
     * @link org.apache.rocketmq.common.protocol.route.BrokerData#compareTo
     */
    @Override
    public int compareTo(BrokerEndpoints o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public void put(Long id, String endpoint) {
        Long prevId = removeByEndpoint(endpoint);
        if (prevId != null && !prevId.equals(id)) {
            log.info("remove same broker endpoint: endpoint={}, id={}", endpoint, id);
        }
        this.endpointMap.put(id, endpoint);
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

    public void removeById(long id) {
        String prev = this.endpointMap.remove(id);
        log.debug("remove broker endpoint: id={}, prev={}", id, prev);
    }

    public boolean isEmpty() {
        return this.endpointMap.isEmpty();
    }
}
