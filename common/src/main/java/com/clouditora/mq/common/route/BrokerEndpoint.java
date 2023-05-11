package com.clouditora.mq.common.route;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @link org.apache.rocketmq.common.protocol.route.BrokerData
 */
@Slf4j
@Data
public class BrokerEndpoint {
    private String cluster;
    private String name;
    /**
     * id: endpoint
     */
    private HashMap<Long, String> endpointMap;

    public BrokerEndpoint(String cluster, String name) {
        this.cluster = cluster;
        this.name = name;
        this.endpointMap = new HashMap<>();
    }

    public void put(Long id, String endpoint) {
        Long prevId = removeByEndpoint(endpoint);
        if (!id.equals(prevId)) {
            log.info("remove same broker endpoint: endpoint={}, id={}", endpoint, id);
        }
        this.endpointMap.put(id, endpoint);
    }

    public Long findIdByEndpoint(String endpoint) {
        for (Map.Entry<Long, String> entry : this.endpointMap.entrySet()) {
            if (entry.getValue().equals(endpoint)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public Long removeByEndpoint(String endpoint) {
        Iterator<Map.Entry<Long, String>> iterator = this.endpointMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (entry.getValue().equals(endpoint)) {
                // remove same endpoint: maybe switch slave to master
                iterator.remove();
                return entry.getKey();
            }
        }
        return null;
    }

    public boolean removeById(long id) {
        String prev = this.endpointMap.remove(id);
        log.info("remove broker endpoint: id={}, prev={}", id, prev);
        return this.endpointMap.isEmpty();
    }

    public boolean isEmpty() {
        return this.endpointMap.isEmpty();
    }
}
