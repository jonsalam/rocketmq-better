package com.clouditora.mq.common.route;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Data
public class BrokerName {
    private Set<String> brokerNames = new HashSet<>();

    public void add(String brokerName) {
        this.brokerNames.add(brokerName);
    }

    public boolean remove(String brokerName) {
        boolean removed = this.brokerNames.remove(brokerName);
        log.info("remove broker name: name={}, removed={}", brokerName, removed);
        return removed;
    }

    public boolean isEmpty() {
        return this.brokerNames.isEmpty();
    }

}
