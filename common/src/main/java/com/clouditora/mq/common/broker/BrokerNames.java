package com.clouditora.mq.common.broker;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Data
public class BrokerNames {
    private Set<String> names = new HashSet<>();

    public void add(String name) {
        this.names.add(name);
    }

    public void remove(String name) {
        boolean removed = this.names.remove(name);
        log.info("remove broker name: name={}, removed={}", name, removed);
    }

    public boolean isEmpty() {
        return this.names.isEmpty();
    }

}
