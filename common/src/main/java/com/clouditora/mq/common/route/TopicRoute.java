package com.clouditora.mq.common.route;

import lombok.Data;

import java.util.List;

@Data
public class TopicRoute {
    private List<BrokerEndpoint> brokers;
}
