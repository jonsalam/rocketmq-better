package com.clouditora.mq.client.topic;

import com.clouditora.mq.client.util.ThreadLocalIndex;
import com.clouditora.mq.common.command.body.TopicRouteData;
import com.clouditora.mq.common.message.MessageQueue;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @link org.apache.rocketmq.client.impl.producer.TopicPublishInfo
 */
@Data
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    private TopicRouteData topicRouteData;
    private List<MessageQueue> messageQueueList = new ArrayList<>();
    private ThreadLocalIndex queueIndex = new ThreadLocalIndex();
}
