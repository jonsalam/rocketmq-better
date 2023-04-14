package com.clouditora.mq.client.facade;

import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.client.util.TopicValidator;
import com.clouditora.mq.common.command.body.TopicRouteData;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.route.BrokerData;
import com.clouditora.mq.common.topic.TopicConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @link org.apache.rocketmq.client.impl.MQAdminImpl
 * @link org.apache.rocketmq.client.impl.MQClientAPIImpl
 */
@Slf4j
public class BrokerApiFacade {
    private BrokerRpcFacade brokerRpcFacade;
    private long timeout = 6000;

    /**
     * @link org.apache.rocketmq.client.impl.MQAdminImpl#createTopic
     */
    public void createTopic(String defaultTopic, String topic, int queueNum, int flag) throws MqClientException {
        try {
            TopicValidator.checkTopic(topic);
            TopicRouteData topicRouteData = brokerRpcFacade.getTopicRouteInfoFromNameServer(topic, this.timeout);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (CollectionUtils.isEmpty(brokerDataList)) {
                throw new MqClientException(String.format("topic [%s] create failed: no broker found", topic));
            }
            for (BrokerData brokerData : brokerDataList) {
                String address = brokerData.getBrokerAddrs().get(GlobalConstant.MASTER_ID);
                if (StringUtils.isBlank(address)) {
                    continue;
                }
                TopicConfig topicConfig = new TopicConfig(topic);
                topicConfig.setReadQueueNums(queueNum);
                topicConfig.setWriteQueueNums(queueNum);
                topicConfig.setTopicSysFlag(flag);
                brokerRpcFacade.createTopic(address, defaultTopic, topicConfig, this.timeout);
            }
        } catch (Exception e) {
            throw new MqClientException(String.format("topic [%s] create failed", topic), e);
        }
    }

}
