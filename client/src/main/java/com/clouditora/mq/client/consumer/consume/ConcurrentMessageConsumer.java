package com.clouditora.mq.client.consumer.consume;

import com.clouditora.mq.client.consumer.ConsumerConfig;
import com.clouditora.mq.client.consumer.listener.ConcurrentMessageListener;
import com.clouditora.mq.common.message.MessageEntity;

import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService
 */
public class ConcurrentMessageConsumer extends AbstractMessageConsumer {
    private final ConcurrentMessageListener messageListener;

    public ConcurrentMessageConsumer(ConsumerConfig config, ConcurrentMessageListener messageListener) {
        super(config);
        this.messageListener = messageListener;
    }

    @Override
    public String getServiceName() {
        return "ConcurrentMessageConsumer";
    }

    @Override
    public void startup() {
        scheduled(TimeUnit.MINUTES, super.config.getConsumeTimeout(), super.config.getConsumeTimeout(), this::cleanExpiredMessages);
        super.startup();
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#cleanExpireMsg
     */
    private void cleanExpiredMessages() {

    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#submitConsumeRequest
     */
    public void consume(MessageEntity message) {
        ConsumeResult result = new ConsumeResult();
        try {
            ConsumeStatus status = messageListener.consume(message);
            if(status==null){
                result.set
            }
        } catch (Exception e) {

        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#submitConsumeRequestLater
     */
    public void consumeLater(MessageEntity message){

    }
}
