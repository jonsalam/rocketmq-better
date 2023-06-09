package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.constant.SystemTopic;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * part of @link org.apache.rocketmq.client.producer.DefaultMQProducer
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class ProducerConfig extends ClientConfig {
    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved.
     * <p>
     * For non-transactional messages, it does not matter as long as it's unique per process.
     * <p>
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">core concepts</a> for more discussion.
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     */
    private String createTopicKey = SystemTopic.AUTO_CREATE_TOPIC.getTopic();

    /**
     * Number of queues to create per default topic.
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
     * <p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode.
     * <p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message body size in bytes.
     */
    private int maxMessageSize = 4 * 1024 * 1024;
}
