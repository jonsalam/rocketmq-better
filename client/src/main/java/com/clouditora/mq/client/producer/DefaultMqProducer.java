package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.MqProducer;
import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.client.facade.BrokerApiFacade;
import com.clouditora.mq.client.topic.TopicPublishInfo;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
public class DefaultMqProducer extends AbstractNothingService implements MqProducer {
    protected ProducerConfig producerConfig;
    protected BrokerApiFacade brokerApiFacade;

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#topicPublishInfoTable
     */
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    private ExecutorService asyncSenderExecutor;


    public DefaultMqProducer(String producerConfig) {
        super();
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(producerConfig);

        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
        int poolSize = Runtime.getRuntime().availableProcessors();
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.asyncSenderThreadPoolQueue,
                ThreadUtil.buildFactory("AsyncSenderExecutor", poolSize)
        );
    }

    @Override
    public void setNameServer(String address) {
        this.producerConfig.setNameserver(address);
    }

    @Override
    public String getServiceName() {
        return "Producer";
    }

    @Override
    protected void init() throws Exception {
        initClientInstance();

    }

    @Override
    public SendResult send(Message message) throws MqClientException, InterruptedException {
        return send(message, RpcModel.SYNC, null, producerConfig.getSendMsgTimeout());
    }

    private SendResult send(Message message, RpcModel rpcModel, Object callback, int timeout) {
        return null;
    }

    private void initClientInstance() {

    }

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#createTopic
     */
    public void createTopic(String key, String topic, int queueNum) throws MqClientException {
        createTopic(key, topic, queueNum, 0);
    }

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#createTopic
     */
    private void createTopic(String key, String topic, int queueNum, int flag) throws MqClientException {
        brokerApiFacade.createTopic(key, topic, queueNum, flag);
    }
}
