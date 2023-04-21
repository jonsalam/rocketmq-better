package com.clouditora.mq.starter;

import com.clouditora.mq.starter.config.RocketMqListenerBeanPostProcessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
@ConditionalOnProperty(value = "rocketmq.enable", havingValue = "true", matchIfMissing = true)
public class RocketMqConfiguration {

    @ConditionalOnProperty(value = "rocketmq.consumer.enable", matchIfMissing = true, havingValue = "true")
    @ConditionalOnMissingBean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public RocketMqListenerBeanPostProcessor rocketMqListenerBeanPostProcessor() {
        return new RocketMqListenerBeanPostProcessor();
    }
}
