package com.clouditora.mq.starter.config;

import com.clouditora.mq.starter.RocketMqConsumer;
import com.clouditora.mq.starter.RocketMqConsumerEndpointRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.HashSet;
import java.util.Set;

public class RocketMqListenerBeanPostProcessor implements BeanFactoryAware, BeanFactoryPostProcessor, DestructionAwareBeanPostProcessor, Ordered, SmartInitializingSingleton {
    private ConfigurableListableBeanFactory configurableListableBeanFactory;
    private BeanFactory beanFactory;

    private RocketMqConsumerEndpointRegistry registry = new RocketMqConsumerEndpointRegistry();
    private Set<Class<?>> consumerClasses = new HashSet<>();

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        this.configurableListableBeanFactory = beanFactory;
    }

    @Override
    public void afterSingletonsInstantiated() {

    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (consumerClasses.contains(bean.getClass())) {
            return bean;
        }
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        RocketMqConsumer annotation = AnnotationUtils.findAnnotation(targetClass, RocketMqConsumer.class);
        if (annotation == null) {
            return bean;
        }
        consumerClasses.add(targetClass);
        registry.register();
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) throws BeansException {
        RocketMqConsumer consumer = getConsumer(bean, beanName);
        if (consumer == null) {
            return;
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
