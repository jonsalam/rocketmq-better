package com.clouditora.mq.starter;

import com.clouditora.mq.starter.constant.MessageConsumeType;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMqConsumer {
    /**
     * id用户标示每一个独立的Consumer，配置文件中可以根据id来为每个Consumer增加独立的配置项。
     * 一个应用内id不能相同。
     */
    String id();

    /**
     * consumer所属组，默认为 ${spring.application.name} + "_" + RocketMQListener.id()
     * 当同一个应用内两个Consumer需要独立消费同一个topic时需要为两个Consumer分别指定不同的group
     */
    String group() default "";

    String topic();

    String[] tags();

    MessageConsumeType consumeType() default MessageConsumeType.Concurrently;
}
