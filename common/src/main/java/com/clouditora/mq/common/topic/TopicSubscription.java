package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.constant.ExpressionType;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @link org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData
 */
@Data
public class TopicSubscription {
    public final static String SUB_ALL = "*";

    private String topic;

    @JSONField(name = "subString")
    private String expression;

    private ExpressionType expressionType = ExpressionType.TAG;

    @JSONField(name = "tagsSet")
    private Set<String> tags;

    @JSONField(name = "codeSet")
    private Set<Integer> codes;

    @JSONField(name = "subVersion")
    private long version = System.currentTimeMillis();

    public static TopicSubscription build(String topic, String expression, ExpressionType expressionType) throws Exception {
        if (expressionType == null || expressionType == ExpressionType.TAG) {
            return buildTagSubscription(topic, expression);
        }
        return null;
    }

    private static TopicSubscription buildTagSubscription(String topic, String expression) throws Exception {
        TopicSubscription subscription = new TopicSubscription();
        subscription.setTopic(topic);

        if (StringUtils.isBlank(expression) || expression.equals(SUB_ALL)) {
            subscription.setExpression(SUB_ALL);
        } else {
            Set<String> tags = new HashSet<>();
            Set<Integer> codes = new HashSet<>();
            subscription.setExpression(expression);
            subscription.setTags(tags);
            subscription.setCodes(codes);

            String[] split = expression.split("\\|\\|");
            if (split.length == 0) {
                throw new Exception("subString split error");
            }
            for (String tag : split) {
                String trim = tag.trim();
                if (trim.length() > 0) {
                    tags.add(trim);
                    codes.add(trim.hashCode());
                }
            }
        }
        return subscription;
    }
}
