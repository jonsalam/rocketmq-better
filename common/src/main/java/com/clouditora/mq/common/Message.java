package com.clouditora.mq.common;

import com.clouditora.mq.common.util.MessageUtil;
import lombok.Data;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @link org.apache.rocketmq.common.message.Message
 */
@Data
@ToString(exclude = "body")
public class Message {
    protected String topic;
    protected int flag;
    protected Map<String, String> properties;
    protected byte[] body;
    protected String transactionId;

    public Message() {
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this.topic = topic;
        this.body = body;
        setTags(tags);
        setKeys(keys);
    }

    public void putProperty(String name, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(name, value);
    }

    public String getProperty(String name) {
        if (this.properties == null) {
            return null;
        }
        return this.properties.get(name);
    }

    public void setTags(String tags) {
        this.putProperty(MessageConst.Property.TAGS, tags);
    }

    public void setKeys(String keys) {
        this.putProperty(MessageConst.Property.KEYS, keys);
    }

    public void setKeys(Collection<String> collection) {
        String keys = String.join(MessageConst.Property.Separator.KEY, collection);
        this.setKeys(keys);
    }

    public String properties2String() {
        return MessageUtil.properties2String(getProperties());
    }
}
