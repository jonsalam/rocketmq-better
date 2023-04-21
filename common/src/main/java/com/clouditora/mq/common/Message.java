package com.clouditora.mq.common;

import lombok.Data;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

    void putProperty(String name, String value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
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
}
