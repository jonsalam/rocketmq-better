package com.clouditora.mq.common.topic;

import com.clouditora.mq.common.constant.PermitBit;
import com.clouditora.mq.common.constant.TopicFilterType;
import lombok.Data;

/**
 * @link org.apache.rocketmq.common.TopicConfig
 */
@Data
public class TopicConfig {
    private static final String SEPARATOR = " ";

    private String topicName;
    private int readQueueNums = 16;
    private int writeQueueNums = 16;
    private int perm = PermitBit.RW;
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    private int topicSysFlag = 0;
    private boolean order = false;

    public TopicConfig(String topic) {
        this.topicName = topic;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.perm);
        sb.append(SEPARATOR);
        sb.append(this.topicFilterType);

        return sb.toString();
    }

    public boolean decode(String str) {
        String[] split = str.split(SEPARATOR);
        if (split.length == 5) {
            this.topicName = split[0];
            this.readQueueNums = Integer.parseInt(split[1]);
            this.writeQueueNums = Integer.parseInt(split[2]);
            this.perm = Integer.parseInt(split[3]);
            this.topicFilterType = TopicFilterType.valueOf(split[4]);
            return true;
        }
        return false;
    }
}
