package com.clouditora.mq.common.command.header;

import lombok.Data;

@Data
public class CreateTopicRequestHeader implements CommandHeader{
    private String topic;
    private String defaultTopic;
    private Integer readQueueNums;
    private Integer writeQueueNums;
    private Integer perm;
    private String topicFilterType;
    private Integer topicSysFlag;
    private Boolean order = false;
}
