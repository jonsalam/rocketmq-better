package com.clouditora.mq.store.consume;

import lombok.Data;

@Data
public class ConsumeFileEntity {
    private Long commitLogOffset;
    private Integer messageLength;
    private Long tagsCode;
}
