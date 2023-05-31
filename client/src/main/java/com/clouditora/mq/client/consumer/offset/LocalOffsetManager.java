package com.clouditora.mq.client.consumer.offset;

import com.clouditora.mq.client.consumer.ConsumerConfig;
import com.clouditora.mq.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore
 */
@Slf4j
public class LocalOffsetManager extends AbstractOffsetManager {
    public LocalOffsetManager(String clientId, String group) {
        super(group, "%s/%s/%s/offsets.json".formatted(ConsumerConfig.LOCAL_OFFSET_STORE_DIR, clientId, group));
    }

    @Override
    public String getServiceName() {
        return LocalOffsetManager.class.getSimpleName();
    }

    @Override
    protected void decode(String content) {
        LocalOffsetFile offset = JsonUtil.toJsonObject(content, LocalOffsetFile.class);
        super.offsetMap = offset.getOffsetMap();
        offset.getOffsetMap().forEach((k, v) -> {
            log.info("load local consumer offset: group={}, {}={}", super.group, k, v.get());
        });
    }

    @Override
    protected String encode() {
        LocalOffsetFile file = new LocalOffsetFile();
        file.setOffsetMap(super.offsetMap);
        return JsonUtil.toJsonStringPretty(file);
    }
}
