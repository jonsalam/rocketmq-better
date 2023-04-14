package com.clouditora.top.nameserver.config;

import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.FileUtil;
import com.clouditora.mq.common.util.JsonUtil;
import com.clouditora.top.nameserver.NameserverConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @link org.apache.rocketmq.namesrv.kvconfig.ConfigMapManager
 */
@Slf4j
public class ConfigMapManager extends AbstractNothingService {
    private final String configPath;
    /**
     * namespace: [key: value]
     *
     * @link org.apache.rocketmq.namesrv.kvconfig.ConfigMapManager#configMap
     */
    private final Map<String, Map<String, String>> configMap = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static final String NAMESPACE_ORDER_TOPIC_CONFIG = "ORDER_TOPIC_CONFIG";

    public ConfigMapManager(NameserverConfig nameserverConfig) {
        this.configPath = nameserverConfig.getKvConfigPath();
    }

    public static class ConfigMap {
        Map<String, Map<String, String>> configMap;
    }

    @Override
    public String getServiceName() {
        return "ConfigMapManager";
    }

    @Override
    protected void init() throws Exception {
        String json = FileUtil.file2String(configPath);
        ConfigMap configMap = JsonUtil.parse(json, ConfigMap.class);
        this.configMap.putAll(configMap.configMap);
        super.init();
    }

    public void put(String namespace, String key, String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                Map<String, String> map = this.configMap.computeIfAbsent(namespace, k -> new HashMap<>());
                String prev = map.put(key, value);
                if (prev == null) {
                    log.info("create config: namespace={}, key={}, value={}", namespace, key, value);
                } else {
                    log.info("update config: namespace={}, key={}, value={}", namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update config interrupted", e);
        }
        persist();
    }

    public String get(String namespace, String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                Map<String, String> kvTable = this.configMap.get(namespace);
                if (null != kvTable) {
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("get config interrupted", e);
        }
        return null;
    }

    public void delete(String namespace, String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                Map<String, String> map = this.configMap.get(namespace);
                if (map != null) {
                    String value = map.remove(key);
                    log.info("delete config: namespace={}, key={}, value={}", namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteKVConfig interrupted", e);
        }
        persist();
    }

    public byte[] getValues(String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                Map<String, String> map = this.configMap.get(namespace);
                if (map != null) {
                    return JsonUtil.toBytes(map);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getValues interrupted", e);
        }

        return null;
    }

    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                byte[] bytes = JsonUtil.toBytes(this.configMap);
                FileUtil.overwriteFile(bytes, this.configPath);
            } catch (IOException e) {
                log.info("persist config: path={}", this.configPath);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist interrupted", e);
        }

    }
}
