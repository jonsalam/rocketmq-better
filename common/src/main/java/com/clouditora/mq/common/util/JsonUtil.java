package com.clouditora.mq.common.util;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class JsonUtil {
    public static byte[] toBytes(Object obj) {
        String json = JSONObject.toJSONString(obj);
        if (StringUtils.isBlank(json)) {
            return null;
        }
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T parse(byte[] bytes, Class<T> clazz) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return JSONObject.parseObject(json, clazz);
    }

    public static <T> T parse(String json, Class<T> clazz) {
        return JSONObject.parseObject(json, clazz);
    }
}
