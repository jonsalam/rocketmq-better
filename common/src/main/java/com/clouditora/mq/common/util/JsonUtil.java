package com.clouditora.mq.common.util;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class JsonUtil {
    public static String toJsonString(Object obj, JSONWriter.Feature... features) {
        String json = JSONObject.toJSONString(obj, features);
        if (StringUtils.isBlank(json)) {
            return null;
        }
        return json;
    }

    public static String toJsonStringPretty(Object obj) {
        return toJsonString(obj, JSONWriter.Feature.PrettyFormat);
    }

    public static byte[] toJsonBytes(Object obj) {
        String json = toJsonString(obj);
        if (json == null) {
            return null;
        }
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T toJsonObject(byte[] bytes, Class<T> clazz) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return JSONObject.parseObject(json, clazz);
    }

    public static <T> T toJsonObject(String json, Class<T> clazz) {
        return JSONObject.parseObject(json, clazz);
    }
}
