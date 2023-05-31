package com.clouditora.mq.network.protocol;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.constant.ClassCanonical;
import com.clouditora.mq.common.constant.SerializeType;
import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.util.JsonUtil;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @link org.apache.rocketmq.remoting.protocol.RemotingCommand
 */
@Slf4j
@ToString(exclude = "body")
@Data
public class Command {
    private int code;
    private LanguageCode language = LanguageCode.JAVA;
    private int version = 0;
    private int opaque = REQUEST_ID.getAndIncrement();
    private int flag = 0;
    private String remark;
    private Map<String, String> extFields;
    /**
     * @see Command#header 是一个接口, 无法直接反序列化
     */
    @JSONField(name = "customHeader")
    private transient CommandHeader header;
    private transient byte[] body;
    @JSONField(name = "serializeTypeCurrentRPC")
    private SerializeType serializeType = SerializeType.JSON;

    private static final int BIT_RESPONSE = 1 << 0;
    private static final int BIT_ONEWAY = 1 << 1;
    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);
    private static final Map<Class<? extends CommandHeader>, Field[]> HEADER_FIELD_MAP = new HashMap<>();

    public void setCode(int code) {
        this.code = code;
    }

    public void setCode(ResponseCode code) {
        this.code = code.getCode();
    }

    public CommandType getType() {
        if (isResponseType()) {
            return CommandType.RESPONSE;
        }
        return CommandType.REQUEST;
    }

    public void markResponseType() {
        this.flag |= BIT_RESPONSE;
    }

    public boolean isResponseType() {
        return (this.flag & BIT_RESPONSE) == BIT_RESPONSE;
    }

    public void markOneway() {
        this.flag |= BIT_ONEWAY;
    }

    public boolean isOneway() {
        return (this.flag & BIT_ONEWAY) == BIT_ONEWAY;
    }

    private Field[] getClassFields(Class<? extends CommandHeader> clazz) {
        return HEADER_FIELD_MAP.computeIfAbsent(clazz, e -> clazz.getDeclaredFields());
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RemotingCommand#makeCustomHeaderToNet
     */
    public void encodeHeader() {
        if (this.header == null) {
            return;
        }
        if (this.extFields == null) {
            this.extFields = new HashMap<>();
        }

        Field[] fields = getClassFields(this.header.getClass());
        for (Field field : fields) {
            String name = getFieldName(field);
            if (name == null) {
                continue;
            }
            try {
                field.setAccessible(true);
                Object value = field.get(this.header);
                if (value != null) {
                    this.extFields.put(name, value.toString());
                }
            } catch (UnsupportedOperationException e) {
                this.extFields = new HashMap<>(this.extFields);
                encodeHeader();
            } catch (Exception e) {
                log.error("header to ext field exception", e);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RemotingCommand#decodeCommandCustomHeader
     */
    public <T extends CommandHeader> T decodeHeader(Class<T> clazz) {
        if (MapUtils.isEmpty(this.extFields)) {
            return null;
        }
        T instance;
        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("decode header exception", e);
            return null;
        }

        Field[] fields = getClassFields(clazz);
        for (Field field : fields) {
            String name = getFieldName(field);
            if (name == null) {
                continue;
            }
            String value = this.extFields.get(name);
            if (StringUtils.isBlank(value)) {
                continue;
            }
            try {
                field.setAccessible(true);
                Object fieldValue = ClassCanonical.parseValue(field.getType().getCanonicalName(), value);
                if (fieldValue != null) {
                    field.set(instance, fieldValue);
                }
            } catch (IllegalAccessException e) {
                log.error("decode header field exception", e);
            }
        }
        return instance;
    }

    private static String getFieldName(Field field) {
        if (Modifier.isStatic(field.getModifiers())) {
            return null;
        }
        String name = field.getName();
        if (name.startsWith("this")) {
            return null;
        }
        JSONField jsonField = field.getAnnotation(JSONField.class);
        if (jsonField != null && jsonField.name() != null) {
            name = jsonField.name();
        }
        return name;
    }

    public <T> T decodeBody(Class<T> clazz) {
        if (this.serializeType == SerializeType.JSON) {
            return JsonUtil.toJsonObject(this.body, clazz);
        } else {
            return null;
        }
    }

    public static Command buildRequest(int code, CommandHeader header) {
        Command command = new Command();
        command.setCode(code);
        command.setHeader(header);
        return command;
    }

    public static Command buildRequest(RequestCode code, CommandHeader header) {
        return buildRequest(code.getCode(), header);
    }

    public static Command buildResponse(int code, String remark) {
        Command command = new Command();
        command.markResponseType();
        command.setCode(code);
        command.setRemark(remark);
        return command;
    }

    public static Command buildResponse(ResponseCode code, String remark) {
        return buildResponse(code.getCode(), remark);
    }

    public static Command buildResponse(ResponseCode code) {
        return buildResponse(code.getCode(), null);
    }
}
