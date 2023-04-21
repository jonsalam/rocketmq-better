package com.clouditora.mq.common.command;

public interface JsonBodyHeader<T extends JsonBody> {
    default T decode(byte[] body) {
        if (body == null) {
            try {
                return (T) getClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                return null;
            }
        }
        return (T) JsonBody.decode(body, getClass());
    }
}
