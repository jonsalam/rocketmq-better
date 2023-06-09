package com.clouditora.mq.common.constant;

public interface ClientErrorCode {
    int CONNECT_BROKER_EXCEPTION = 10001;
    int ACCESS_BROKER_TIMEOUT = 10002;
    int BROKER_NOT_EXIST_EXCEPTION = 10003;
    int NO_NAME_SERVER_EXCEPTION = 10004;
    int NOT_FOUND_TOPIC_EXCEPTION = 10005;
    int REQUEST_TIMEOUT_EXCEPTION = 10006;
    int CREATE_REPLY_MESSAGE_EXCEPTION = 10007;
}