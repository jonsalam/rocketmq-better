package com.clouditora.mq.network.protocol;

import com.clouditora.mq.common.constant.CodeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @link org.apache.rocketmq.common.protocol.ResponseCode
 * @link org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode
 */
@Getter
@AllArgsConstructor
public enum ResponseCode implements CodeEnum {
    SUCCESS(0),
    SYSTEM_ERROR(1),
    SYSTEM_BUSY(2),
    NOT_SUPPORTED(3),
    TRANSACTION_FAILED(4),

    FLUSH_DISK_TIMEOUT(10),
    SLAVE_NOT_AVAILABLE(11),
    FLUSH_SLAVE_TIMEOUT(12),
    MESSAGE_ILLEGAL(13),
    SERVICE_NOT_AVAILABLE(14),
    VERSION_NOT_SUPPORTED(15),
    NO_PERMISSION(16),
    TOPIC_NOT_EXIST(17),
    TOPIC_EXIST_ALREADY(18),
    PULL_NOT_FOUND(19),
    PULL_RETRY_IMMEDIATELY(20),
    PULL_OFFSET_MOVED(21),
    QUERY_NOT_FOUND(22),
    SUBSCRIPTION_PARSE_FAILED(23),
    SUBSCRIPTION_NOT_EXIST(24),
    SUBSCRIPTION_NOT_LATEST(25),
    SUBSCRIPTION_GROUP_NOT_EXIST(26),
    FILTER_DATA_NOT_EXIST(27),
    FILTER_DATA_NOT_LATEST(28),
    TRANSACTION_SHOULD_COMMIT(200),
    TRANSACTION_SHOULD_ROLLBACK(201),
    TRANSACTION_STATE_UNKNOW(202),
    TRANSACTION_STATE_GROUP_WRONG(203),
    NO_BUYER_ID(204),
    NOT_IN_CURRENT_UNIT(205),
    CONSUMER_NOT_ONLINE(206),
    CONSUME_MSG_TIMEOUT(207),
    NO_MESSAGE(208),
    UPDATE_AND_CREATE_ACL_CONFIG_FAILED(209),
    DELETE_ACL_CONFIG_FAILED(210),
    UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED(211),
    ;

    private final int code;
}
