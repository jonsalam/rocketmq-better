package com.clouditora.mq.common.command;

import com.clouditora.mq.common.constant.CodeEnum;
import com.clouditora.mq.common.constant.SerializeType;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @link org.apache.rocketmq.common.protocol.RequestCode
 */
@Getter
@AllArgsConstructor
public enum RequestCode implements CodeEnum {
    SEND_MESSAGE(10, null),
    PULL_MESSAGE(11, null),
    QUERY_MESSAGE(12, null),
    QUERY_BROKER_OFFSET(13, null),
    QUERY_CONSUMER_OFFSET(14, null),
    UPDATE_CONSUMER_OFFSET(15, null),
    UPDATE_AND_CREATE_TOPIC(17, SerializeType.JSON),
    GET_ALL_TOPIC_CONFIG(21, null),
    GET_TOPIC_CONFIG_LIST(22, null),
    GET_TOPIC_NAME_LIST(23, null),
    UPDATE_BROKER_CONFIG(25, null),
    GET_BROKER_CONFIG(26, null),
    TRIGGER_DELETE_FILES(27, null),
    GET_BROKER_RUNTIME_INFO(28, null),
    SEARCH_OFFSET_BY_TIMESTAMP(29, null),
    GET_MAX_OFFSET(30, null),
    GET_MIN_OFFSET(31, null),
    GET_EARLIEST_MSG_STORETIME(32, null),
    VIEW_MESSAGE_BY_ID(33, null),
    HEART_BEAT(34, null),
    UNREGISTER_CLIENT(35, null),
    CONSUMER_SEND_MSG_BACK(36, null),
    END_TRANSACTION(37, null),
    GET_CONSUMER_LIST_BY_GROUP(38, null),
    CHECK_TRANSACTION_STATE(39, null),
    NOTIFY_CONSUMER_IDS_CHANGED(40, null),
    LOCK_BATCH_MQ(41, null),
    UNLOCK_BATCH_MQ(42, null),
    GET_ALL_CONSUMER_OFFSET(43, null),
    GET_ALL_DELAY_OFFSET(45, null),
    CHECK_CLIENT_CONFIG(46, null),
    UPDATE_AND_CREATE_ACL_CONFIG(50, null),
    DELETE_ACL_CONFIG(51, null),
    GET_BROKER_CLUSTER_ACL_INFO(52, null),
    UPDATE_GLOBAL_WHITE_ADDRS_CONFIG(53, null),
    GET_BROKER_CLUSTER_ACL_CONFIG(54, null),
    PUT_KV_CONFIG(100, null),
    GET_KV_CONFIG(101, null),
    DELETE_KV_CONFIG(102, null),
    REGISTER_BROKER(103, null),
    UNREGISTER_BROKER(104, null),
    GET_ROUTEINFO_BY_TOPIC(105, SerializeType.JSON),
    GET_BROKER_CLUSTER_INFO(106, null),
    UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200, null),
    GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201, null),
    GET_TOPIC_STATS_INFO(202, null),
    GET_CONSUMER_CONNECTION_LIST(203, null),
    GET_PRODUCER_CONNECTION_LIST(204, null),
    WIPE_WRITE_PERM_OF_BROKER(205, null),
    GET_ALL_TOPIC_LIST_FROM_NAMESERVER(206, null),
    DELETE_SUBSCRIPTIONGROUP(207, null),
    GET_CONSUME_STATS(208, null),
    SUSPEND_CONSUMER(209, null),
    RESUME_CONSUMER(210, null),
    RESET_CONSUMER_OFFSET_IN_CONSUMER(211, null),
    RESET_CONSUMER_OFFSET_IN_BROKER(212, null),
    ADJUST_CONSUMER_THREAD_POOL(213, null),
    WHO_CONSUME_THE_MESSAGE(214, null),
    DELETE_TOPIC_IN_BROKER(215, null),
    DELETE_TOPIC_IN_NAMESRV(216, null),
    GET_KVLIST_BY_NAMESPACE(219, null),
    RESET_CONSUMER_CLIENT_OFFSET(220, null),
    GET_CONSUMER_STATUS_FROM_CLIENT(221, null),
    INVOKE_BROKER_TO_RESET_OFFSET(222, null),
    INVOKE_BROKER_TO_GET_CONSUMER_STATUS(223, null),
    GET_TOPICS_BY_CLUSTER(224, null),
    QUERY_TOPIC_CONSUME_BY_WHO(300, null),
    REGISTER_FILTER_SERVER(301, null),
    REGISTER_MESSAGE_FILTER_CLASS(302, null),
    QUERY_CONSUME_TIME_SPAN(303, null),
    GET_SYSTEM_TOPIC_LIST_FROM_NS(304, null),
    GET_SYSTEM_TOPIC_LIST_FROM_BROKER(305, null),
    CLEAN_EXPIRED_CONSUMEQUEUE(306, null),
    GET_CONSUMER_RUNNING_INFO(307, null),
    QUERY_CORRECTION_OFFSET(308, null),
    CONSUME_MESSAGE_DIRECTLY(309, null),
    SEND_MESSAGE_V2(310, null),
    GET_UNIT_TOPIC_LIST(311, null),
    GET_HAS_UNIT_SUB_TOPIC_LIST(312, null),
    GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST(313, null),
    CLONE_GROUP_OFFSET(314, null),
    VIEW_BROKER_STATS_DATA(315, null),
    CLEAN_UNUSED_TOPIC(316, null),
    GET_BROKER_CONSUME_STATS(317, null),
    /**
     * update the config of name server
     */
    UPDATE_NAMESRV_CONFIG(318, null),
    /**
     * get config from name server
     */
    GET_NAMESRV_CONFIG(319, null),
    SEND_BATCH_MESSAGE(320, null),
    QUERY_CONSUME_QUEUE(321, null),
    QUERY_DATA_VERSION(322, null),
    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    RESUME_CHECK_HALF_MESSAGE(323, null),
    SEND_REPLY_MESSAGE(324, null),
    SEND_REPLY_MESSAGE_V2(325, null),
    PUSH_REPLY_MESSAGE_TO_CLIENT(326, null),
    ADD_WRITE_PERM_OF_BROKER(327, null),
    GET_ALL_PRODUCER_INFO(328, null),
    DELETE_EXPIRED_COMMITLOG(329, null),
    ;

    private final int code;
    private final SerializeType serializeType;
}
