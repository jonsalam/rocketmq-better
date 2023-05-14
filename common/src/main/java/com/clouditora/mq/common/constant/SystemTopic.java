package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.common.topic.TopicValidator
 */
@Getter
@AllArgsConstructor
public enum SystemTopic {
    AUTO_CREATE_TOPIC("TBW102"),
    SCHEDULE_TOPIC("SCHEDULE_TOPIC_XXXX"),
    BENCHMARK_TOPIC("BenchmarkTest"),
    TRANS_HALF_TOPIC("RMQ_SYS_TRANS_HALF_TOPIC"),
    TRACE_TOPIC("RMQ_SYS_TRACE_TOPIC"),
    TRANS_OP_HALF_TOPIC("RMQ_SYS_TRANS_OP_HALF_TOPIC"),
    TRANS_CHECK_MAX_TIME_TOPIC("TRANS_CHECK_MAX_TIME_TOPIC"),
    SELF_TEST_TOPIC("SELF_TEST_TOPIC"),
    OFFSET_MOVED_EVENT("OFFSET_MOVED_EVENT"),
    ;

    private String topic;

    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    private static final Set<String> SYSTEM_TOPIC_SET;
    private static final boolean[] VALID_CHARACTERS;

    static {
        SYSTEM_TOPIC_SET = Arrays.stream(SystemTopic.values()).map(SystemTopic::getTopic).collect(Collectors.toSet());

        VALID_CHARACTERS = new boolean[128];
        VALID_CHARACTERS['%'] = true;
        VALID_CHARACTERS['-'] = true;
        VALID_CHARACTERS['_'] = true;
        VALID_CHARACTERS['|'] = true;
        for (int i = '0'; i < '9'; i++) {
            VALID_CHARACTERS[i] = true;
        }
        for (int i = 'a'; i < 'z'; i++) {
            VALID_CHARACTERS[i] = true;
        }
        for (int i = 'A'; i < 'Z'; i++) {
            VALID_CHARACTERS[i] = true;
        }
    }

    /**
     * @link org.apache.rocketmq.common.topic.TopicValidator#isTopicOrGroupIllegal
     */
    public static boolean validCharacters(String string) {
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if (ch <= 0) {
                return false;
            }
            if (ch >= VALID_CHARACTERS.length) {
                return false;
            }
            if (!VALID_CHARACTERS[ch]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @link org.apache.rocketmq.common.topic.TopicValidator#isSystemTopic(java.lang.String)
     */
    public static boolean isSystemTopic(String topic) {
        return SYSTEM_TOPIC_SET.contains(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    public static void addSystemTopic(String topic) {
        SYSTEM_TOPIC_SET.add(topic);
    }
}
