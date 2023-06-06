package com.clouditora.mq.common;

public interface MessageConst {
    interface Maximum {
        int MESSAGE_LENGTH = 1024 * 1024 * 4;
        int TOPIC_LENGTH = Byte.MAX_VALUE;
        int PROPERTIES_LENGTH = Short.MAX_VALUE;
        /**
         * File at the end of the minimum fixed length empty
         */
        int MIN_BLANK_LENGTH = 4 + 4;
    }

    interface MagicCode {
        /**
         * Message's MAGIC CODE daa320a7 = 0xAABBCCDD ^ 1880681586 + 8
         * 标记某一段为消息, 即: [msgId, MESSAGE_MAGIC_CODE, 消息]
         *
         * @link org.apache.rocketmq.store.CommitLog#MESSAGE_MAGIC_CODE
         */
        int MESSAGE = -626843481;

        /**
         * End of file empty MAGIC CODE cbd43194 = 0xBBCCDDEE ^ 1880681586 + 8
         * 标记某一段为空白, 即: [msgId, BLANK_MAGIC_CODE, 空白]
         * 当CommitLog无法容纳消息时，使用该类型结尾
         *
         * @link org.apache.rocketmq.store.CommitLog#BLANK_MAGIC_CODE
         */
        int BLANK = -875286124;
    }

    interface Property {
        String TAGS = "TAGS";
        String KEYS = "KEYS";
        String MESSAGE_ID = "UNIQ_KEY";
        String CLUSTER = "CLUSTER";

        interface Separator {
            String KEY = " ";
            char NAME_VALUE = 1;
            char PROPERTY = 2;
        }
    }

    /**
     * Meaning of each bit in the system flag
     * <p>
     * | bit    | 7 | 6 | 5         | 4        | 3           | 2                | 1                | 0                |
     * |--------|---|---|-----------|----------|-------------|------------------|------------------|------------------|
     * | byte 1 |   |   | STOREHOST | BORNHOST | TRANSACTION | TRANSACTION      | MULTI_TAGS       | COMPRESSED       |
     * | byte 2 |   |   |           |          |             | COMPRESSION_TYPE | COMPRESSION_TYPE | COMPRESSION_TYPE |
     * | byte 3 |   |   |           |          |             |                  |                  |                  |
     * | byte 4 |   |   |           |          |             |                  |                  |                  |
     */
    interface SysFlg {
        /**
         * 标记位 - 压缩
         */
        int COMPRESSED_FLAG = 0x1;
        int MULTI_TAGS_FLAG = 0x1 << 1;
        /**
         * 事务类型 - 非事务
         */
        int TRANSACTION_NOT_TYPE = 0;
        /**
         * 事务类型 - 事务准备
         */
        int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
        /**
         * 事务类型 - 提交
         */
        int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
        /**
         * 事务类型 - 回滚
         * = TRANSACTION_PREPARED_TYPE | TRANSACTION_COMMIT_TYPE
         */
        int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

        int BORN_HOST_V6_FLAG = 0x1 << 4;
        int STORE_HOST_V6_FLAG = 0x1 << 5;
        // Mark the flag for batch to avoid conflict
        int NEED_UNWRAP_FLAG = 0x1 << 6;
        int INNER_BATCH_FLAG = 0x1 << 7;

        static int bornHostLength(int flag) {
            return (flag & BORN_HOST_V6_FLAG) == 0 ? 4 : 16;
        }

        static int storeHostLength(int flag) {
            return (flag & STORE_HOST_V6_FLAG) == 0 ? 4 : 16;
        }
    }
}
