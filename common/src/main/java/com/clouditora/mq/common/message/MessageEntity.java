package com.clouditora.mq.common.message;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.constant.ClassType;
import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.util.MessageUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @link org.apache.rocketmq.common.message.MessageExt
 * @link org.apache.rocketmq.store.MessageExtBrokerInner
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class MessageEntity extends Message {
    /**
     * @link org.apache.rocketmq.common.message.MessageExt#msgId
     */
    @JSONField(name = "msgId")
    private String messageId;
    /**
     * @link org.apache.rocketmq.common.message.MessageExt#storeSize
     */
    @JSONField(name = "storeSize")
    private int messageLength;
    private String brokerName;
    private int queueId;
    private long queuePosition;
    private int sysFlag;
    private long bornTimestamp;
    private InetSocketAddress bornHost;
    private long storeTimestamp;
    private InetSocketAddress storeHost;
    private long commitLogOffset;
    /**
     * @link org.apache.rocketmq.common.message.MessageExt#reconsumeTimes
     */
    @JSONField(name = "reconsumeTimes")
    private int reConsumeTimes;
    /**
     * @link org.apache.rocketmq.common.message.MessageExt#preparedTransactionOffset
     */
    @JSONField(name = "preparedTransactionOffset")
    private long transactionOffset;
    private int bodyCrc;
    @JSONField(serialize = false, deserialize = false)
    private MagicCode magicCode;

    public MessageEntity(String topic, String tags, String keys, byte[] body) {
        super(topic, tags, keys, body);
    }

    @Override
    public void setBody(byte[] body) {
        this.body = body;
        this.bodyCrc = MessageUtil.crc32(body);
    }

    public void setBornHost(InetSocketAddress bornHost) {
        this.bornHost = bornHost;
        if (bornHost.getAddress() instanceof Inet6Address) {
            this.sysFlag |= MessageConst.SysFlg.BORN_HOST_V6_FLAG;
        } else {
            this.sysFlag &= ~MessageConst.SysFlg.BORN_HOST_V6_FLAG;
        }
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
        if (storeHost.getAddress() instanceof Inet6Address) {
            this.sysFlag |= MessageConst.SysFlg.STORE_HOST_V6_FLAG;
        } else {
            this.sysFlag &= ~MessageConst.SysFlg.STORE_HOST_V6_FLAG;
        }
    }

    public void setPropertyBytes(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        Map<String, String> map = MessageUtil.string2Properties(string);
        super.setProperties(map);
    }

    public <T> T getProperty(String property, Class<T> clazz, T defaultValue) {
        String value = this.properties.get(property);
        if (value == null) {
            return defaultValue;
        }
        return ClassType.parseValue(clazz, value);
    }
}
