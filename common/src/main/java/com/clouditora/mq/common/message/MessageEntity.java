package com.clouditora.mq.common.message;

import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.util.MessageUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @link org.apache.rocketmq.common.message.MessageExt
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MessageEntity extends Message {
    private String brokerName;
    private int messageLength;
    private int magicCode;
    private int bodyCrc;
    private int queueId;
    private long queueOffset;
    private long logOffset;
    private int sysFlag;
    private long bornTimestamp;
    private InetSocketAddress bornHost;
    private long storeTimestamp;
    private InetSocketAddress storeHost;
    private int reConsumeTimes;
    private long transactionOffset;
    private String messageId;

    public MessageEntity() {

    }

    public MessageEntity(String topic, String tags, String keys, byte[] body) {
        super(topic, tags, keys, body);
    }

    @Override
    public void setBody(byte[] body) {
        this.body = body;
        this.bodyCrc = StoreUtil.crc32(body);
    }

    public void setBornHost(InetSocketAddress bornHost) {
        this.bornHost = bornHost;
        if (bornHost.getAddress() instanceof Inet6Address) {
            this.sysFlag = this.sysFlag | MessageConst.SysFlg.BORN_HOST_V6_FLAG;
        }
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
        if (storeHost.getAddress() instanceof Inet6Address) {
            this.sysFlag = this.sysFlag | MessageConst.SysFlg.STORE_HOST_V6_FLAG;
        }
    }

    public void setPropertyBytes(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        Map<String, String> map = MessageUtil.string2Properties(string);
        super.setProperties(map);
    }

    public byte[] getPropertyBytes() {
        return MessageUtil.properties2Bytes(properties);
    }

}
