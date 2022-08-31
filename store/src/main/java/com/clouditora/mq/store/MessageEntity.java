package com.clouditora.mq.store;

import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true, exclude = "propertyBytes")
public class MessageEntity extends Message {
    private String brokerName;
    private int messageLength;
    private int magicCode;
    private int bodyCRC;
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
    private byte[] propertyBytes;

    @Override
    public void setBody(byte[] body) {
        this.body = body;
        this.bodyCRC = StoreUtil.crc32(body);
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
        this.propertyBytes = bytes;

        String properties = new String(bytes, StandardCharsets.UTF_8);
        Map<String, String> map = StoreUtil.string2Properties(properties);
        super.setProperties(map);
    }

    public byte[] getPropertyBytes() {
        if (this.propertyBytes != null) {
            return this.propertyBytes;
        }
        return StoreUtil.properties2Bytes(properties);
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        super.setProperties(properties);
        this.propertyBytes = StoreUtil.properties2Bytes(properties);
    }
}
