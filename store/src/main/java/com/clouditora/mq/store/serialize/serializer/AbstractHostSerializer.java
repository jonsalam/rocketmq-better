package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;
import com.clouditora.mq.store.util.StoreUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public abstract class AbstractHostSerializer implements Serializer {
    private ByteBuffer host;

    @Override
    public int fieldLength(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        InetSocketAddress address = getAddress(message);
        this.host = StoreUtil.socketAddress2ByteBuffer(address);
        return this.host.limit();
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.put(this.host);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        MessageEntity message = context.getMessage();
        ByteBuffer byteBuffer = context.getByteBuffer();

        int sysFlag = message.getSysFlag();
        int length = getHostLength(sysFlag);
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        int port = byteBuffer.getInt();
        try {
            InetSocketAddress address = new InetSocketAddress(InetAddress.getByAddress(bytes), port);
            setAddress(message, address);
            context.next();
        } catch (UnknownHostException e) {
            throw new RuntimeException(String.format("读取Host失败, sysFlag=%s, pos=%s", sysFlag, byteBuffer.position() - length));
        }
    }

    protected abstract void setAddress(MessageEntity message, InetSocketAddress address);

    protected abstract InetSocketAddress getAddress(MessageEntity message);

    protected abstract int getHostLength(int sysFlag);
}
