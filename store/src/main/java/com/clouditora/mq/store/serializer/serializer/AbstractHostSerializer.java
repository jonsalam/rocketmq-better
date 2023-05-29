package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;
import com.clouditora.mq.store.util.StoreUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public abstract class AbstractHostSerializer implements Serializer {
    private ByteBuffer host;

    @Override
    public void preSerializer(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        InetSocketAddress address = getAddress(message);
        host = StoreUtil.socketAddress2ByteBuffer(address);
        context.addMessageLength(host.limit());
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.put(host);
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

    protected abstract InetSocketAddress getAddress(MessageEntity message);

    protected abstract int getHostLength(int sysFlag);

    protected abstract void setAddress(MessageEntity message, InetSocketAddress address);
}
