package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;
import com.clouditora.mq.store.util.UtilAll;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public abstract class AbstractHostSerializer implements Serializer {
    private ByteBuffer host;

    @Override
    public void preSerializer(SerializerChain chain) {
        MessageEntity message = chain.getMessage();
        InetSocketAddress address = getAddress(message);
        host = UtilAll.socketAddress2ByteBuffer(address);
        chain.addMessageLength(host.limit());
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byteBuffer.put(host);
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        MessageEntity message = chain.getMessage();
        ByteBuffer byteBuffer = chain.getByteBuffer();

        int sysFlag = message.getSysFlag();
        int length = getHostLength(sysFlag);
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        int port = byteBuffer.getInt();
        try {
            InetSocketAddress address = new InetSocketAddress(InetAddress.getByAddress(bytes), port);
            setAddress(message, address);
            chain.next();
        } catch (UnknownHostException e) {
            throw new RuntimeException(String.format("读取Host失败, sysFlag=%s, pos=%s", sysFlag, byteBuffer.position() - length));
        }
    }

    protected abstract InetSocketAddress getAddress(MessageEntity message);

    protected abstract int getHostLength(int sysFlag);

    protected abstract void setAddress(MessageEntity message, InetSocketAddress address);
}
