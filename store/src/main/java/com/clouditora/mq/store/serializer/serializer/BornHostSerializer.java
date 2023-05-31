package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;

import java.net.InetSocketAddress;

public class BornHostSerializer extends AbstractHostSerializer {
    @Override
    protected InetSocketAddress getAddress(MessageEntity message) {
        return message.getBornHost();
    }

    @Override
    protected int getHostLength(int sysFlag) {
        return MessageConst.SysFlg.bornHostLength(sysFlag);
    }

    @Override
    protected void setAddress(MessageEntity message, InetSocketAddress address) {
        message.setBornHost(address);
    }
}
