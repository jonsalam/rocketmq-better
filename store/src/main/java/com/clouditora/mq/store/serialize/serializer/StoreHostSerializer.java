package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;

import java.net.InetSocketAddress;

public class StoreHostSerializer extends AbstractHostSerializer {

    @Override
    protected void setAddress(MessageEntity message, InetSocketAddress address) {
        message.setStoreHost(address);
    }

    @Override
    protected InetSocketAddress getAddress(MessageEntity message) {
        return message.getStoreHost();
    }

    @Override
    protected int getHostLength(int sysFlag) {
        return MessageConst.SysFlg.storeHostLength(sysFlag);
    }
}
