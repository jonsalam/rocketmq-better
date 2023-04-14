package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;

import java.net.InetSocketAddress;

public class StoreHostSerializer extends AbstractHostSerializer {

    @Override
    protected InetSocketAddress getAddress(MessageEntity message) {
        return message.getStoreHost();
    }

    @Override
    protected int getHostLength(int sysFlag) {
        return MessageConst.SysFlg.storeHostLength(sysFlag);
    }

    @Override
    protected void setAddress(MessageEntity message, InetSocketAddress address) {
        message.setStoreHost(address);
    }
}