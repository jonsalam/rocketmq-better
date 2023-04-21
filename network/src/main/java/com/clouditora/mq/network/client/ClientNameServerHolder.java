package com.clouditora.mq.network.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ClientNameServerHolder {
    @Getter
    protected final ClientChannelHolder channelHolder;
    protected final AtomicInteger nameServerAddressIndex = new AtomicInteger(random());
    protected final AtomicReference<List<String>> nameServerAddressList = new AtomicReference<>();
    protected final AtomicReference<String> nameServerAddress = new AtomicReference<>();
    protected final Lock lock = new ReentrantLock();

    public ClientNameServerHolder(ClientChannelHolder channelHolder) {
        this.channelHolder = channelHolder;
    }

    private static int random() {
        return Math.abs(new Random().nextInt() % 999) % 999;
    }

    public List<String> getNameServerAddressList(){
        return this.nameServerAddressList.get();
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#getAndCreateNameserverChannel
     */
    public Channel getOrCreateChannel() {
        Channel channel = getCurrentChannel();
        if (channel != null) {
            return null;
        }
        try {
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("[channel] create name server wait timeout");
                return null;
            }
            try {
                channel = getCurrentChannel();
                if (channel != null) {
                    return null;
                }
                List<String> list = nameServerAddressList.get();
                for (String address : list) {
                    int index = Math.abs(nameServerAddressIndex.incrementAndGet());
                    index = index % list.size();
                    address = list.get(index);
                    channel = channelHolder.getOrCreateChannel(address);
                    if (channel != null) {
                        this.nameServerAddress.set(address);
                        return null;
                    }
                }
            } catch (Exception e) {
                log.error("[channel] create name server exception", e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("[channel] create name server exception", e);
        }
        return null;
    }

    private Channel getCurrentChannel() {
        String address = this.nameServerAddress.get();
        if (address == null) {
            return null;
        }
        return channelHolder.getChannel(address);
    }
}
