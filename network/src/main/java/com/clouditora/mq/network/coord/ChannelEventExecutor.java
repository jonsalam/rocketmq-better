package com.clouditora.mq.network.coord;

import com.clouditora.mq.common.service.AbstractLoopedService;
import com.clouditora.mq.network.ChannelEventListener;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract.NettyEventExecutor
 */
@Slf4j
public class ChannelEventExecutor extends AbstractLoopedService {
    private final LinkedBlockingQueue<ChannelEvent> eventQueue = new LinkedBlockingQueue<>();
    @Setter
    private int maxSize = 10000;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#getChannelEventListener
     */
    @Getter
    private final ChannelEventListener listener;

    public ChannelEventExecutor(ChannelEventListener listener) {
        this.listener = listener;
    }

    @Override
    public String getServiceName() {
        return "ChannelExecutor";
    }

    public void addEvent(ChannelEvent event) {
        if (listener == null) {
            return;
        }
        int size = this.eventQueue.size();
        if (size > this.maxSize) {
            log.warn("event queue size [{}] over the limit [{}], so drop this event {}", size, this.maxSize, event);
        } else {
            this.eventQueue.add(event);
        }
    }

    @Override
    protected void loop() throws Exception {
        ChannelEvent event = eventQueue.poll(3000, TimeUnit.MILLISECONDS);
        if (event == null) {
            return;
        }
        switch (event.getType()) {
            case idle -> listener.onIdle(event.getAddress(), event.getChannel());
            case close -> listener.onClose(event.getAddress(), event.getChannel());
            case connect -> listener.onConnect(event.getAddress(), event.getChannel());
            case exception -> listener.onException(event.getAddress(), event.getChannel());
        }
    }
}
