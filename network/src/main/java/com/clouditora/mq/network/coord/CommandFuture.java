package com.clouditora.mq.network.coord;

import com.clouditora.mq.network.CommandFutureCallback;
import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @link org.apache.rocketmq.remoting.netty.ResponseFuture
 */
@ToString
@Getter
public class CommandFuture {
    private volatile Command command;
    private final int opaque;
    private final Channel channel;
    @Setter
    private volatile boolean sendOk = true;
    @Setter
    private volatile Throwable cause;
    private final long beginTime = System.currentTimeMillis();
    private final long timeout;
    private final CommandFutureCallback callback;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    public CommandFuture(Channel channel, int opaque, long timeout, CommandFutureCallback callback) {
        this.channel = channel;
        this.opaque = opaque;
        this.timeout = timeout;
        this.callback = callback;
    }

    public void putResponse(Command command) {
        this.command = command;
        this.countDownLatch.countDown();
    }

    public Command waitCommand(long timeout) throws InterruptedException {
        countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return command;
    }

    public void invokeCallback() {
        if (callback == null) {
            return;
        }
        if (callbackInvoked.compareAndSet(false, true)) {
            callback.callback(this);
        }
    }
}
