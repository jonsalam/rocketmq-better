package com.clouditora.mq.network.coord;

import com.clouditora.mq.network.CommandFutureCallback;
import com.clouditora.mq.network.exception.SendException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
@Getter
public class CommandInvoker implements CallbackExecutor {
    protected final ConcurrentMap<Integer, CommandFuture> commandMap;
    /**
     * Semaphore to limit maximum number of ongoing asynchronous requests, which protects system memory footprint.
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#semaphoreOneway
     */
    protected final Semaphore asyncSemaphore;
    /**
     * Semaphore to limit maximum number of ongoing one-way requests, which protects system memory footprint.
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#semaphoreAsync
     */
    protected final Semaphore onewaySemaphore;
    protected final ExecutorService callbackExecutor;

    public CommandInvoker(int asyncPermits, int onewayPermits, ConcurrentMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor) {
        this.asyncSemaphore = new Semaphore(asyncPermits);
        this.onewaySemaphore = new Semaphore(onewayPermits);
        this.commandMap = commandMap;
        this.callbackExecutor = callbackExecutor;
    }

    private void tryAcquire(long timeout) throws TimeoutException, SendException {
        try {
            boolean acquired = this.asyncSemaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
            if (!acquired) {
                log.warn("tryAcquire semaphore timeout {}/{}", this.asyncSemaphore.availablePermits(), this.asyncSemaphore.getQueueLength());
                throw new TimeoutException("tryAcquire");
            }
        } catch (InterruptedException e) {
            throw new SendException("InterruptedException", e);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#invokeSyncImpl
     */
    public Command syncInvoke(Channel channel, Command request, long timeout) throws SendException, TimeoutException, InterruptedException {
        log.debug("[invoke][sync] channel={}, request={}, timeout={}", channel, request, timeout);
        int opaque = request.getOpaque();
        try {
            CommandFuture commandFuture = new CommandFuture(channel, opaque, timeout, null);
            this.commandMap.put(opaque, commandFuture);
            channel.writeAndFlush(request)
                    .addListener((ChannelFutureListener) f -> {
                        if (f.isSuccess()) {
                            commandFuture.setSendOk(true);
                            return;
                        }
                        this.commandMap.remove(opaque);
                        commandFuture.setSendOk(false);
                        commandFuture.setCause(f.cause());
                        commandFuture.putResponse(null);
                        log.warn("send request command failed: channel={}", channel);
                    });
            Command response = commandFuture.waitCommand(timeout);
            if (response != null) {
                return response;
            }
            if (commandFuture.isSendOk()) {
                throw new TimeoutException(CoordinatorUtil.toAddress(channel), timeout, commandFuture.getCause());
            } else {
                throw new SendException(CoordinatorUtil.toAddress(channel), commandFuture.getCause());
            }
        } finally {
            commandMap.remove(opaque);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#invokeAsyncImpl
     */
    public void asyncInvoke(Channel channel, Command request, long timeout, CommandFutureCallback callback) throws SendException, TimeoutException {
        long startTime = System.currentTimeMillis();
        tryAcquire(timeout);
        long now = System.currentTimeMillis();
        CommandFuture commandFuture = new CommandFuture(channel, request.getOpaque(), startTime - now, callback);
        this.commandMap.put(request.getOpaque(), commandFuture);
        try {
            channel.writeAndFlush(request)
                    .addListener((ChannelFutureListener) f -> {
                        if (f.isSuccess()) {
                            commandFuture.setSendOk(true);
                            return;
                        }
                        CommandFuture prev = this.commandMap.remove(request.getOpaque());
                        if (prev != null) {
                            prev.setSendOk(false);
                            prev.setCause(f.cause());
                            prev.putResponse(null);
                            log.warn("send request command failed: channel={}", CoordinatorUtil.toAddress(channel));
                            try {
                                CoordinatorUtil.invokeCallback(prev, getCallbackExecutor());
                            } catch (Throwable e) {
                                log.warn("invoke callback exception", e);
                            }
                        }
                    });
        } catch (Exception e) {
            String address = CoordinatorUtil.toAddress(channel);
            log.warn("send request command exception: channel={}", address);
            throw new SendException(address, e);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#invokeOnewayImpl
     */
    public void onewayInvoke(Channel channel, Command request, long timeout) throws SendException, TimeoutException {
        request.markOneway();
        tryAcquire(timeout);
        try {
            channel.writeAndFlush(request)
                    .addListener((ChannelFutureListener) f -> {
                        if (!f.isSuccess()) {
                            log.warn("send request command failed: channel={}", channel);
                        }
                    });
        } catch (Exception e) {
            String address = CoordinatorUtil.toAddress(channel);
            log.warn("send request command exception: channel={}", address);
            throw new SendException(address, e);
        }
    }
}
