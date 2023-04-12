package com.clouditora.mq.network.command;

/**
 * @link org.apache.rocketmq.remoting.InvokeCallback
 */
public interface CommandFutureCallback {
    void callback(CommandFuture commandFuture);
}
