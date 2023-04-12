package com.clouditora.mq.network;

import com.clouditora.mq.network.coord.CommandFuture;

/**
 * @link org.apache.rocketmq.remoting.InvokeCallback
 */
public interface CommandFutureCallback {
    void callback(CommandFuture commandFuture);
}
