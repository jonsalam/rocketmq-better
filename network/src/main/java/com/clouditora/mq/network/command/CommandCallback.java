package com.clouditora.mq.network.command;

import com.clouditora.mq.network.protocol.Command;

/**
 * @link org.apache.rocketmq.remoting.netty.RemotingResponseCallback
 */
public interface CommandCallback {
    void callback(Command command);
}
