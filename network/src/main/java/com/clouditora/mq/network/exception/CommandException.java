package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingCommandException
 */
public class CommandException extends AbstractNetworkException {
    public CommandException(String message) {
        super(message);
    }
}
