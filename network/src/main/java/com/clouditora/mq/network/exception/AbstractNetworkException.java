package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingException
 */
public abstract class AbstractNetworkException extends Exception {

    public AbstractNetworkException(String message) {
        super(message);
    }

    public AbstractNetworkException(String reason, String address) {
        this(reason, address, null);
    }

    public AbstractNetworkException(String reason, String address, Throwable cause) {
        super(String.format("%s exception on [%s]", reason, address), cause);
    }
}
