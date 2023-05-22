package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingException
 */
public abstract class AbstractNetworkException extends Exception {
    public AbstractNetworkException(String message) {
        super(message);
    }

    public AbstractNetworkException(String reason, String endpoint) {
        this(reason, endpoint, null);
    }

    public AbstractNetworkException(String reason, String endpoint, Throwable cause) {
        super(String.format("%s exception to %s", reason, endpoint == null ? "nameserver" : endpoint), cause);
    }
}
