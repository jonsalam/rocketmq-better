package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingTimeoutException
 */
public class TimeoutException extends AbstractNetworkException {
    public TimeoutException(String endpoint) {
        super(endpoint);
    }

    public TimeoutException(String endpoint, long ms) {
        this(endpoint, ms, null);
    }

    public TimeoutException(String endpoint, long ms, Throwable cause) {
        super(String.format("timeout[%s mS]", ms), endpoint, cause);
    }
}
