package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingTimeoutException
 */
public class TimeoutException extends AbstractNetworkException {

    public TimeoutException(String address) {
        super(address);
    }

    public TimeoutException(String address, long ms) {
        this(address, ms, null);
    }

    public TimeoutException(String address, long ms, Throwable cause) {
        super(String.format("timeout[%s mS]", ms), address, cause);
    }
}
