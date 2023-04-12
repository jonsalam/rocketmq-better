package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingConnectException
 */
public class ConnectException extends AbstractNetworkException {

    public ConnectException(String endpoint, Throwable cause) {
        super("connect", endpoint, cause);
    }

    public ConnectException(String endpoint) {
        this(endpoint, null);
    }
}
