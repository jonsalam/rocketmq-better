package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingConnectException
 */
public class ConnectException extends AbstractNetworkException {

    public ConnectException(String address, Throwable cause) {
        super("connect", address, cause);
    }

    public ConnectException(String address) {
        super(address, null);
    }
}
