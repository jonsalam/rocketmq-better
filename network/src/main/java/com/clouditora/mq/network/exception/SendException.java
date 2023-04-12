package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingSendRequestException
 */
public class SendException extends AbstractNetworkException {

    public SendException(String address, Throwable cause) {
        super("send", address, cause);
    }
}
