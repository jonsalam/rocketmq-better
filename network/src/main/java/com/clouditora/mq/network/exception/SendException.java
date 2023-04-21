package com.clouditora.mq.network.exception;

/**
 * @link org.apache.rocketmq.remoting.exception.RemotingSendRequestException
 */
public class SendException extends AbstractNetworkException {

    public SendException(String endpoint, Throwable cause) {
        super("send", endpoint, cause);
    }
}
