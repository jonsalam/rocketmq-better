package com.clouditora.mq.network.netty;

import java.util.concurrent.ExecutorService;

public interface CallbackExecutor {
    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified;
     * or null if the callback is supposed to be executed in the netty event-loop thread.
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#getCallbackExecutor
     */
    ExecutorService getCallbackExecutor();
}
