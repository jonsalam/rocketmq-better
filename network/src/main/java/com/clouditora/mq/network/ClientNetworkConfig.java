package com.clouditora.mq.network;

import com.clouditora.mq.network.netty.NetworkConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class ClientNetworkConfig extends NetworkConfig {
    /**
     * Worker thread number
     */
    private int clientWorkerThreads = Integer.parseInt(System.getProperty("com.rocketmq.remoting.client.worker.size", "4"));
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int clientOnewaySemaphoreValue = Integer.parseInt(System.getProperty("com.rocketmq.remoting.clientOnewaySemaphoreValue", "65535"));
    private int clientAsyncSemaphoreValue = Integer.parseInt(System.getProperty("com.rocketmq.remoting.clientAsyncSemaphoreValue", "65535"));
    private int connectTimeoutMillis = Integer.parseInt(System.getProperty("com.rocketmq.remoting.client.connect.timeout", "3000"));
    private long channelNotActiveInterval = 1000 * 60;
    /**
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private int clientChannelMaxIdleTimeSeconds = Integer.parseInt(System.getProperty("com.rocketmq.remoting.client.channel.maxIdleTimeSeconds", "120"));
    private int clientSocketSndBufSize = Integer.parseInt(System.getProperty("com.rocketmq.remoting.socket.sndbuf.size", "0"));
    private int clientSocketRcvBufSize = Integer.parseInt(System.getProperty("com.rocketmq.remoting.socket.rcvbuf.size", "0"));
    private boolean clientPooledByteBufAllocatorEnable = false;
    private boolean clientCloseSocketIfTimeout = Boolean.parseBoolean(System.getProperty("com.rocketmq.remoting.client.closeSocketIfTimeout", "true"));
    private int writeBufferHighWaterMark = Integer.parseInt(System.getProperty("com.rocketmq.remoting.write.buffer.high.water.mark", "0"));
    private int writeBufferLowWaterMark = Integer.parseInt(System.getProperty("com.rocketmq.remoting.write.buffer.low.water.mark", "0"));

    @Override
    protected int getWorkerThreads() {
        return this.clientWorkerThreads;
    }

    @Override
    protected int getCallbackExecutorThreads() {
        return this.clientCallbackExecutorThreads;
    }
}
