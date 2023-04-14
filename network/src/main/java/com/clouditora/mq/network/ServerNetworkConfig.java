package com.clouditora.mq.network;

import com.clouditora.mq.network.coord.CoordinatorConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class ServerNetworkConfig extends CoordinatorConfig {
    private int listenPort = 8888;
    private int serverWorkerThreads = 8;
    private int serverCallbackExecutorThreads = 0;
    private int serverSelectorThreads = 3;
    private int serverOnewaySemaphoreValue = 256;
    private int serverAsyncSemaphoreValue = 64;
    private int serverChannelMaxIdleTimeSeconds = 120;

    private int serverSocketSndBufSize = Integer.parseInt(System.getProperty("com.rocketmq.remoting.socket.sndbuf.size", "0"));
    private int serverSocketRcvBufSize = Integer.parseInt(System.getProperty("com.rocketmq.remoting.socket.rcvbuf.size", "0"));
    private int writeBufferHighWaterMark = Integer.parseInt(System.getProperty("com.rocketmq.remoting.write.buffer.high.water.mark", "0"));
    private int writeBufferLowWaterMark = Integer.parseInt(System.getProperty("com.rocketmq.remoting.write.buffer.low.water.mark", "0"));
    private int serverSocketBacklog = Integer.parseInt(System.getProperty("com.rocketmq.remoting.socket.backlog", "1024"));
    private boolean serverPooledByteBufAllocatorEnable = true;

    private boolean useEpollNativeSelector = false;

    @Override
    protected int getWorkerThreads() {
        return this.serverWorkerThreads;
    }

    @Override
    protected int getCallbackExecutorThreads() {
        return this.serverCallbackExecutorThreads;
    }
}
