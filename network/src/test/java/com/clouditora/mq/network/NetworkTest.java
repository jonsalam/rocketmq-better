package com.clouditora.mq.network;

import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

@Slf4j
class NetworkTest {
    ServerNetwork serverNetwork;
    ClientNetwork clientNetwork;

    @BeforeEach
    public void startup() {
        ServerNetworkConfig serverConfig = new ServerNetworkConfig();
        this.serverNetwork = new ServerNetwork(serverConfig, null);
        this.serverNetwork.startup();

        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        this.clientNetwork = new ClientNetwork(clientNetworkConfig, null);
        this.clientNetwork.startup();
    }

    @AfterEach
    public void shutdown() {
        log.info("---- shutdown ----");
        Optional.ofNullable(this.serverNetwork).ifPresent(ServerNetwork::shutdown);
        Optional.ofNullable(this.clientNetwork).ifPresent(ClientNetwork::shutdown);
    }

    @Test
    void syncInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.serverNetwork.registerDispatcher(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );

        this.clientNetwork.syncInvoke("localhost:8888", command, 1000 * 30);
        countDownLatch.await();
    }

    @Test
    void asyncInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.serverNetwork.registerDispatcher(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );
        this.clientNetwork.asyncInvoke("localhost:8888", command, 1000 * 30, commandFuture -> countDownLatch.countDown());
        countDownLatch.await();
    }

    @Test
    void onewayInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.serverNetwork.registerDispatcher(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );
        this.clientNetwork.onewayInvoke("localhost:8888", command, 1000 * 30);
        countDownLatch.await();
    }

}
