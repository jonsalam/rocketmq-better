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
    Server server;
    Client client;

    @BeforeEach
    public void startup() {
        ServerNetworkConfig serverConfig = new ServerNetworkConfig();
        this.server = new Server(serverConfig, null);
        this.server.startup();

        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        this.client = new Client(clientNetworkConfig, null);
        this.client.startup();
    }

    @AfterEach
    public void shutdown() {
        log.info("---- shutdown ----");
        Optional.ofNullable(this.server).ifPresent(Server::shutdown);
        Optional.ofNullable(this.client).ifPresent(Client::shutdown);
    }

    @Test
    void syncInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.server.registerProcessor(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );

        this.client.syncInvoke("localhost:8888", command, 1000 * 30);
        countDownLatch.await();
    }

    @Test
    void asyncInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.server.registerProcessor(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );
        this.client.asyncInvoke("localhost:8888", command, 1000 * 30, commandFuture -> countDownLatch.countDown());
        countDownLatch.await();
    }

    @Test
    void onewayInvoke() throws InterruptedException, TimeoutException, ConnectException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("hello");
        Command command = Command.buildRequest(0, requestHeader);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.server.registerProcessor(
                0,
                (context, request) -> {
                    request.setRemark("Hi " + context.channel().remoteAddress());
                    countDownLatch.countDown();
                    return request;
                },
                null
        );
        this.client.onewayInvoke("localhost:8888", command, 1000 * 30);
        countDownLatch.await();
    }

}
