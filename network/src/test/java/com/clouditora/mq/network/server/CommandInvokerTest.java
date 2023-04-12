package com.clouditora.mq.network.server;

import com.clouditora.mq.network.coord.CommandFuture;
import com.clouditora.mq.network.coord.CommandInvoker;
import com.clouditora.mq.netty.SucceededChannelFuture;
import com.clouditora.mq.network.exception.SendException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandInvokerTest {

    @Test
    void syncInvoke_timeout() {
        assertThrows(TimeoutException.class, () -> {
            int opaque = 1;
            ConcurrentHashMap<Integer, CommandFuture> commandMap = new ConcurrentHashMap<>();
            CommandInvoker invoker = new CommandInvoker(1, 1, commandMap, null);

            Channel channel = Mockito.mock(Channel.class);
            ChannelFuture channelFuture = new SucceededChannelFuture();
            Mockito.when(channel.writeAndFlush(Mockito.any())).thenReturn(channelFuture);

            Command request = new Command();
            request.setCode(0);
            request.setOpaque(opaque);

            invoker.syncInvoke(channel, request, 100);
        });
    }

    @Test
    void syncInvoke() throws InterruptedException, SendException, TimeoutException {
        int opaque = 1;
        ConcurrentHashMap<Integer, CommandFuture> commandMap = new ConcurrentHashMap<>();
        CommandInvoker invoker = new CommandInvoker(1, 1, commandMap, null);

        Channel channel = Mockito.mock(Channel.class);
        ChannelFuture channelFuture = new SucceededChannelFuture();
        Mockito.when(channel.writeAndFlush(Mockito.any())).thenReturn(channelFuture);

        Command request = new Command();
        request.setCode(0);
        request.setOpaque(opaque);

        // 模拟netty发送消息
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
                () -> {
                    CommandFuture commandFuture = commandMap.get(opaque);
                    if (commandFuture != null) {
                        commandFuture.putResponse(request);
                    }
                },
                10,
                20,
                TimeUnit.MILLISECONDS
        );

        invoker.syncInvoke(channel, request, 100);
        assertNull(commandMap.get(opaque));
    }
}
