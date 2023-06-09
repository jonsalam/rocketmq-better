package com.clouditora.mq.network.util;

import com.clouditora.mq.network.command.CommandFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
public class NetworkUtil {
    private static final AttributeKey<String> REMOTE_ADDRESS_KEY = AttributeKey.valueOf("RemoteAddr");

    /**
     * @link org.apache.rocketmq.remoting.common.RemotingHelper#parseChannelRemoteAddr
     */
    public static String toEndpoint(Channel channel) {
        if (null == channel) {
            return "";
        }
        Attribute<String> att = channel.attr(REMOTE_ADDRESS_KEY);
        if (att == null) {
            // mocked in unit test
            return toEndpoint(channel.remoteAddress());
        }
        String endpoint = att.get();
        if (endpoint == null) {
            endpoint = toEndpoint(channel.remoteAddress());
            att.set(endpoint);
        }
        return endpoint;
    }

    public static String toEndpoint(SocketAddress socketAddress) {
        String endpoint = socketAddress == null ? StringUtils.EMPTY : socketAddress.toString();
        if (endpoint.length() == 0) {
            return StringUtils.EMPTY;
        }
        int index = endpoint.lastIndexOf("/");
        if (index >= 0) {
            return endpoint.substring(index + 1);
        }
        return endpoint;
    }

    public static SocketAddress toSocketAddress(String endpoint) {
        String[] split = endpoint.split(":");
        String ip = split[0];
        String port = split[1];
        return new InetSocketAddress(ip, Integer.parseInt(port));
    }

    public static void closeChannel(Channel channel) {
        String endpoint = toEndpoint(channel);
        channel.close().addListener(future -> log.debug("close connection: endpoint={}, result={}", endpoint, future.isSuccess()));
    }

    public static String simplifyException(Exception e) {
        if (e == null) {
            return "";
        }
        return Arrays.stream(e.getStackTrace())
                .map(Objects::toString)
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#executeInvokeCallback
     */
    public static void invokeCallback(CommandFuture command, ExecutorService executor) {
        if (executor == null) {
            invokeCallbackByCurrentThread(command);
        } else {
            invokeCallbackByExecutor(command, executor);
        }
    }

    private static void invokeCallbackByCurrentThread(CommandFuture command) {
        try {
            command.invokeCallback();
        } catch (Throwable e) {
            log.warn("invoke callback exception", e);
        }
    }

    private static void invokeCallbackByExecutor(CommandFuture command, ExecutorService executor) {
        try {
            executor.submit(() -> {
                try {
                    command.invokeCallback();
                } catch (Throwable e) {
                    log.warn("invoke callback exception by executor", e);
                }
            });
        } catch (RejectedExecutionException e) {
            // 被线程池拒绝了
            log.warn("invoke callback exception by executor", e);
            invokeCallbackByCurrentThread(command);
        }
    }

    public static boolean isActive(ChannelFuture channelFuture) {
        return channelFuture != null && channelFuture.channel() != null && channelFuture.channel().isActive();
    }
}
