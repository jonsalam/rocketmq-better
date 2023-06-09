package com.clouditora.mq.network.netty;

import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.CommandCodec;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyEncoder
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyCommandEncoder extends MessageToByteEncoder<Command> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Command command, ByteBuf byteBuf) throws Exception {
        try {
            CommandCodec.encode(byteBuf, command);
        } catch (Exception e) {
            log.error("encode exception: {}", NetworkUtil.toEndpoint(ctx.channel()), e);
            if (command != null) {
                log.error(command.toString());
            }
            NetworkUtil.closeChannel(ctx.channel());
        }
    }
}
