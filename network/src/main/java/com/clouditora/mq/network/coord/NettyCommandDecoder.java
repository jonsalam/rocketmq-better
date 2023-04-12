package com.clouditora.mq.network.coord;

import com.clouditora.mq.network.protocol.CommandCodec;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyDecoder
 */
@Slf4j
public class NettyCommandDecoder extends LengthFieldBasedFrameDecoder {
    private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyCommandDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, byteBuf);
            if (frame == null) {
                return null;
            }
            return CommandCodec.decode(frame);
        } catch (Exception e) {
            log.error("decode exception: {}", CoordinatorUtil.toAddress(ctx.channel()), e);
            CoordinatorUtil.closeChannel(ctx.channel());
            return null;
        } finally {
            if (frame != null) {
                frame.release();
            }
        }
    }
}
