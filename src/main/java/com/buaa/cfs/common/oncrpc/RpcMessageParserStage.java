package com.buaa.cfs.common.oncrpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;

public class RpcMessageParserStage extends SimpleChannelInboundHandler<Object> {
    private static final Log LOG = LogFactory.getLog(RpcMessageParserStage.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object e) {
        byte[] tmp = (byte[]) e;
        ByteBuffer byteBuffer = ByteBuffer.wrap(tmp);
        XDR in = new XDR(byteBuffer, XDR.State.READING);
        RpcInfo info = null;
        try {
            RpcCall callHeader = RpcCall.read(in);
            ByteBuf dataBuffer = Unpooled.wrappedBuffer(in.buffer()
                    .slice());
            info = new RpcInfo(callHeader, dataBuffer, ctx, ctx.channel(),
                    ctx.channel().remoteAddress());
        } catch (Exception exc) {
            LOG.info("Malformed RPC request from " + ctx.channel().remoteAddress().toString());
        }
        if (info != null) {
            LOG.info("--- parse success.");
            ctx.fireChannelRead(info);
        } else {
            LOG.info("--- parse failed.");
        }
    }
}
