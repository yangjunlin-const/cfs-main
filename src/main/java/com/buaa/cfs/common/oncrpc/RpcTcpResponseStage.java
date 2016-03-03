package com.buaa.cfs.common.oncrpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * RpcTcpResponseStage sends an RpcResponse across the wire with the appropriate fragment header.
 */
public class RpcTcpResponseStage extends SimpleChannelInboundHandler<Object> {
    private static final Log LOG = LogFactory.getLog(RpcTcpResponseStage.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object e)
            throws Exception {
        RpcResponse r = (RpcResponse) e;
        byte[] fragmentHeader = XDR.recordMark(r.data().readableBytes(), true);
        ByteBuf header = Unpooled.wrappedBuffer(fragmentHeader);
        ByteBuf d = Unpooled.wrappedBuffer(header, r.data());
        ctx.writeAndFlush(d);
    }
}
