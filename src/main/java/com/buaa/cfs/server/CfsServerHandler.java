package com.buaa.cfs.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

public class CfsServerHandler extends SimpleChannelInboundHandler {
    private static Log LOG = LogFactory.getLog(CfsServerHandler.class);
    private static AtomicLong atomicLong = new AtomicLong(0);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error(cause.getMessage());
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        LOG.info("---server read complete.");
        ctx.flush();
    }
}
