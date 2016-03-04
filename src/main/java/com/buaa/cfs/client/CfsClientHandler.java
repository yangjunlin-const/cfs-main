package com.buaa.cfs.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by root on 3/4/16.
 */
public class CfsClientHandler extends SimpleChannelInboundHandler {
    private Log log = LogFactory.getLog(CfsClientHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

    }
}
