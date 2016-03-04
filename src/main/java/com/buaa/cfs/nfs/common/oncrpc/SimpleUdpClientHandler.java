package com.buaa.cfs.nfs.common.oncrpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by root on 3/3/16.
 */
public class SimpleUdpClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    public static final Log LOG = LogFactory.getLog(SimpleUdpClientHandler.class);

    private DatagramPacket datagramPacket;

    public SimpleUdpClientHandler(DatagramPacket datagramPacket) {
        this.datagramPacket = datagramPacket;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
        byte[] bytes = new byte[]{};
        ByteBuf byteBuf = msg.content().getBytes(0, bytes);
        LOG.info("--- bytes is : " + bytes.toString());
        XDR xdr = new XDR(byteBuf.nioBuffer(), XDR.State.READING);
        RpcReply reply = RpcReply.read(xdr);
        if (reply.getState() != RpcReply.ReplyState.MSG_ACCEPTED) {
            throw new IOException("Request failed: " + reply.getState());
        }
        LOG.info("--- the result is : " + reply.getState().getValue());
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(datagramPacket);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(cause.getMessage());
        ctx.close();
    }
}
