package com.buaa.cfs.common.oncrpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.DatagramPacket;

/**
 * RpcUdpResponseStage sends an RpcResponse as a UDP packet, which does not require a fragment header.
 */
public class RpcUdpResponseStage extends
        SimpleChannelInboundHandler<Object> {
    private static final Log LOG = LogFactory.getLog(RpcUdpResponseStage.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object e)
            throws Exception {
        ctx.channel().remoteAddress();
        RpcResponse r = (RpcResponse) e;
        LOG.info("--- portmapt last handler data :  " + r.data());
//            ctx.writeAndFlush(r.data());
//            DatagramSocket datagramSocket = new DatagramSocket(r.remoteAddress());
        ByteBuf byteBuf = r.data().copy();
        byte[] tmp = new byte[]{};
        byteBuf.getBytes(0, tmp);
        LOG.info("--- the tmp is : " + tmp);
        DatagramPacket datagramPacket = new DatagramPacket(tmp, 0, tmp.length, r.remoteAddress());
//            datagramSocket.send(datagramPacket);
//            datagramSocket.setSoTimeout(1000);
        ctx.writeAndFlush(datagramPacket);

    }
}
