package com.buaa.cfs.server;

import com.buaa.cfs.protobufer.CfsProto;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by on 12/24/15.
 */
public class CfsServerHandler extends SimpleChannelInboundHandler {
    private static Log LOG = LogFactory.getLog(CfsServerHandler.class);
    private static AtomicLong atomicLong = new AtomicLong(0);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        CfsProto.CfsMessage cfsMessage = (CfsProto.CfsMessage) msg;
//        Message.OpMessage response = (Message.OpMessage) msg;
//        long count = atomicLong.incrementAndGet();
//        Message.OpMessage.Builder builder = new Message.OpMessage.Builder().setName(response.getName()).setAge(response.getAge()).setCount(++count);
//        response = builder.build();
//        logger.info("server add one to count , now count is : " + count);
//        ChannelFuture channelFuture = ctx.writeAndFlush(response);
       /* channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                ctx.close();
            }
        });*/
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
//        ctx.close();
    }
}
