package com.buaa.cfs.client;

import io.netty.channel.ChannelHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by on 12/24/15.
 */
public class CfsClientHandler extends ChannelHandlerAdapter {
    private static Log LOG = LogFactory.getLog(CfsClientHandler.class);

//    private Message.OpMessage message;

//    private boolean isSuccess = false;
//
////    public CfsClientHandler(Message.OpMessage message) {
////        this.message = message;
////    }
//
//    @Override
//    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        ChannelFuture c = ctx.writeAndFlush(message);
//        /*c.addListener(new ChannelFutureListener() {
//            @Override
//            public void operationComplete(ChannelFuture future) throws Exception {
//                ctx.close();
//            }
//        });*/
//    }
//
//    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        try {
//            Message.OpMessage response = (Message.OpMessage) msg;
//            logger.info("Receive server response : " + response.getName() + " , "
//                    + response.getAge() + " , " + response.getCount());
//            if (response.getCount() - 1 == message.getCount()) {
//                isSuccess = true;
//            }
//            logger.info("isSuccess is : " + isSuccess);
//            this.message = response;
//        } finally {
//            ReferenceCountUtil.safeRelease(msg);
//        }
//    }
//
//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        logger.info("client channelReadComplete");
//        ctx.flush();
//        ctx.close();
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        logger.info("client exceptionCaught");
//        cause.printStackTrace();
//        ctx.close();
//    }
//
//    public boolean isSuccess() {
//        return isSuccess;
//    }
//
//    public Message.OpMessage getMessage() {
//        return message;
//    }
}