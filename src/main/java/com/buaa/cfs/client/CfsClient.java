package com.buaa.cfs.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by on 12/24/15.
 */
public class CfsClient {
    private static Log LOG = LogFactory.getLog(CfsClient.class);
    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private Bootstrap bootstrap = new Bootstrap();

    public void sendMessage(final String host, final int port,
            final ChannelHandlerAdapter handler) {
        bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline().addLast(new ProtobufVarint32FrameDecoder());
//                channel.pipeline().addLast(
//                        new ProtobufDecoder(Message.OpMessage.getDefaultInstance()));
                channel.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                channel.pipeline().addLast(new ProtobufEncoder());
                channel.pipeline().addLast(handler);
            }
        });

        try {
            ChannelFuture f = bootstrap.connect(host, port).sync();
//            f.channel().closeFuture().sync();
            f.channel().closeFuture().sync();
          /*  f.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    Message.OpMessage message = (Message.OpMessage) future.getNow();
                    logger.info("listenler : " + message.getCount());
                }
            });*/
        } catch (InterruptedException e) {
        }
    }
}
