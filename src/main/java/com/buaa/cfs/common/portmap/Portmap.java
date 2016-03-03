/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.buaa.cfs.common.portmap;

import com.buaa.cfs.common.oncrpc.RpcMessageParserStage;
import com.buaa.cfs.common.oncrpc.RpcProgram;
import com.buaa.cfs.common.oncrpc.RpcTcpResponseStage;
import com.buaa.cfs.common.oncrpc.RpcUtil;
import com.buaa.cfs.utils.StringUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Portmap service for binding RPC protocols. See RFC 1833 for details.
 */
final class Portmap {
    private static final Log LOG = LogFactory.getLog(Portmap.class);
    private static final int DEFAULT_IDLE_TIME_MILLISECONDS = 5000;

    private Bootstrap udpServer;
    private ServerBootstrap tcpServer;
    EventLoopGroup tcpbossGroup = new NioEventLoopGroup(1);
    EventLoopGroup tcpworkerGroup = new NioEventLoopGroup(1);
    EventLoopGroup udpworkerGroup = new NioEventLoopGroup(1);
    private RpcProgramPortmapTCP handlerTCP = new RpcProgramPortmapTCP();
    private RpcProgramPortmapUDP handlerUDP = new RpcProgramPortmapUDP();

    public static void main(String[] args) {
        StringUtils.startupShutdownMessage(Portmap.class, args, LOG);

        final int port = RpcProgram.RPCB_PORT;
        Portmap pm = new Portmap();
        try {
            pm.start(DEFAULT_IDLE_TIME_MILLISECONDS,
                    new InetSocketAddress(port), new InetSocketAddress(port));
        } catch (Throwable e) {
            LOG.fatal("Failed to start the server. Cause:", e);
            pm.shutdown();
            System.exit(-1);
        }
    }

    void shutdown() {
        tcpbossGroup.shutdownGracefully();
        tcpworkerGroup.shutdownGracefully();
        udpworkerGroup.shutdownGracefully();
    }

    void start(final int idleTimeMilliSeconds, final SocketAddress tcpAddress,
            final SocketAddress udpAddress) throws InterruptedException {
        SimpleChannelInboundHandler<Object> STAGE_RPC_MESSAGE_PARSER = new RpcMessageParserStage();
        tcpServer = new ServerBootstrap();
        tcpServer.group(tcpbossGroup, tcpworkerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
//                    private final IdleStateHandler idleStateHandler = new IdleStateHandler(
//                            0, 0, idleTimeMilliSeconds, TimeUnit.MILLISECONDS);

                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(RpcUtil.constructRpcFrameDecoder());
                        p.addLast(new RpcMessageParserStage());
//                        p.addLast(new IdleStateHandler(
//                                0, 0, idleTimeMilliSeconds, TimeUnit.MILLISECONDS));
                        p.addLast(handlerTCP);
                        p.addLast(new RpcTcpResponseStage());

                    }
                });
        ChannelFuture f = tcpServer.bind(tcpAddress).sync();
//        f.channel().closeFuture().sync();

        udpServer = new Bootstrap();
        udpServer.group(udpworkerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    public void initChannel(DatagramChannel ch) {
                        ChannelPipeline p = ch.pipeline();
//                        p.addLast(RpcUtil.constructRpcFrameDecoder());
//                        p.addLast(RpcUtil.STAGE_RPC_MESSAGE_PARSER);
                        p.addLast(handlerUDP);
//                        p.addLast(RpcUtil.STAGE_RPC_UDP_RESPONSE);

                    }
                });
        ChannelFuture udpFuture = udpServer.bind(udpAddress).sync();
//        udpFuture.channel().closeFuture().sync();
        LOG.info("Portmap server started at tcp://" + ", udp://");
    }
}
