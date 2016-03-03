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
package com.buaa.cfs.common.oncrpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;

/**
 * Simple UDP server implemented using netty.
 */
public class SimpleTcpServer {
    public static final Log LOG = LogFactory.getLog(SimpleTcpServer.class);
    protected final int port;
    protected int boundPort = -1; // Will be set after server starts
    protected final SimpleChannelInboundHandler rpcProgram;
    private ServerBootstrap server;
    private EventLoopGroup tcpbossGroup;
    private EventLoopGroup tcpworkerGroup;

    /** The maximum number of I/O worker threads */
    protected final int workerCount;

    /**
     * @param port        TCP port where to start the server at
     * @param program     RPC program corresponding to the server
     * @param workercount Number of worker threads
     */
    public SimpleTcpServer(int port, RpcProgram program, int workercount) {
        this.port = port;
        this.rpcProgram = program;
        this.workerCount = workercount;
        tcpbossGroup = new NioEventLoopGroup(1);
        if (workercount > 0) {
            tcpworkerGroup = new NioEventLoopGroup(workercount);
        } else {
            tcpworkerGroup = new NioEventLoopGroup();
        }
    }

    public void run() throws InterruptedException {
        server = new ServerBootstrap();
        server.group(tcpbossGroup, tcpworkerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(RpcUtil.constructRpcFrameDecoder());
                        p.addLast(new RpcMessageParserStage());
                        p.addLast(rpcProgram);
                        p.addLast(new RpcTcpResponseStage());

                    }
                });
        ChannelFuture f = server.bind(new InetSocketAddress(port)).sync();
        boundPort = port;
//        f.channel().closeFuture().sync();
        LOG.info("Started listening to TCP requests at port " + boundPort + " for "
                + rpcProgram + " with workerCount " + workerCount);
    }

    // boundPort will be set only after server starts
    public int getBoundPort() {
        return this.boundPort;
    }

    public void shutdown() {
        if (tcpworkerGroup != null) {
            tcpworkerGroup.shutdownGracefully();
        }
        if (tcpbossGroup != null) {
            tcpbossGroup.shutdownGracefully();
        }
    }
}
