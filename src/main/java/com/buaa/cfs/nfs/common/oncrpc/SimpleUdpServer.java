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
package com.buaa.cfs.nfs.common.oncrpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Simple UDP server implemented based on netty.
 */
public class SimpleUdpServer {
    public static final Log LOG = LogFactory.getLog(SimpleUdpServer.class);

    //udp
    public static final int SEND_BUFFER_SIZE = 65536;
    public static final int RECEIVE_BUFFER_SIZE = 65536;

    protected final int port;
    protected final SimpleChannelInboundHandler rpcProgram;
    protected final int workerCount;
    protected int boundPort = -1; // Will be set after server starts
    private Bootstrap server;
    private Channel ch;
    EventLoopGroup group;


    public SimpleUdpServer(int port, SimpleChannelInboundHandler program,
            int workerCount) {
        this.port = port;
        this.rpcProgram = program;
        this.workerCount = workerCount;
        if (workerCount > 0) {
            group = new NioEventLoopGroup(workerCount);
        } else {
            group = new NioEventLoopGroup();
        }
    }

    public void run() throws InterruptedException {
        server = new Bootstrap();
        server.group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
//                        p.addLast(RpcUtil.constructRpcFrameDecoder());
                        p.addLast(new RpcMessageParserStage());
                        p.addLast(rpcProgram);
                        p.addLast(new RpcUdpResponseStage());
                    }
                });

        ChannelFuture f = server.bind(port).sync();
        boundPort = port;
//        f.channel().closeFuture().sync();

//        server.setOption("broadcast", "false");
//        server.setOption("sendBufferSize", SEND_BUFFER_SIZE);
//        server.setOption("receiveBufferSize", RECEIVE_BUFFER_SIZE);

        LOG.info("Started listening to UDP requests at port " + boundPort + " for "
                + rpcProgram + " with workerCount " + workerCount);
    }

    // boundPort will be set only after server starts
    public int getBoundPort() {
        return this.boundPort;
    }

    public void shutdown() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
