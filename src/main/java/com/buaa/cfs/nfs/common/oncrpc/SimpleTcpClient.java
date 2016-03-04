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
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * A simple TCP based RPC client which just sends a request to a server.
 */
public class SimpleTcpClient {
    protected final String host;
    protected final int port;
    protected final XDR request;
    protected final boolean oneShot;

    public SimpleTcpClient(String host, int port, XDR request) {
        this(host, port, request, true);
    }

    public SimpleTcpClient(String host, int port, XDR request, Boolean oneShot) {
        this.host = host;
        this.port = port;
        this.request = request;
        this.oneShot = oneShot;
    }

    public void run() throws InterruptedException {
        Bootstrap server = new Bootstrap();
        EventLoopGroup tcpworkerGroup = new NioEventLoopGroup();
        server.group(tcpworkerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(RpcUtil.constructRpcFrameDecoder());
                        p.addLast(new SimpleTcpClientHandler(request));
                    }
                });
        ChannelFuture f = server.connect(new InetSocketAddress(host, port)).sync();

        if (oneShot) {
            f.channel().closeFuture().awaitUninterruptibly();
            tcpworkerGroup.shutdownGracefully();
        }
    }
}
