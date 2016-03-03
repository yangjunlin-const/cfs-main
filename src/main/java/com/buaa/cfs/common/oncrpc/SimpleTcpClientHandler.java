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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple TCP based RPC client handler used by {@link SimpleTcpServer}.
 */
public class SimpleTcpClientHandler extends SimpleChannelInboundHandler<Object> {
    public static final Log LOG = LogFactory.getLog(SimpleTcpClient.class);
    protected final XDR request;

    public SimpleTcpClientHandler(XDR request) {
        this.request = request;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the request
        if (LOG.isDebugEnabled()) {
            LOG.debug("sending PRC request");
        }
        ByteBuf outBuf = XDR.writeMessageTcp(request, true);
        LOG.info("--- send request success.");
        ctx.writeAndFlush(outBuf);
    }

    /**
     * Shutdown connection by default. Subclass can override this method to do more interaction with the server.
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object e) {
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        LOG.warn("Unexpected exception from downstream: ", e.getCause());
        ctx.channel().close();
    }
}
