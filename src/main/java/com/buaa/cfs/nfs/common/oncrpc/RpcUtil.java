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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public final class RpcUtil {
    /**
     * The XID in RPC call. It is used for starting with new seed after each reboot.
     */
    private static int xid = (int) (System.currentTimeMillis() / 1000) << 12;

    public static int getNewXid(String caller) {
        return xid = ++xid + caller.hashCode();
    }

    public static void sendRpcResponse(ChannelHandlerContext ctx,
            RpcResponse response) {
        ctx.fireChannelRead(response);
    }

    public static ByteToMessageDecoder constructRpcFrameDecoder() {
        return new RpcFrameDecoder();
    }

}
