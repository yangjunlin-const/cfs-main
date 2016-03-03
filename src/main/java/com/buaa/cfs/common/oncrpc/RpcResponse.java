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

import java.net.SocketAddress;

/**
 * RpcResponse encapsulates a response to a RPC request. It contains the data
 * that is going to cross the wire, as well as the information of the remote
 * peer.
 */
public class RpcResponse {
    private final ByteBuf data;
    private final SocketAddress remoteAddress;

    public RpcResponse(ByteBuf data, SocketAddress remoteAddress) {
        this.data = data;
        this.remoteAddress = remoteAddress;
    }

    public ByteBuf data() {
        return data;
    }

    public SocketAddress remoteAddress() {
        return remoteAddress;
    }
}
