/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.buaa.cfs.nfs3.response;


import com.buaa.cfs.common.oncrpc.RpcAcceptedReply;
import com.buaa.cfs.common.oncrpc.XDR;
import com.buaa.cfs.common.oncrpc.security.Verifier;

/**
 * Base class for a NFSv3 response. This class and its subclasses contain the response from NFSv3 handlers.
 */
public class NFS3Response {
    protected int status;

    public NFS3Response(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Write the response, along with the rpc header (including verifier), to the XDR.
     */
    public XDR serialize(XDR out, int xid, Verifier verifier) {
        RpcAcceptedReply reply = RpcAcceptedReply.getAcceptInstance(xid, verifier);
        reply.write(out);
        out.writeInt(this.getStatus());
        return out;
    }
}
