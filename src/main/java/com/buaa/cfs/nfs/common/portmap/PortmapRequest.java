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
package com.buaa.cfs.nfs.common.portmap;


import com.buaa.cfs.nfs.common.oncrpc.RpcCall;
import com.buaa.cfs.nfs.common.oncrpc.RpcUtil;
import com.buaa.cfs.nfs.common.oncrpc.XDR;
import com.buaa.cfs.nfs.common.oncrpc.security.CredentialsNone;
import com.buaa.cfs.nfs.common.oncrpc.security.VerifierNone;

/**
 * Helper utility for building portmap request
 */
public class PortmapRequest {
    public static PortmapMapping mapping(XDR xdr) {
        return PortmapMapping.deserialize(xdr);
    }

    public static XDR create(PortmapMapping mapping, boolean set) {
        XDR request = new XDR();
        int procedure = set ? RpcProgramPortmapTCP.PMAPPROC_SET
                : RpcProgramPortmapTCP.PMAPPROC_UNSET;
        RpcCall call = RpcCall.getInstance(
                RpcUtil.getNewXid(String.valueOf(RpcProgramPortmapTCP.PROGRAM)),
                RpcProgramPortmapTCP.PROGRAM, RpcProgramPortmapTCP.VERSION, procedure,
                new CredentialsNone(), new VerifierNone());
        call.write(request);
        return mapping.serialize(request);
    }
}
