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
package com.buaa.cfs.nfs.common.oncrpc.security;

import com.buaa.cfs.nfs.common.oncrpc.RpcCall;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.buaa.cfs.nfs.common.oncrpc.XDR;

import java.io.IOException;

public abstract class SecurityHandler {
    public static final Log LOG = LogFactory.getLog(SecurityHandler.class);

    public abstract String getUser();

    public abstract boolean shouldSilentlyDrop(RpcCall request);

    public abstract Verifier getVerifer(RpcCall request) throws IOException;

    public boolean isUnwrapRequired() {
        return false;
    }

    public boolean isWrapRequired() {
        return false;
    }

    /** Used by GSS */
    public XDR unwrap(RpcCall request, byte[] data) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** Used by GSS */
    public byte[] wrap(RpcCall request, XDR response) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** Used by AUTH_SYS */
    public int getUid() {
        throw new UnsupportedOperationException();
    }

    /** Used by AUTH_SYS */
    public int getGid() {
        throw new UnsupportedOperationException();
    }

    /** Used by AUTH_SYS */
    public int[] getAuxGids() {
        throw new UnsupportedOperationException();
    }
}
