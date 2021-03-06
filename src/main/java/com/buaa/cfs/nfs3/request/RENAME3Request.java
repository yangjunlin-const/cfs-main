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
package com.buaa.cfs.nfs3.request;

import org.apache.commons.io.Charsets;
import com.buaa.cfs.nfs3.FileHandle;
import com.buaa.cfs.common.oncrpc.XDR;

import java.io.IOException;

/**
 * RENAME3 Request
 */
public class RENAME3Request extends NFS3Request {
    private final FileHandle fromDirHandle;
    private final String fromName;
    private final FileHandle toDirHandle;
    private final String toName;

    public static RENAME3Request deserialize(XDR xdr) throws IOException {
        FileHandle fromDirHandle = readHandle(xdr);
        String fromName = xdr.readString();
        FileHandle toDirHandle = readHandle(xdr);
        String toName = xdr.readString();
        return new RENAME3Request(fromDirHandle, fromName, toDirHandle, toName);
    }

    public RENAME3Request(FileHandle fromDirHandle, String fromName,
            FileHandle toDirHandle, String toName) {
        this.fromDirHandle = fromDirHandle;
        this.fromName = fromName;
        this.toDirHandle = toDirHandle;
        this.toName = toName;
    }

    public FileHandle getFromDirHandle() {
        return fromDirHandle;
    }

    public String getFromName() {
        return fromName;
    }

    public FileHandle getToDirHandle() {
        return toDirHandle;
    }

    public String getToName() {
        return toName;
    }

    @Override
    public void serialize(XDR xdr) {
        fromDirHandle.serialize(xdr);
        xdr.writeInt(fromName.getBytes(Charsets.UTF_8).length);
        xdr.writeFixedOpaque(fromName.getBytes(Charsets.UTF_8));
        toDirHandle.serialize(xdr);
        xdr.writeInt(toName.getBytes(Charsets.UTF_8).length);
        xdr.writeFixedOpaque(toName.getBytes(Charsets.UTF_8));
    }
}
