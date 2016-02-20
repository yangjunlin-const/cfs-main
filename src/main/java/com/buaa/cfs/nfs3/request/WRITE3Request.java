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

import com.buaa.cfs.nfs3.FileHandle;
import com.buaa.cfs.constant.Nfs3Constant;
import com.buaa.cfs.common.oncrpc.XDR;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * WRITE3 Request
 */
public class WRITE3Request extends RequestWithHandle {
    private long offset;
    private int count;
    private final Nfs3Constant.WriteStableHow stableHow;
    private final ByteBuffer data;

    public static WRITE3Request deserialize(XDR xdr) throws IOException {
        FileHandle handle = readHandle(xdr);
        long offset = xdr.readHyper();
        int count = xdr.readInt();
        Nfs3Constant.WriteStableHow stableHow = Nfs3Constant.WriteStableHow.fromValue(xdr.readInt());
        ByteBuffer data = ByteBuffer.wrap(xdr.readFixedOpaque(xdr.readInt()));
        return new WRITE3Request(handle, offset, count, stableHow, data);
    }

    public WRITE3Request(FileHandle handle, final long offset, final int count,
            final Nfs3Constant.WriteStableHow stableHow, final ByteBuffer data) {
        super(handle);
        this.offset = offset;
        this.count = count;
        this.stableHow = stableHow;
        this.data = data;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Nfs3Constant.WriteStableHow getStableHow() {
        return this.stableHow;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    @Override
    public void serialize(XDR xdr) {
        handle.serialize(xdr);
        xdr.writeLongAsHyper(offset);
        xdr.writeInt(count);
        xdr.writeInt(stableHow.getValue());
        xdr.writeInt(count);
        xdr.writeFixedOpaque(data.array(), count);
    }

    @Override
    public String toString() {
        return String.format("fileId: %d offset: %d count: %d stableHow: %s",
                handle.getFileId(), offset, count, stableHow.name());
    }
}
