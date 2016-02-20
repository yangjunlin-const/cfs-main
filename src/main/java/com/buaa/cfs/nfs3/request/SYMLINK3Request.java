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
 * SYMLINK3 Request
 */
public class SYMLINK3Request extends RequestWithHandle {
    private final String name;     // The name of the link
    private final SetAttr3 symAttr;
    private final String symData;  // It contains the target

    public static SYMLINK3Request deserialize(XDR xdr) throws IOException {
        FileHandle handle = readHandle(xdr);
        String name = xdr.readString();
        SetAttr3 symAttr = new SetAttr3();
        symAttr.deserialize(xdr);
        String symData = xdr.readString();
        return new SYMLINK3Request(handle, name, symAttr, symData);
    }

    public SYMLINK3Request(FileHandle handle, String name, SetAttr3 symAttr,
            String symData) {
        super(handle);
        this.name = name;
        this.symAttr = symAttr;
        this.symData = symData;
    }

    public String getName() {
        return name;
    }

    public SetAttr3 getSymAttr() {
        return symAttr;
    }

    public String getSymData() {
        return symData;
    }

    @Override
    public void serialize(XDR xdr) {
        handle.serialize(xdr);
        xdr.writeInt(name.getBytes(Charsets.UTF_8).length);
        xdr.writeFixedOpaque(name.getBytes(Charsets.UTF_8));
        symAttr.serialize(xdr);
        xdr.writeInt(symData.getBytes(Charsets.UTF_8).length);
        xdr.writeFixedOpaque(symData.getBytes(Charsets.UTF_8));
    }
}
