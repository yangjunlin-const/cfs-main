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
package com.buaa.cfs.nfs3;


import com.buaa.cfs.nfs3.response.WccAttr;
import com.buaa.cfs.common.oncrpc.XDR;

/**
 * File attrbutes reported in NFS. The fattr3 structure contains the basic attributes of a file. All servers should
 * support this set of attributes even if they have to simulate some of the fields. Type is the type of the file. Mode
 * is the protection mode bits. Nlink is the number of hard links to the file - that is, the number of different names
 * for the same file. Uid is the user ID of the owner of the file. Gid is the group ID of the group of the file. Size is
 * the size of the file in bytes. Used is the number of bytes of disk space that the file actually uses (which can be
 * smaller than the size because the file may have holes or it may be larger due to fragmentation). Rdev describes the
 * device file if the file type is NF3CHR or NF3BLK - see specdata3 on page 20. Fsid is the file system identifier for
 * the file system. Fileid is a number which uniquely identifies the file within its file system (on UNIX this would be
 * the inumber). Atime is the time when the file data was last accessed. Mtime is the time when the file data was last
 * modified. Ctime is the time when the attributes of the file were last changed. Writing to the file changes the ctime
 * in addition to the mtime.
 */
public class Nfs3FileAttributes {
    private int type;
    private int mode;
    private int nlink;
    private int uid;
    private int gid;
    private long size;
    private long used;
    private Specdata3 rdev;
    private long fsid;
    private long fileId;
    private NfsTime atime;
    private NfsTime mtime;
    private NfsTime ctime;

    /**
     * The interpretation of the two words depends on the type of file system object. For a block special (NF3BLK) or
     * character special (NF3CHR) file, specdata1 and specdata2 are the major and minor device numbers, respectively.
     * (This is obviously a UNIX-specific interpretation.) For all other file types, these two elements should either be
     * set to 0 or the values should be agreed upon by the client and server. If the client and server do not agree upon
     * the values, the client should treat these fields as if they are set to 0.
     */
    public static class Specdata3 {
        final int specdata1;
        final int specdata2;

        public Specdata3() {
            specdata1 = 0;
            specdata2 = 0;
        }

        public Specdata3(int specdata1, int specdata2) {
            this.specdata1 = specdata1;
            this.specdata2 = specdata2;
        }

        public int getSpecdata1() {
            return specdata1;
        }

        public int getSpecdata2() {
            return specdata2;
        }

        @Override
        public String toString() {
            return "(Specdata3: specdata1" + specdata1 + ", specdata2:" + specdata2
                    + ")";
        }
    }

    public Nfs3FileAttributes() {
        this(NfsFileType.NFSREG, 1, (short) 0, 0, 0, 0, 0, 0, 0, 0, new Specdata3());
    }

    public Nfs3FileAttributes(NfsFileType nfsType, int nlink, short mode, int uid,
            int gid, long size, long fsid, long fileId, long mtime, long atime, Specdata3 rdev) {
        this.type = nfsType.toValue();
        this.mode = mode;
        this.nlink = nlink;
        this.uid = uid;
        this.gid = gid;
        this.size = size;
        this.used = this.size;
        this.rdev = new Specdata3();
        this.fsid = fsid;
        this.fileId = fileId;
        this.mtime = new NfsTime(mtime);
        this.atime = atime != 0 ? new NfsTime(atime) : this.mtime;
        this.ctime = this.mtime;
        this.rdev = rdev;
    }

    public Nfs3FileAttributes(Nfs3FileAttributes other) {
        this.type = other.getType();
        this.mode = other.getMode();
        this.nlink = other.getNlink();
        this.uid = other.getUid();
        this.gid = other.getGid();
        this.size = other.getSize();
        this.used = other.getUsed();
        this.rdev = new Specdata3();
        this.fsid = other.getFsid();
        this.fileId = other.getFileId();
        this.mtime = new NfsTime(other.getMtime());
        this.atime = new NfsTime(other.getAtime());
        this.ctime = new NfsTime(other.getCtime());
    }

    public void serialize(XDR xdr) {
        xdr.writeInt(type);
        xdr.writeInt(mode);
        xdr.writeInt(nlink);
        xdr.writeInt(uid);
        xdr.writeInt(gid);
        xdr.writeLongAsHyper(size);
        xdr.writeLongAsHyper(used);
        xdr.writeInt(rdev.getSpecdata1());
        xdr.writeInt(rdev.getSpecdata2());
        xdr.writeLongAsHyper(fsid);
        xdr.writeLongAsHyper(fileId);
        atime.serialize(xdr);
        mtime.serialize(xdr);
        ctime.serialize(xdr);
    }

    public static Nfs3FileAttributes deserialize(XDR xdr) {
        Nfs3FileAttributes attr = new Nfs3FileAttributes();
        attr.type = xdr.readInt();
        attr.mode = xdr.readInt();
        attr.nlink = xdr.readInt();
        attr.uid = xdr.readInt();
        attr.gid = xdr.readInt();
        attr.size = xdr.readHyper();
        attr.used = xdr.readHyper();
        attr.rdev = new Specdata3(xdr.readInt(), xdr.readInt());
        attr.fsid = xdr.readHyper();
        attr.fileId = xdr.readHyper();
        attr.atime = NfsTime.deserialize(xdr);
        attr.mtime = NfsTime.deserialize(xdr);
        attr.ctime = NfsTime.deserialize(xdr);
        return attr;
    }

    @Override
    public String toString() {
        return String.format("type:%d, mode:%d, nlink:%d, uid:%d, gid:%d, " +
                        "size:%d, used:%d, rdev:%s, fsid:%d, fileid:%d, atime:%s, " +
                        "mtime:%s, ctime:%s",
                type, mode, nlink, uid, gid, size, used, rdev, fsid, fileId, atime,
                mtime, ctime);
    }

    public int getNlink() {
        return nlink;
    }

    public long getUsed() {
        return used;
    }

    public long getFsid() {
        return fsid;
    }

    public long getFileId() {
        return fileId;
    }

    public NfsTime getAtime() {
        return atime;
    }

    public NfsTime getMtime() {
        return mtime;
    }

    public NfsTime getCtime() {
        return ctime;
    }

    public int getType() {
        return type;
    }

    public WccAttr getWccAttr() {
        return new WccAttr(size, mtime, ctime);
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    public int getMode() {
        return this.mode;
    }

    public int getUid() {
        return this.uid;
    }

    public int getGid() {
        return this.gid;
    }

    public Specdata3 getRdev() {
        return rdev;
    }

    public void setRdev(Specdata3 rdev) {
        this.rdev = rdev;
    }
}
