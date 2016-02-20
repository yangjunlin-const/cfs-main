package com.buaa.cfs.client;

import com.buaa.cfs.exception.AccessControlException;
import com.buaa.cfs.fs.DirectoryListing;
import com.buaa.cfs.fs.FsStatus;
import com.buaa.cfs.fs.HdfsFileStatus;
import com.buaa.cfs.fs.Options;
import com.buaa.cfs.fs.permission.FsPermission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;

/**
 * Created by yjl on 2/19/16.
 */
public class DFSClient implements java.io.Closeable {

    @Override
    public void close() throws IOException {

    }

    /**
     * Get the file info for a specific file or directory.
     *
     * @param src The string representation of the path to the file
     *
     * @return object containing information regarding the file or null if file not found
     */
    public HdfsFileStatus getFileInfo(String src) throws IOException {
        return null;
    }

    /**
     * Set permissions to a file or directory.
     *
     * @param src        path name.
     * @param permission permission to set to
     */
    public void setPermission(String src, FsPermission permission)
            throws IOException {
    }

    /**
     * Set file or directory owner.
     *
     * @param src       path name.
     * @param username  user id.
     * @param groupname user group.
     */
    public void setOwner(String src, String username, String groupname)
            throws IOException {
    }

    /**
     * set the modification and access time of a file
     */
    public void setTimes(String src, long mtime, long atime) throws IOException {

    }

    /**
     * Resolve the *first* symlink, if any, in the path.
     */
    public String getLinkTarget(String path) throws IOException {
        return null;
    }

    /**
     * Create a directory (or hierarchy of directories) with the given name and permission.
     *
     * @param src          The path of the directory being created
     * @param permission   The permission of the directory being created. If permission == null, use {@link
     *                     FsPermission#getDefault()}.
     * @param createParent create missing parent directory if true
     *
     * @return True if the operation success.
     */
    public boolean mkdirs(String src, FsPermission permission,
            boolean createParent) throws IOException {
        return true;
    }

    /**
     * delete file or directory. delete contents of the directory if non empty and recursive set to true
     */
    public boolean delete(String src, boolean recursive) throws IOException {
        return true;
    }

    /**
     * Rename file or directory.
     */
    public void rename(String src, String dst, Options.Rename... options)
            throws IOException {
    }

    /**
     * Creates a symbolic link.
     */
    public void createSymlink(String target, String link, boolean createParent)
            throws IOException {
    }

    /**
     * Get the file info for a specific file or directory. If src refers to a symlink then the FileStatus of the link is
     * returned.
     *
     * @param src path to a file or directory.
     *            <p>
     *            For description of exceptions thrown
     */
    public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
        return null;
    }

    /**
     * Get a partial listing of the indicated directory No block locations need to be fetched
     */
    public DirectoryListing listPaths(String src, byte[] startAfter)
            throws IOException {
        return null;
    }

    /**
     */
    public FsStatus getDiskStatus() throws IOException {
        return null;
    }
}
