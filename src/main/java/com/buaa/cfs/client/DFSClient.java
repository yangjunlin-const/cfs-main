package com.buaa.cfs.client;

import com.buaa.cfs.fs.*;
import com.buaa.cfs.fs.permission.FsAction;
import com.buaa.cfs.fs.permission.FsPermission;
import com.buaa.cfs.utils.FileUtil;
import org.mortbay.log.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yjl on 2/19/16.
 */
public class DFSClient implements java.io.Closeable {

    private static String preFile = DFSClient.class.getResource("").getPath();
    public static Map<Long, String> fileId_fileName = new ConcurrentHashMap<>();

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
        String realSrc = preFile.substring(0, preFile.length() - 1) + src;
        Log.info("--- the real src path is : " + realSrc);
        Path srcPath = Paths.get(realSrc);
        BasicFileAttributes basicFileAttributes = Files.readAttributes(srcPath, BasicFileAttributes.class);
        PosixFileAttributes posixFileAttributes = Files.readAttributes(srcPath, PosixFileAttributes.class);
        byte[] path = srcPath.toAbsolutePath().toString().getBytes();
        long length = basicFileAttributes.size();
        boolean isdir = basicFileAttributes.isDirectory();
        short block_replication = 1;
        long blocksize = 128 * 1024;
        long access_time = basicFileAttributes.lastAccessTime().toMillis();
        long modification_time = basicFileAttributes.lastModifiedTime().toMillis();
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        String owner = posixFileAttributes.owner().getName();
        String group = posixFileAttributes.group().getName();
        long filedId = FileUtil.getFileId(srcPath);
        fileId_fileName.put(filedId, realSrc);
        FileEncryptionInfo feInfo = null;
        int childrenNum = 0;
        if (Files.isDirectory(srcPath)) {
            childrenNum = srcPath.toFile().listFiles().length;
        }
        byte[] symlink = null;
        byte storagePolicy = 0;
        HdfsFileStatus hdfsFileStatus = new HdfsFileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, symlink, path, filedId, childrenNum, feInfo, storagePolicy);
        return hdfsFileStatus;
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
        String realSrc = src;
        Log.info("--- the real src path is : " + realSrc);
        Path srcPath = Paths.get(realSrc);
        BasicFileAttributes basicFileAttributes = Files.readAttributes(srcPath, BasicFileAttributes.class);
        PosixFileAttributes posixFileAttributes = Files.readAttributes(srcPath, PosixFileAttributes.class);
        byte[] path = srcPath.toAbsolutePath().toString().getBytes();
        long length = basicFileAttributes.size();
        boolean isdir = basicFileAttributes.isDirectory();
        short block_replication = 1;
        long blocksize = 128 * 1024;
        long access_time = basicFileAttributes.lastAccessTime().toMillis();
        long modification_time = basicFileAttributes.lastModifiedTime().toMillis();
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        String owner = posixFileAttributes.owner().getName();
        String group = posixFileAttributes.group().getName();
        long filedId = FileUtil.getFileId(srcPath);
        FileEncryptionInfo feInfo = null;
        int childrenNum = 0;
        if (Files.isDirectory(srcPath)) {
            childrenNum = srcPath.toFile().listFiles().length;
        }
        byte[] symlink = null;
        byte storagePolicy = 0;
        HdfsFileStatus hdfsFileStatus = new HdfsFileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, symlink, path, filedId, childrenNum, feInfo, storagePolicy);
        return hdfsFileStatus;
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
