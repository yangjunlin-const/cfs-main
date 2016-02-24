package com.buaa.cfs.nfs3;

import com.buaa.cfs.conf.NfsConfigKeys;
import com.buaa.cfs.conf.NfsConfiguration;
import com.buaa.cfs.mount.Mountd;
import com.buaa.cfs.utils.StringUtils;

import java.io.IOException;
import java.net.DatagramSocket;

/**
 * Nfs server. Supports NFS v3 using {@link RpcProgramNfs3}. Currently Mountd program is also started inside this class.
 * Only TCP server is supported and UDP is not supported.
 */
public class Nfs3 extends Nfs3Base {
    private Mountd mountd;

    public Nfs3(NfsConfiguration conf) throws IOException {
        this(conf, null, true);
    }

    public Nfs3(NfsConfiguration conf, DatagramSocket registrationSocket,
            boolean allowInsecurePorts) throws IOException {
        super(RpcProgramNfs3.createRpcProgramNfs3(conf, registrationSocket,
                allowInsecurePorts), conf);
        mountd = new Mountd(conf, registrationSocket, allowInsecurePorts);
    }

    public Mountd getMountd() {
        return mountd;
    }

    public void startServiceInternal(boolean register) throws IOException {
        mountd.start(register); // Start mountd
        start(register);
    }

    static void startService(String[] args,
            DatagramSocket registrationSocket) throws IOException {
        StringUtils.startupShutdownMessage(Nfs3.class, args, LOG);
        NfsConfiguration conf = new NfsConfiguration();
        boolean allowInsecurePorts = conf.getBoolean(
                NfsConfigKeys.DFS_NFS_PORT_MONITORING_DISABLED_KEY,
                NfsConfigKeys.DFS_NFS_PORT_MONITORING_DISABLED_DEFAULT);
        final Nfs3 nfsServer = new Nfs3(conf, registrationSocket,
                allowInsecurePorts);
        nfsServer.startServiceInternal(true);
    }

    public static void main(String[] args) throws IOException {
        startService(args, null);
        LOG.info("--- ********************* start nfs server success ************************");
    }
}
