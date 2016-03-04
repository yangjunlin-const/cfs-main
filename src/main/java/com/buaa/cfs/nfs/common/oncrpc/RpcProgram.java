/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.buaa.cfs.nfs.common.oncrpc;

import com.buaa.cfs.nfs.common.oncrpc.security.Verifier;
import com.buaa.cfs.nfs.common.oncrpc.security.VerifierNone;
import com.buaa.cfs.nfs.common.portmap.PortmapMapping;
import com.buaa.cfs.nfs.common.portmap.PortmapRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Class for writing RPC server programs based on RFC 1050. Extend this class and implement {@link #handleInternal} to
 * handle the requests received.
 */
public abstract class RpcProgram extends SimpleChannelInboundHandler<Object> {
    static final Log LOG = LogFactory.getLog(RpcProgram.class);
    public static final int RPCB_PORT = 111;
    private final String program;
    private final String host;
    private int port; // Ephemeral port is chosen later
    private final int progNumber;
    private final int lowProgVersion;
    private final int highProgVersion;
    protected final boolean allowInsecurePorts;

    /**
     * If not null, this will be used as the socket to use to connect to the system portmap daemon when registering this
     * RPC server program.
     */
    private final DatagramSocket registrationSocket;

    /**
     * Constructor
     *
     * @param program            program name
     * @param host               host where the Rpc server program is started
     * @param port               port where the Rpc server program is listening to
     * @param progNumber         program number as defined in RFC 1050
     * @param lowProgVersion     lowest version of the specification supported
     * @param highProgVersion    highest version of the specification supported
     * @param registrationSocket if not null, use this socket to register with portmap daemon
     * @param allowInsecurePorts true to allow client connections from unprivileged ports, false otherwise
     */
    protected RpcProgram(String program, String host, int port, int progNumber,
            int lowProgVersion, int highProgVersion,
            DatagramSocket registrationSocket, boolean allowInsecurePorts) {
        this.program = program;
        this.host = host;
        this.port = port;
        this.progNumber = progNumber;
        this.lowProgVersion = lowProgVersion;
        this.highProgVersion = highProgVersion;
        this.registrationSocket = registrationSocket;
        this.allowInsecurePorts = allowInsecurePorts;
        LOG.info("Will " + (allowInsecurePorts ? "" : "not ") + "accept client "
                + "connections from unprivileged ports");
    }

    /**
     * Register this program with the local portmapper.
     */
    public void register(int transport, int boundPort) {
        if (boundPort != port) {
            LOG.info("The bound port is " + boundPort
                    + ", different with configured port " + port);
            port = boundPort;
        }
        // Register all the program versions with portmapper for a given transport
        for (int vers = lowProgVersion; vers <= highProgVersion; vers++) {
            PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport,
                    port);
            register(mapEntry, true);
        }
    }

    /**
     * Unregister this program with the local portmapper.
     */
    public void unregister(int transport, int boundPort) {
        if (boundPort != port) {
            LOG.info("The bound port is " + boundPort
                    + ", different with configured port " + port);
            port = boundPort;
        }
        // Unregister all the program versions with portmapper for a given transport
        for (int vers = lowProgVersion; vers <= highProgVersion; vers++) {
            PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport,
                    port);
            register(mapEntry, false);
        }
    }

    /**
     * Register the program with Portmap or Rpcbind
     */
    protected void register(PortmapMapping mapEntry, boolean set) {
        XDR mappingRequest = PortmapRequest.create(mapEntry, set);
        SimpleUdpClient registrationClient = new SimpleUdpClient(host, RPCB_PORT,
                mappingRequest, registrationSocket);
        try {
            registrationClient.run();
        } catch (IOException e) {
            String request = set ? "Registration" : "Unregistration";
            LOG.error(request + " failure with " + host + ":" + port
                    + ", portmap entry: " + mapEntry);
            throw new RuntimeException(request + " failure", e);
        }
    }

    // Start extra daemons or services
    public void startDaemons() {
    }

    public void stopDaemons() {
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object e)
            throws Exception {
        RpcInfo info = (RpcInfo) e;
        RpcCall call = (RpcCall) info.header();

        SocketAddress remoteAddress = info.remoteAddress();
        if (LOG.isTraceEnabled()) {
            LOG.trace(program + " procedure #" + call.getProcedure());
        }

        if (this.progNumber != call.getProgram()) {
            LOG.warn("Invalid RPC call program " + call.getProgram());
            sendAcceptedReply(call, remoteAddress, RpcAcceptedReply.AcceptState.PROG_UNAVAIL, ctx);
            return;
        }

        int ver = call.getVersion();
        if (ver < lowProgVersion || ver > highProgVersion) {
            LOG.warn("Invalid RPC call version " + ver);
            sendAcceptedReply(call, remoteAddress, RpcAcceptedReply.AcceptState.PROG_MISMATCH, ctx);
            return;
        }

        handleInternal(ctx, info);
    }

    public boolean doPortMonitoring(SocketAddress remoteAddress) {
        if (!allowInsecurePorts) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Will not allow connections from unprivileged ports. "
                        + "Checking for valid client port...");
            }

            if (remoteAddress instanceof InetSocketAddress) {
                InetSocketAddress inetRemoteAddress = (InetSocketAddress) remoteAddress;
                if (inetRemoteAddress.getPort() > 1023) {
                    LOG.warn("Connection attempted from '" + inetRemoteAddress + "' "
                            + "which is an unprivileged port. Rejecting connection.");
                    return false;
                }
            } else {
                LOG.warn("Could not determine remote port of socket address '"
                        + remoteAddress + "'. Rejecting connection.");
                return false;
            }
        }
        return true;
    }

    private void sendAcceptedReply(RpcCall call, SocketAddress remoteAddress,
            RpcAcceptedReply.AcceptState acceptState, ChannelHandlerContext ctx) {
        RpcAcceptedReply reply = RpcAcceptedReply.getInstance(call.getXid(),
                acceptState, Verifier.VERIFIER_NONE);

        XDR out = new XDR();
        reply.write(out);
        if (acceptState == RpcAcceptedReply.AcceptState.PROG_MISMATCH) {
            out.writeInt(lowProgVersion);
            out.writeInt(highProgVersion);
        }
        ByteBuf b = Unpooled.wrappedBuffer(out.asReadOnlyWrap()
                .buffer());
        RpcResponse rsp = new RpcResponse(b, remoteAddress);
        RpcUtil.sendRpcResponse(ctx, rsp);
    }

    protected static void sendRejectedReply(RpcCall call,
            SocketAddress remoteAddress, ChannelHandlerContext ctx) {
        XDR out = new XDR();
        RpcDeniedReply reply = new RpcDeniedReply(call.getXid(),
                RpcReply.ReplyState.MSG_DENIED,
                RpcDeniedReply.RejectState.AUTH_ERROR, new VerifierNone());
        reply.write(out);
        ByteBuf buf = Unpooled.wrappedBuffer(out.asReadOnlyWrap()
                .buffer());
        RpcResponse rsp = new RpcResponse(buf, remoteAddress);
        RpcUtil.sendRpcResponse(ctx, rsp);
    }

    protected abstract void handleInternal(ChannelHandlerContext ctx, RpcInfo info);

    @Override
    public String toString() {
        return "Rpc program: " + program + " at " + host + ":" + port;
    }

    protected abstract boolean isIdempotent(RpcCall call);

    public int getPort() {
        return port;
    }
}
