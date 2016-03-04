
package com.buaa.cfs.nfs.common.oncrpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;


public class SimpleUdpClient {
    public static final Log LOG = LogFactory.getLog(SimpleUdpClient.class);

    protected final String host;
    protected final int port;
    protected final XDR request;
    protected final boolean oneShot;
    protected final DatagramSocket clientSocket;


    public SimpleUdpClient(String host, int port, XDR request,
            DatagramSocket clientSocket) {
        this(host, port, request, true, clientSocket);
    }

    public SimpleUdpClient(String host, int port, XDR request, Boolean oneShot,
            DatagramSocket clientSocket) {
        this.host = host;
        this.port = port;
        this.request = request;
        this.oneShot = oneShot;
        this.clientSocket = clientSocket;
    }

    public void run() throws IOException {

        EventLoopGroup group = new NioEventLoopGroup();
        byte[] sendData = request.getBytes();
        InetAddress IPAddress = InetAddress.getByName(host);
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, false)
                    .handler(new SimpleUdpClientHandler(new DatagramPacket(Unpooled.copiedBuffer(sendData), new InetSocketAddress(IPAddress, port))));
            b.connect(new InetSocketAddress(IPAddress, port)).sync();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            group.shutdownGracefully();
        } finally {
            group.shutdownGracefully();
        }
    }
}
