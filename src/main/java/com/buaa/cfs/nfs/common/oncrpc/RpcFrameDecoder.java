package com.buaa.cfs.nfs.common.oncrpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by root on 3/3/16.
 */
public class RpcFrameDecoder extends ByteToMessageDecoder {
    public static final Log LOG = LogFactory.getLog(RpcFrameDecoder.class);
    private ByteBuf currentFrame;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf,
            List<Object> out) {

        if (buf.readableBytes() < 4)
            return;

        buf.markReaderIndex();

        byte[] fragmentHeader = new byte[4];
        buf.readBytes(fragmentHeader);
        int length = XDR.fragmentSize(fragmentHeader);
        boolean isLast = XDR.isLastFragment(fragmentHeader);

        if (buf.readableBytes() < length) {
            buf.resetReaderIndex();
            return;
        }

        ByteBuf newFragment = buf.readSlice(length);
        if (currentFrame == null) {
            currentFrame = newFragment;
        } else {
            currentFrame = Unpooled.wrappedBuffer(currentFrame, newFragment);
        }
        if (isLast) {
            byte[] completeFrame = new byte[currentFrame.capacity()];
            currentFrame.getBytes(0, completeFrame);
            currentFrame = null;
            out.add(completeFrame);
        } else {
            return;
        }
    }
}
