package com.yp.java.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author lh
 * @Package com.yp.java.netty
 * @Description: TODO
 * @date Date : 2018年10月25日 下午2:47
 */
public class SmartCarEncoder extends MessageToByteEncoder<SmartCarProtocol> {
    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, SmartCarProtocol msg, ByteBuf out) throws Exception {
        // 写入消息SmartCar的具体内容
        // 1.写入消息的开头的信息标志(int类型)
        out.writeInt(msg.getHead_data());
        // 2.写入消息的长度(int 类型)
        out.writeInt(msg.getContentLength());
        // 3.写入消息的内容(byte[]类型)
        out.writeBytes(msg.getContent());

    }
}
