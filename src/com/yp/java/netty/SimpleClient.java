package com.yp.java.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/**
 * @author lh
 * @Package com.yp.java.netty
 * @Description: TODO
 * @date Date : 2018年10月10日 上午11:47
 */
public class SimpleClient {

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        String host="localhost";
        Integer port=8888;
        try {
            //创建Bootstrap
            Bootstrap b = new Bootstrap();
            //指定EventLoopGroup来处理客户端事件；需要EventLoopGroup的NIO实现
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new SmartCarEncoder());
                            ch.pipeline().addLast(new SmartCarDecoder());
                            ch.pipeline().addLast(new ClientHandler());
                        }
                    });
            //连接到远端，一直等到连接完成
            ChannelFuture f = b.connect(host,port).sync();
           // f.channel().writeAndFlush("hello");
            //一直阻塞到Channel关闭
            f.channel().closeFuture().sync();

        } finally {
            //关闭所有连接池，释放所有资源
            group.shutdownGracefully().sync();
        }
    }

}
