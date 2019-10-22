/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.example.echo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * Handler implementation for the echo client.  It initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public class EchoClientHandler extends ChannelInboundHandlerAdapter {

    private final ByteBuf firstMessage;

    /**
     * Creates a client-side handler.
     */
    public EchoClientHandler() {
        firstMessage = Unpooled.buffer(EchoClient.SIZE);
        for (int i = 0; i < firstMessage.capacity(); i ++) {
            firstMessage.writeByte((byte) i);
        }
    }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    /**
     * outBound事件的传播起点：
     * 写操作一般的源头是从Netty 的通道channel开始的。当服务器需要发送一个业务消息到客户端，
     * 会使用到之前打开的客户端通道channel，调用通道channel的出站写操作write方法，完成写操作。
     *
     * 直接调用write方法是不能往对方channel中写入数据的, 因为这种方式只能写入到缓冲区,
     * 还要调用flush方法才能将缓冲区数据刷到channel中,或者直接调用writeAndFlush方法。
     *
     * 无论是从tail节点开始还是从当前节点开始调用write方法，最终都会到head节点，
     * 而头节点正是使用unsafe来具体完成这些操作的
     *
     * 在此调用接口方法：
     * {@link io.netty.channel.ChannelOutboundInvoker#write(Object)}  }
     * Channel接口继承了ChannelOutboundInvoker接口，因此write方法的默认基础实现在：
     * {@link io.netty.channel.AbstractChannel#write(Object)}
     * 一个通道一个pipeline流水线。一个流水线串起来一系列的Handler。通道将出站的操作，直接委托给了自己的成员——pipeline流水线:
     * {@link io.netty.channel.DefaultChannelPipeline#write(Object)}
     * 然鹅，Pipeline 又将出站操作，甩给双向链表的最后一个节点—— tail 节点。
     * 于是乎，在pipeline的链表上，tail节点，偏偏就是出站操作的启动节点。
     *
     * TailContext 类的定义中，并没有实现 write写出站的方法。这个write(Object msg) 方法，
     * 定义在TailContext的基类——AbstractChannelHandlerContext 中。
     *
     * 出站操作的迭代过程：
     * {@link io.netty.channel.AbstractChannelHandlerContext#write(Object)}
     * {@link io.netty.channel.AbstractChannelHandlerContext#write(Object, ChannelPromise)}
     * {@link io.netty.channel.AbstractChannelHandlerContext#write(Object, boolean, ChannelPromise)}
     * {@link io.netty.channel.AbstractChannelHandlerContext#findContextOutbound(int)}
     * next.write:{@link io.netty.channel.AbstractChannelHandlerContext#invokeWrite(Object, ChannelPromise)}
     * 最后执行Handler.write() ，
     * 默认的write 出站方法的实现，定义在
     * {@link io.netty.channel.ChannelOutboundHandlerAdapter#write(ChannelHandlerContext, Object, ChannelPromise)} 中.
     * 简单的调用context. write方法，回到了下一次小迭代的第一步。
     * 换句话说，默认的ChannelOutboundHandlerAdapter 中的handler方法，是流水线的迭代一个一个环节前后连接起来，的关键的一小步，保证了流水线不被中断掉。
     *
     * 反复进行小迭代，迭代处理完中间的业务handler之后，就会走到流水线的HeadContext。HeadContext完成出站的最后一棒
     */
      ctx.channel().write("test data");
      ctx.write("test data");
      ctx.writeAndFlush(firstMessage);

      //出站的read()，是由程序主动发起的read()操作，最终会转发到HeadContext的read操作上执行unsafe.read();
      ctx.read();
  }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
       ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
