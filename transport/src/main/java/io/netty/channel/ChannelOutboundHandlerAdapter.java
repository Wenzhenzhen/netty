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
package io.netty.channel;

import io.netty.channel.ChannelHandlerMask.Skip;

import java.net.SocketAddress;

/**
 * {@link ChannelOutboundHandler}接口的实现类，作为骨架提供方法的默认实现.
 * 这个实现只是通过{@link ChannelHandlerContext}转发每个方法调用。
 * 如果要实现定制的出站业务逻辑，继承ChannelOutboundHandlerAdapter 适配器即可。
 * ChannelOutboundHandlerAdapter 面向的是通用的出站处理场景。
 *
 * 出站和入站，复用了同一个上下文Context基类。它就是{@link AbstractChannelHandlerContext}。
 * 在这个AbstractChannelHandlerContext基类中，定义了每一个出站操作的默认实现。
 */
public class ChannelOutboundHandlerAdapter extends ChannelHandlerAdapter
    implements ChannelOutboundHandler {

    /**
     * Calls {@link ChannelHandlerContext#bind(SocketAddress, ChannelPromise)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#connect(SocketAddress, SocketAddress, ChannelPromise)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
            SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.connect(remoteAddress, localAddress, promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#disconnect(ChannelPromise)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise)
            throws Exception {
        ctx.disconnect(promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#close(ChannelPromise)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
            throws Exception {
        ctx.close(promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#deregister(ChannelPromise)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.deregister(promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#read()} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
    }

  /**
   * Calls {@link ChannelHandlerContext#write(Object, ChannelPromise)} to forward to the next {@link
   * ChannelOutboundHandler} in the {@link ChannelPipeline}.
   *
   * 默认的出站处理方法，简单的调用context. write方法，回到了下一次小迭代的第一步。
   */
  @Skip
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
        ctx.write(msg, promise);
    }

    /**
     * Calls {@link ChannelHandlerContext#flush()} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Skip
    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}
