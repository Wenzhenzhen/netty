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
package io.netty.bootstrap;

import io.netty.channel.*;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link Bootstrap} sub-class which allows easy bootstrap of {@link ServerChannel}
 *
 */
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ServerBootstrap.class);

    private final Map<ChannelOption<?>, Object> childOptions = new ConcurrentHashMap<ChannelOption<?>, Object>();
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<AttributeKey<?>, Object>();
    private final ServerBootstrapConfig config = new ServerBootstrapConfig(this);
    private volatile EventLoopGroup childGroup;
    private volatile ChannelHandler childHandler;

    public ServerBootstrap() { }

    private ServerBootstrap(ServerBootstrap bootstrap) {
        super(bootstrap);
        childGroup = bootstrap.childGroup;
        childHandler = bootstrap.childHandler;
        childOptions.putAll(bootstrap.childOptions);
        childAttrs.putAll(bootstrap.childAttrs);
    }

    /**
     * Specify the {@link EventLoopGroup} which is used for the parent (acceptor) and the child (client).
     */
    @Override
    public ServerBootstrap group(EventLoopGroup group) {
        return group(group, group);
    }

    /**
     * Set the {@link EventLoopGroup} for the parent (acceptor) and the child (client). These
     * {@link EventLoopGroup}'s are used to handle all the events and IO for {@link ServerChannel} and
     * {@link Channel}'s.
     */
    public ServerBootstrap group(EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        super.group(parentGroup);
        ObjectUtil.checkNotNull(childGroup, "childGroup");
        if (this.childGroup != null) {
            throw new IllegalStateException("childGroup set already");
        }
        this.childGroup = childGroup;
        return this;
    }

    /**
     * Allow to specify a {@link ChannelOption} which is used for the {@link Channel} instances once they get created
     * (after the acceptor accepted the {@link Channel}). Use a value of {@code null} to remove a previous set
     * {@link ChannelOption}.
     */
    public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) {
        ObjectUtil.checkNotNull(childOption, "childOption");
        if (value == null) {
            childOptions.remove(childOption);
        } else {
            childOptions.put(childOption, value);
        }
        return this;
    }

    /**
     * Set the specific {@link AttributeKey} with the given value on every child {@link Channel}. If the value is
     * {@code null} the {@link AttributeKey} is removed
     */
    public <T> ServerBootstrap childAttr(AttributeKey<T> childKey, T value) {
        ObjectUtil.checkNotNull(childKey, "childKey");
        if (value == null) {
            childAttrs.remove(childKey);
        } else {
            childAttrs.put(childKey, value);
        }
        return this;
    }

    /**
     * Set the {@link ChannelHandler} which is used to serve the request for the {@link Channel}'s.
     */
    public ServerBootstrap childHandler(ChannelHandler childHandler) {
        this.childHandler = ObjectUtil.checkNotNull(childHandler, "childHandler");
        return this;
    }

    /**
     * 在服务端启动时参数的配置。
     * childGroup,childOptions,childAttrs,childHandler等参数被进行了单独配置。作为参数和ServerBootstrapAcceptor一起，
     * 被当作一个特殊的handle，封装到pipeline中。
     * ServerBootstrapAcceptor中的eventLoop为workGroup。
     * */
    @Override
    void init(Channel channel) {
        /**配置{@link AbstractBootstrap#options}*/
        setChannelOptions(channel, options0().entrySet().toArray(newOptionArray(0)), logger);
        /**配置{@link AbstractBootstrap#attrs}*/
        setAttributes(channel, attrs0().entrySet().toArray(newAttrArray(0)));

        //配置Pipeline
        ChannelPipeline p = channel.pipeline();

        //获取ServerBootstrapAcceptor配置参数
        //childGroup为workGroup
        final EventLoopGroup currentChildGroup = childGroup;
        final ChannelHandler currentChildHandler = childHandler;
        final Entry<ChannelOption<?>, Object>[] currentChildOptions =
                childOptions.entrySet().toArray(newOptionArray(0));
        final Entry<AttributeKey<?>, Object>[] currentChildAttrs = childAttrs.entrySet().toArray(newAttrArray(0));

    // 它提供了一种在{@link eventloop}注册后初始化{@link channel}的简单方法。
    /**
     * 问题：ChannelInitializer到底什么时候被触发？
     * addLast()方法调用链
     * ->{@link DefaultChannelPipeline#addLast(ChannelHandler...)}
     * ->{@link DefaultChannelPipeline#addLast(EventExecutorGroup, ChannelHandler...)}
     * -> 循环调用 {@link DefaultChannelPipeline#addLast(EventExecutorGroup, String, ChannelHandler)}
     * -> {@link DefaultChannelPipeline#addLast0(AbstractChannelHandlerContext)}将此Handler添加到链表尾部
     *     handler加入到链表尾部（ChannelPipeline）之后，开始进入Handler的handlerAdded()方法入口
     *    {@link DefaultChannelPipeline#callHandlerAdded0(AbstractChannelHandlerContext)}
     *    ->{@link AbstractChannelHandlerContext#callHandlerAdded()}
     *    执行刚刚加入的Handler的handlerAdded方法
     *    ->{@link ChannelHandler#handlerAdded(ChannelHandlerContext)}
     *    答：至此{@link ChannelInitializer#handlerAdded(ChannelHandlerContext)}方法被触发
     * */
    p.addLast(
        new ChannelInitializer<Channel>() {
          @Override
          public void initChannel(final Channel ch) {
            /**
             * 这里是服务端的Pipeline 整个bossGroup对应的pipeline结构：
             * ->头{@link io.netty.channel.DefaultChannelPipeline.HeadContext}
             * -> {@link ServerBootstrap.ServerBootstrapAcceptor} 封装客户端channel相关属性
             * ->尾{@link io.netty.channel.DefaultChannelPipeline.TailContext}
             */
            final ChannelPipeline pipeline = ch.pipeline();
            ChannelHandler handler = config.handler();
            if (handler != null) {
              pipeline.addLast(handler);
            }

            ch.eventLoop()
                .execute(
                    new Runnable() {
                      @Override
                      public void run() {
                        /**
                         * 配置ServerBootstrapAcceptor,作为Handle紧跟HeadContext；
                         * 当新连接接入的时候AbstractNioMessageChannel.NioMessageUnsafe#read()方法被调用，
                         * 最终调用fireChannelRead()，方法来触发下一个Handler的channelRead方法。
                         * 而这个Handler正是ServerBootstrapAcceptor
                         */
                        pipeline.addLast(
                            new ServerBootstrapAcceptor(
                                ch,
                                currentChildGroup,
                                currentChildHandler,
                                currentChildOptions,
                                currentChildAttrs));
                      }
                    });
          }
        });
    }

    @Override
    public ServerBootstrap validate() {
        super.validate();
        if (childHandler == null) {
            throw new IllegalStateException("childHandler not set");
        }
        if (childGroup == null) {
            logger.warn("childGroup is not set. Using parentGroup instead.");
            childGroup = config.group();
        }
        return this;
    }

    private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

        private final EventLoopGroup childGroup;
        private final ChannelHandler childHandler;
        private final Entry<ChannelOption<?>, Object>[] childOptions;
        private final Entry<AttributeKey<?>, Object>[] childAttrs;
        private final Runnable enableAutoReadTask;

        ServerBootstrapAcceptor(
                final Channel channel, EventLoopGroup childGroup, ChannelHandler childHandler,
                Entry<ChannelOption<?>, Object>[] childOptions, Entry<AttributeKey<?>, Object>[] childAttrs) {
            this.childGroup = childGroup;
            this.childHandler = childHandler;
            this.childOptions = childOptions;
            this.childAttrs = childAttrs;

            // Task which is scheduled to re-enable auto-read.
            // It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
            // not be able to load the class because of the file limit it already reached.
            //
            // See https://github.com/netty/netty/issues/1328
            enableAutoReadTask = new Runnable() {
                @Override
                public void run() {
                    channel.config().setAutoRead(true);
                }
            };
        }

        /**
         * 当新的连接 接入的时候AbstractNioMessageChannel.NioMessageUnsafe#read()方法被调用，
         * 最终调用fireChannelRead()，方法来触发下一个Handler的channelRead方法。
         * 而这个Handler正是ServerBootstrapAcceptor，它也是ChannelInboundHandler
         * 其中channelRead主要做了以下几件事。
         * 1.为客户端channel的pipeline添加childHandler
         * 2.设置客户端TCP相关属性childOptions和自定义属性childAttrs
         * 3.workGroup选择NioEventLoop并注册Selector，注册流程和服务端Channel一致
         *
         *
         * 4.向Selector注册读事件：触发了通道激活事件;一个入站事件的完整那个流程
         **/
        @Override
        @SuppressWarnings("unchecked")
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            //该channel为客户端接入时创建的channel
            final Channel child = (Channel) msg;

            //添加childHandle
            child.pipeline().addLast(childHandler);

            //设置TCP相关属性：childOptions
            setChannelOptions(child, childOptions, logger);
            //设置自定义属性:childAttrs
            setAttributes(child, childAttrs);

            try {
                /**
                 * 从childGroup中选择NioEventLoop并注册Selector：
                 * 流程：
                 * 1. 在{@link io.netty.channel.MultithreadEventLoopGroup#register(Channel)}中，
                 *    调用next()方法得到下一个EventLoop（即NioEventLoop），然后将通道注册到该NioEventLoop上。
                 * 2. {@link io.netty.channel.SingleThreadEventLoop#register(Channel)}，包装Channel并转发到下一个方法
                 * 3. {@link io.netty.channel.AbstractChannel.AbstractUnsafe#register(EventLoop, ChannelPromise)}
                 *      在这个方法中关联 EventLoop 与 Channel，调用register0()将Channel注册到EventLoop
                 * 4. {@link AbstractChannel.AbstractUnsafe#register0(ChannelPromise)}
                 *      a.调用实际注册方法doRegister()完成注册
                 *      b.注册完成，fire通道激活事件;调用HeadContent的fireChannelActive()方法
                 *      c.在通道激活事件之后，触发注册OP_READ 的操作
                 * 5. {@link io.netty.channel.nio.AbstractNioChannel#doRegister()}
                 * */
                childGroup.register(child).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            forceClose(child, future.cause());
                        }
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }

        private static void forceClose(Channel child, Throwable t) {
            child.unsafe().closeForcibly();
            logger.warn("Failed to register an accepted channel: {}", child, t);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final ChannelConfig config = ctx.channel().config();
            if (config.isAutoRead()) {
                // stop accept new connections for 1 second to allow the channel to recover
                // See https://github.com/netty/netty/issues/1328
                config.setAutoRead(false);
                ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
            }
            // still let the exceptionCaught event flow through the pipeline to give the user
            // a chance to do something with it
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public ServerBootstrap clone() {
        return new ServerBootstrap(this);
    }

    /**
     * Return the configured {@link EventLoopGroup} which will be used for the child channels or {@code null}
     * if non is configured yet.
     *
     * @deprecated Use {@link #config()} instead.
     */
    @Deprecated
    public EventLoopGroup childGroup() {
        return childGroup;
    }

    final ChannelHandler childHandler() {
        return childHandler;
    }

    final Map<ChannelOption<?>, Object> childOptions() {
        return copiedMap(childOptions);
    }

    final Map<AttributeKey<?>, Object> childAttrs() {
        return copiedMap(childAttrs);
    }

    @Override
    public final ServerBootstrapConfig config() {
        return config;
    }
}
