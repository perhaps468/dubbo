/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.api.pu.DefaultPuHandler;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.dubbo.common.constants.CommonConstants.EXECUTOR_MANAGEMENT_MODE_DEFAULT;
import static org.apache.dubbo.common.constants.CommonConstants.EXT_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.KEEP_ALIVE_KEY;

class PortUnificationServerTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(booleans = {false, true})
    void testAcceptedChannelKeepAlive(Boolean keepAlive) throws Throwable {
        FrameworkModel frameworkModel = new FrameworkModel();
        NettyPortUnificationServer server = null;
        try {
            ApplicationModel applicationModel = frameworkModel.newApplication();
            applicationModel.getApplicationConfigManager().setApplication(new ApplicationConfig("provider-app"));
            URL url = URL.valueOf("empty://127.0.0.1:" + NetUtils.getAvailablePort())
                    .setScopeModel(applicationModel.getDefaultModule());
            if (keepAlive != null) {
                url = url.addParameter(KEEP_ALIVE_KEY, keepAlive);
            }
            CompletableFuture<Channel> accepted = new CompletableFuture<>();
            server = new NettyPortUnificationServer(url, new ChannelHandlerAdapter() {
                @Override
                public void connected(Channel channel) {
                    accepted.complete(channel);
                }
            });
            server.bind();
            try (Socket socket = new Socket()) {
                socket.connect(server.getLocalAddress(), 5000);
                NettyChannel channel = (NettyChannel) accepted.get(5, TimeUnit.SECONDS);
                Assertions.assertEquals(
                        Boolean.TRUE.equals(keepAlive),
                        channel.getNioChannel().config().getOption(ChannelOption.SO_KEEPALIVE));
            }
        } finally {
            if (server != null) {
                server.close();
            }
            frameworkModel.destroy();
        }
    }

    @Test
    void testBind() throws Throwable {
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("empty://127.0.0.1:" + port + "?foo=bar&" + EXT_PROTOCOL + "=tri");
        ApplicationModel applicationModel = ApplicationModel.defaultModel();
        ApplicationConfig applicationConfig = new ApplicationConfig("provider-app");
        applicationConfig.setExecutorManagementMode(EXECUTOR_MANAGEMENT_MODE_DEFAULT);
        applicationModel.getApplicationConfigManager().setApplication(applicationConfig);
        ConfigManager configManager = new ConfigManager(applicationModel);
        configManager.setApplication(applicationConfig);
        configManager.getApplication();
        applicationModel.setConfigManager(configManager);
        url = url.setScopeModel(applicationModel);
        ModuleModel moduleModel = applicationModel.getDefaultModule();
        url = url.putAttribute(CommonConstants.SCOPE_MODEL, moduleModel);
        // abstract endpoint need to get codec of url(which is in triple package)
        final NettyPortUnificationServer server = new NettyPortUnificationServer(url, new DefaultPuHandler());
        server.bind();
        Assertions.assertTrue(server.isBound());
        Assertions.assertEquals(2, server.getProtocols().size());
        server.close();
    }
}
