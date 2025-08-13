/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.grpc;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.shaded.grpc.generated.ClientServiceGrpc;
import org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest;
import org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC implementation of client service for HBase operations.
 * Lives in hbase-client to avoid shaded circular dependencies.
 */
@InterfaceAudience.Private
public class GrpcClientService implements GrpcClientServiceInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcClientService.class);

  private final Configuration conf;
  private final ConcurrentMap<ServerName, ManagedChannel> serverToChannel = new ConcurrentHashMap<>();
  private final ConcurrentMap<ServerName, ClientServiceGrpc.ClientServiceStub> serverToStub = new ConcurrentHashMap<>();

  private final boolean enabled;
  private final int connectionTimeoutMs;
  private final int requestTimeoutMs;
  private final int maxMessageSize;

  public GrpcClientService(Configuration conf) {
    this.conf = conf;
    this.enabled = GrpcClientConfiguration.isEnabled(conf);
    this.connectionTimeoutMs = GrpcClientConfiguration.getConnectionTimeout(conf);
    this.requestTimeoutMs = GrpcClientConfiguration.getRequestTimeout(conf);
    this.maxMessageSize = GrpcClientConfiguration.getMaxMessageSize(conf);
    LOG.info("GrpcClientService initialized - enabled: {}, connectionTimeout: {}ms, requestTimeout: {}ms", 
      enabled, connectionTimeoutMs, requestTimeoutMs);
  }

  // No-op helper removed: direct generated message types are used end-to-end

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  private int getGrpcPort() {
    return GrpcClientConfiguration.getRegionServerGrpcPort(conf);
  }

  private ManagedChannel getOrCreateChannel(ServerName serverName) {
    return serverToChannel.computeIfAbsent(serverName, this::createChannel);
  }

  private ManagedChannel createChannel(ServerName serverName) {
    InetSocketAddress address = new InetSocketAddress(serverName.getHostname(), getGrpcPort());
    LOG.debug("Creating gRPC channel to {}", address);
    return ManagedChannelBuilder
      .forAddress(address.getHostName(), address.getPort())
      .usePlaintext()
      .keepAliveTime(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS)
      .keepAliveTimeout(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS)
      .keepAliveWithoutCalls(true)
      .maxInboundMessageSize(maxMessageSize)
      .build();
  }

  private ClientServiceGrpc.ClientServiceStub getOrCreateStub(ServerName serverName) {
    return serverToStub.computeIfAbsent(serverName, this::createStub);
  }

  private ClientServiceGrpc.ClientServiceStub createStub(ServerName serverName) {
    ManagedChannel channel = getOrCreateChannel(serverName);
    return ClientServiceGrpc
      .newStub(channel)
      .withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<GetResponse> get(ServerName serverName, GetRequest request) {
    if (!enabled) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("gRPC client is not enabled"));
    }
    CompletableFuture<GetResponse> future = new CompletableFuture<>();
    try {
      ClientServiceGrpc.ClientServiceStub stub = getOrCreateStub(serverName);
      StreamObserver<GetResponse> responseObserver = new StreamObserver<GetResponse>() {
        @Override
        public void onNext(GetResponse value) {
          future.complete(value);
        }
        @Override
        public void onError(Throwable t) {
          LOG.error("gRPC Get failed for {}", serverName, t);
          future.completeExceptionally(t);
        }
        @Override
        public void onCompleted() {
        }
      };
      stub.get(request, responseObserver);
    } catch (RuntimeException e) {
      future.completeExceptionally(e);
    }
    return future;
  }


  @Override
  public void close() {
    LOG.info("Closing GrpcClientService");
    serverToStub.clear();
    serverToChannel.clear();
  }

}


