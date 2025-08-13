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
package org.apache.hadoop.hbase.regionserver.grpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
 
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.ClientInternalHelper;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.shaded.grpc.GrpcUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.grpc.GrpcMessageBridge;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.replication.regionserver.RejectRequestsFromClientStateChecker;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
 
import org.apache.hadoop.hbase.client.RegionInfo;

import org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest;
import org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse;
import org.apache.hadoop.hbase.shaded.grpc.generated.MutateRequest;
import org.apache.hadoop.hbase.shaded.grpc.generated.MutateResponse;
 

/**
 * Minimal protobuf-based service for RegionServer exposing ClientService, delegating to existing
 * {@link RSRpcServices} methods by bridging message types.
 * 
 * Note: This implementation uses protobuf RPC patterns, not gRPC. The name is kept for compatibility
 * but this is actually a protobuf service that bridges between the shaded GRPC protobuf messages
 * and the standard HBase protobuf messages.
 */
@InterfaceAudience.Private
public class RegionServerGrpcService {

  private static final Logger LOG = LoggerFactory.getLogger(RegionServerGrpcService.class);

  public static final String GRPC_ENABLED_KEY = "hbase.regionserver.grpc.enabled";
  public static final String GRPC_PORT_KEY = "hbase.regionserver.grpc.port";
  public static final int DEFAULT_GRPC_PORT = 16070; // default distinct from RPC

  private final HRegionServer regionServer;
  private final RSRpcServices rsRpcServices;
  private Server grpcServer;

  public RegionServerGrpcService(HRegionServer regionServer, RSRpcServices rsRpcServices) {
    this.regionServer = Objects.requireNonNull(regionServer, "regionServer");
    this.rsRpcServices = Objects.requireNonNull(rsRpcServices, "rsRpcServices");
  }

  public void start() throws IOException {
    Configuration conf = regionServer.getConfiguration();
    if (!conf.getBoolean(GRPC_ENABLED_KEY, false)) {
      LOG.info("RegionServer gRPC disabled by config: {}=false", GRPC_ENABLED_KEY);
      return;
    }
    int port = conf.getInt(GRPC_PORT_KEY, DEFAULT_GRPC_PORT);
    String host = regionServer.getRpcServices().getSocketAddress().getHostString();
    InetSocketAddress bindAddr = new InetSocketAddress(host, port);
    this.grpcServer = ServerBuilder.forPort(bindAddr.getPort())
    .addService(new ClientServiceAdapter())
    .build()
    .start();
    LOG.info("Started RegionServer gRPC on {}", bindAddr);
  }

  public void stop() {
    LOG.info("Stopped RegionServer gRPC service");
  }

  

  private final class ClientServiceAdapter implements BindableService {
    private void handleGet(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      long before = System.currentTimeMillis();
      OperationQuota quota = null;
      HRegion region = null;
      try {
        // checkOpen equivalent
        checkRegionServerOpen();

        // Locate region
        region = resolveRegion(request.getRegion());

        // Reject if in standby
        rejectIfInStandByState(region);

        // Quota check
        quota = regionServer.getRegionServerRpcQuotaManager()
          .checkBatchQuota(region, OperationQuota.OperationType.GET);


        // Handle deprecated closest_row_before check same as RPC
//        if (pbGetShaded.hasClosestRowBefore() && pbGetShaded.getClosestRowBefore()) {
//          throw new UnknownProtocolException(
//            "Is this a pre-hbase-1.0.0 or asynchbase client? Client is invoking getClosestRowBefore removed in hbase-2.0.0 replaced by reverse Scan.");
//        }
        Get clientGet = GrpcUtil.toGet(request.getGet());

        Boolean existence = null;
        Result r = null;

//        if (pbGetShaded.getExistenceOnly() && region.getCoprocessorHost() != null) {
//          existence = region.getCoprocessorHost().preExists(clientGet);
//        }

        if (existence == null) {
          // No RpcCallContext in gRPC path; call region.get directly
          r = region.get(clientGet);
//          if (pbGetShaded.getExistenceOnly()) {
//            boolean exists = r.getExists();
//            if (region.getCoprocessorHost() != null) {
//              exists = region.getCoprocessorHost().postExists(clientGet, exists);
//            }
//            existence = exists;
//          }
        }

        GetResponse.Builder respBuilder = GetResponse.newBuilder();
        org.apache.hadoop.hbase.shaded.grpc.generated.Result result;
        result = GrpcUtil.toResult(r);
//          ((HBaseRpcController) controller).setCellScanner(
//            PrivateCellUtil.createExtendedCellScanner(ClientInternalHelper.getExtendedRawCells(r)));
//          addSize(context, r);
        //        if (existence != null) {
//          org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Result pbResultShaded =
//            ProtobufUtil.toResult(existence, region.getRegionInfo().getReplicaId() != 0);
//          org.apache.hadoop.hbase.shaded.grpc.generated.Result grpcResult =
//            org.apache.hadoop.hbase.shaded.grpc.generated.Result.parseFrom(
//              pbResultShaded.toByteArray());
//          respBuilder.setResult(grpcResult);
//        } else if (r != null) {
//          org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Result pbResultShaded;
//          // No cellblocks in gRPC path; embed cells fully
//          pbResultShaded = ProtobufUtil.toResult(r);
//          org.apache.hadoop.hbase.shaded.grpc.generated.Result grpcResult =
//            org.apache.hadoop.hbase.shaded.grpc.generated.Result.parseFrom(
//              pbResultShaded.toByteArray());
//          respBuilder.setResult(grpcResult);
//        }
//        org.apache.hadoop.hbase.shaded.grpc.generated.Result result = GrpcUtil.toResult(r);
        respBuilder.setResult(result);

        if (r != null && r.rawCells() != null) {
          quota.addGetResult(r);
        }

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
      } catch (IOException e) {
        responseObserver.onError(e);
      } finally {
        // metrics
        try {
          if (region != null && regionServer.getMetrics() != null) {
            long elapsed = System.currentTimeMillis() - before;
            // We do not track blockBytesScanned here; pass 0
            regionServer.getMetrics().updateGet(region, elapsed, 0L);
          }
        } catch (Throwable ignore) {
          // best-effort metrics
        }
        if (quota != null) {
          try { quota.close(); } catch (Throwable ignore) {}
        }
      }
    }
    private boolean isClientCellBlockSupport(RpcCallContext context) {
      return context != null && context.isClientCellBlockSupported();
    }

    private void handleMutate(MutateRequest request, StreamObserver<MutateResponse> responseObserver) {
      try {
        org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest pbReq =
          GrpcMessageBridge.convert(
            request,
            org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest.class);
        org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse pbResp =
          rsRpcServices.mutate(null, pbReq);
        MutateResponse resp = GrpcMessageBridge.convert(pbResp, MutateResponse.class);
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
      } catch (ServiceException e) {
        responseObserver.onError(e);
      }
    }

    @Override
    public ServerServiceDefinition bindService() {
      MethodDescriptor<GetRequest, GetResponse> getMethod =
        MethodDescriptor.<GetRequest, GetResponse>newBuilder()
          .setType(MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("hbase.grpc.ClientService", "Get"))
          .setRequestMarshaller(ProtoUtils.marshaller(GetRequest.getDefaultInstance()))
          .setResponseMarshaller(ProtoUtils.marshaller(GetResponse.getDefaultInstance()))
          .build();

      return ServerServiceDefinition.builder("hbase.grpc.ClientService")
        .addMethod(getMethod, ServerCalls.asyncUnaryCall(this::handleGet))
        .build();
    }
  }

  private void checkRegionServerOpen() throws IOException {
    if (regionServer.isAborted()) {
      throw new RegionServerAbortedException("Server " + regionServer.getServerName() + " aborting");
    }
    if (regionServer.isStopped()) {
      throw new RegionServerStoppedException("Server " + regionServer.getServerName() + " stopping");
    }
    // skip data filesystem check; not visible from this package
    if (!regionServer.isOnline()) {
      throw new ServerNotRunningYetException(
        "Server " + regionServer.getServerName() + " is not running yet");
    }
  }

  private HRegion resolveRegion(org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier regionSpec)
    throws IOException {
    org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType type =
      regionSpec.getType();
    byte[] value = regionSpec.getValue().toByteArray();
    String encodedName;
    switch (type) {
      case ENCODED_REGION_NAME:
        encodedName = Bytes.toString(value);
        break;
      case REGION_NAME:
        encodedName = RegionInfo.encodeRegionName(value);
        break;
      default:
        throw new DoNotRetryIOException("Unsupported RegionSpecifierType: " + type);
    }
    HRegion region = regionServer.getRegion(encodedName);
    if (region == null) {
      throw new DoNotRetryIOException("The region " + encodedName + " is not online, and is not opening.");
    }
    return region;
  }

  private boolean shouldRejectRequestsFromClient(HRegion region) {
    ReplicationSourceService service = regionServer.getReplicationSourceService();
    return service != null &&
      service.getSyncReplicationPeerInfoProvider().checkState(
        region.getRegionInfo().getTable(), RejectRequestsFromClientStateChecker.get());
  }

  private void rejectIfInStandByState(HRegion region) throws DoNotRetryIOException {
    if (shouldRejectRequestsFromClient(region)) {
      throw new DoNotRetryIOException(region.getRegionInfo().getRegionNameAsString() + " is in STANDBY state.");
    }
  }
}


