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
package org.apache.hadoop.hbase.shaded.grpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnStoreFileTrackerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;

/**
 * Helper utility to build protocol buffer requests, or build components for protocol buffer
 * requests.
 */
@InterfaceAudience.Private
public final class GrpcRequestConverter {

  private GrpcRequestConverter() {
  }

  // Start utilities for Client

  /**
   * Create a protocol buffer GetRequest for a client Get
   * @param regionName the name of the region to get
   * @param get        the client Get
   * @return a protocol buffer GetRequest
   */
  public static GetRequest buildGetRequest(final byte[] regionName, final Get get)
    throws IOException {
    GetRequest.Builder builder = GetRequest.newBuilder();
    org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier region = buildRegionSpecifier(
      org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setGet(GrpcUtil.toGet(get));
    return builder.build();
  }


  /**
   * Create a protocol buffer GetRegionLoadRequest for all regions/regions of a table.
   * @param tableName the table for which regionLoad should be obtained from RS
   * @return a protocol buffer GetRegionLoadRequest
   */
  public static GetRegionLoadRequest buildGetRegionLoadRequest(final TableName tableName) {
    GetRegionLoadRequest.Builder builder = GetRegionLoadRequest.newBuilder();
    if (tableName != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer GetOnlineRegionRequest
   * @return a protocol buffer GetOnlineRegionRequest
   */
  public static GetOnlineRegionRequest buildGetOnlineRegionRequest() {
    return GetOnlineRegionRequest.newBuilder().build();
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   * @param type  the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier buildRegionSpecifier(final org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType type,
    final byte[] value) {
    org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.Builder regionBuilder = org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.newBuilder();
    regionBuilder.setValue(com.google.protobuf.UnsafeByteOperations.unsafeWrap(value));
    regionBuilder.setType(type);
    return regionBuilder.build();
  }

  /**
   * Create a protocol buffer AddColumnRequest
   * @return an AddColumnRequest
   */
  public static AddColumnRequest buildAddColumnRequest(final TableName tableName,
    final ColumnFamilyDescriptor column, final long nonceGroup, final long nonce) {
    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setColumnFamilies(ProtobufUtil.toColumnFamilySchema(column));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer DeleteColumnRequest
   * @return a DeleteColumnRequest
   */
  public static DeleteColumnRequest buildDeleteColumnRequest(final TableName tableName,
    final byte[] columnName, final long nonceGroup, final long nonce) {
    DeleteColumnRequest.Builder builder = DeleteColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setColumnName(UnsafeByteOperations.unsafeWrap(columnName));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer ModifyColumnRequest
   * @return an ModifyColumnRequest
   */
  public static ModifyColumnRequest buildModifyColumnRequest(final TableName tableName,
    final ColumnFamilyDescriptor column, final long nonceGroup, final long nonce) {
    ModifyColumnRequest.Builder builder = ModifyColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setColumnFamilies(ProtobufUtil.toColumnFamilySchema(column));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  public static ModifyColumnStoreFileTrackerRequest buildModifyColumnStoreFileTrackerRequest(
    final TableName tableName, final byte[] family, final String dstSFT, final long nonceGroup,
    final long nonce) {
    ModifyColumnStoreFileTrackerRequest.Builder builder =
      ModifyColumnStoreFileTrackerRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setFamily(ByteString.copyFrom(family));
    builder.setDstSft(dstSFT);
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest for a single table
   * @param tableName the table name
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest
    buildGetTableDescriptorsRequest(final TableName tableName) {
    return GetTableDescriptorsRequest.newBuilder()
      .addTableNames(ProtobufUtil.toProtoTableName(tableName)).build();
  }

  /**
   * Creates a protocol buffer IsMasterRunningRequest
   * @return a IsMasterRunningRequest
   */
  public static IsMasterRunningRequest buildIsMasterRunningRequest() {
    return IsMasterRunningRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetBalancerRunningRequest
   * @return a SetBalancerRunningRequest
   */
  public static SetBalancerRunningRequest buildSetBalancerRunningRequest(boolean on,
    boolean synchronous) {
    return SetBalancerRunningRequest.newBuilder().setOn(on).setSynchronous(synchronous).build();
  }

  /**
   * Creates a protocol buffer IsBalancerEnabledRequest
   * @return a IsBalancerEnabledRequest
   */
  public static IsBalancerEnabledRequest buildIsBalancerEnabledRequest() {
    return IsBalancerEnabledRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer NormalizeRequest
   * @return a NormalizeRequest
   */
  public static NormalizeRequest buildNormalizeRequest(NormalizeTableFilterParams ntfp) {
    final NormalizeRequest.Builder builder = NormalizeRequest.newBuilder();
    if (ntfp.getTableNames() != null) {
      builder.addAllTableNames(ProtobufUtil.toProtoTableNameList(ntfp.getTableNames()));
    }
    if (ntfp.getRegex() != null) {
      builder.setRegex(ntfp.getRegex());
    }
    if (ntfp.getNamespace() != null) {
      builder.setNamespace(ntfp.getNamespace());
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer IsNormalizerEnabledRequest
   * @return a IsNormalizerEnabledRequest
   */
  public static IsNormalizerEnabledRequest buildIsNormalizerEnabledRequest() {
    return IsNormalizerEnabledRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetNormalizerRunningRequest
   * @return a SetNormalizerRunningRequest
   */
  public static SetNormalizerRunningRequest buildSetNormalizerRunningRequest(boolean on) {
    return SetNormalizerRunningRequest.newBuilder().setOn(on).build();
  }

  /**
   * Creates a protocol buffer IsSplitOrMergeEnabledRequest
   * @param switchType see {@link org.apache.hadoop.hbase.client.MasterSwitchType}
   * @return a IsSplitOrMergeEnabledRequest
   */
  public static IsSplitOrMergeEnabledRequest
    buildIsSplitOrMergeEnabledRequest(MasterSwitchType switchType) {
    IsSplitOrMergeEnabledRequest.Builder builder = IsSplitOrMergeEnabledRequest.newBuilder();
    builder.setSwitchType(convert(switchType));
    return builder.build();
  }

  /**
   * Creates a protocol buffer SetSplitOrMergeEnabledRequest
   * @param enabled     switch is enabled or not
   * @param synchronous set switch sync?
   * @param switchTypes see {@link org.apache.hadoop.hbase.client.MasterSwitchType}, it is a list.
   * @return a SetSplitOrMergeEnabledRequest
   */
  public static SetSplitOrMergeEnabledRequest buildSetSplitOrMergeEnabledRequest(boolean enabled,
    boolean synchronous, MasterSwitchType... switchTypes) {
    SetSplitOrMergeEnabledRequest.Builder builder = SetSplitOrMergeEnabledRequest.newBuilder();
    builder.setEnabled(enabled);
    builder.setSynchronous(synchronous);
    for (MasterSwitchType switchType : switchTypes) {
      builder.addSwitchTypes(convert(switchType));
    }
    return builder.build();
  }

  private static MasterProtos.MasterSwitchType convert(MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return MasterProtos.MasterSwitchType.SPLIT;
      case MERGE:
        return MasterProtos.MasterSwitchType.MERGE;
      default:
        break;
    }
    throw new UnsupportedOperationException("Unsupport switch type:" + switchType);
  }

  public static ReplicationProtos.AddReplicationPeerRequest buildAddReplicationPeerRequest(
    String peerId, ReplicationPeerConfig peerConfig, boolean enabled) {
    AddReplicationPeerRequest.Builder builder = AddReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    ReplicationProtos.ReplicationState.Builder stateBuilder =
      ReplicationProtos.ReplicationState.newBuilder();
    stateBuilder.setState(enabled
      ? ReplicationProtos.ReplicationState.State.ENABLED
      : ReplicationProtos.ReplicationState.State.DISABLED);
    builder.setPeerState(stateBuilder.build());
    return builder.build();
  }

  public static ReplicationProtos.RemoveReplicationPeerRequest
    buildRemoveReplicationPeerRequest(String peerId) {
    RemoveReplicationPeerRequest.Builder builder = RemoveReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static ReplicationProtos.EnableReplicationPeerRequest
    buildEnableReplicationPeerRequest(String peerId) {
    EnableReplicationPeerRequest.Builder builder = EnableReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static ReplicationProtos.DisableReplicationPeerRequest
    buildDisableReplicationPeerRequest(String peerId) {
    DisableReplicationPeerRequest.Builder builder = DisableReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static GetReplicationPeerConfigRequest
    buildGetReplicationPeerConfigRequest(String peerId) {
    GetReplicationPeerConfigRequest.Builder builder = GetReplicationPeerConfigRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static UpdateReplicationPeerConfigRequest
    buildUpdateReplicationPeerConfigRequest(String peerId, ReplicationPeerConfig peerConfig) {
    UpdateReplicationPeerConfigRequest.Builder builder =
      UpdateReplicationPeerConfigRequest.newBuilder();
    builder.setPeerId(peerId);
    builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    return builder.build();
  }

  public static ListReplicationPeersRequest buildListReplicationPeersRequest(Pattern pattern) {
    ListReplicationPeersRequest.Builder builder = ListReplicationPeersRequest.newBuilder();
    if (pattern != null) {
      builder.setRegex(pattern.toString());
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateNamespaceRequest
   * @return a CreateNamespaceRequest
   */
  public static CreateNamespaceRequest
    buildCreateNamespaceRequest(final NamespaceDescriptor descriptor) {
    CreateNamespaceRequest.Builder builder = CreateNamespaceRequest.newBuilder();
    builder.setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor));
    return builder.build();
  }

  /**
   * Creates a protocol buffer ModifyNamespaceRequest
   * @return a ModifyNamespaceRequest
   */
  public static ModifyNamespaceRequest
    buildModifyNamespaceRequest(final NamespaceDescriptor descriptor) {
    ModifyNamespaceRequest.Builder builder = ModifyNamespaceRequest.newBuilder();
    builder.setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteNamespaceRequest
   * @return a DeleteNamespaceRequest
   */
  public static DeleteNamespaceRequest buildDeleteNamespaceRequest(final String name) {
    DeleteNamespaceRequest.Builder builder = DeleteNamespaceRequest.newBuilder();
    builder.setNamespaceName(name);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetNamespaceDescriptorRequest
   * @return a GetNamespaceDescriptorRequest
   */
  public static GetNamespaceDescriptorRequest
    buildGetNamespaceDescriptorRequest(final String name) {
    GetNamespaceDescriptorRequest.Builder builder = GetNamespaceDescriptorRequest.newBuilder();
    builder.setNamespaceName(name);
    return builder.build();
  }

  public static ClearCompactionQueuesRequest buildClearCompactionQueuesRequest(Set<String> queues) {
    ClearCompactionQueuesRequest.Builder builder = ClearCompactionQueuesRequest.newBuilder();
    for (String name : queues) {
      builder.addQueueName(name);
    }
    return builder.build();
  }

  public static ClearDeadServersRequest
    buildClearDeadServersRequest(Collection<ServerName> deadServers) {
    ClearDeadServersRequest.Builder builder = ClearDeadServersRequest.newBuilder();
    for (ServerName server : deadServers) {
      builder.addServerName(ProtobufUtil.toServerName(server));
    }
    return builder.build();
  }

  /**
   * Returns a {@link GetSpaceQuotaRegionSizesRequest} object.
   */
  public static GetSpaceQuotaRegionSizesRequest buildGetSpaceQuotaRegionSizesRequest() {
    return GetSpaceQuotaRegionSizesRequest.getDefaultInstance();
  }

  /**
   * Returns a {@link GetSpaceQuotaSnapshotsRequest} object.
   */
  public static GetSpaceQuotaSnapshotsRequest buildGetSpaceQuotaSnapshotsRequest() {
    return GetSpaceQuotaSnapshotsRequest.getDefaultInstance();
  }

  /**
   * Returns a {@link GetQuotaStatesRequest} object.
   */
  public static GetQuotaStatesRequest buildGetQuotaStatesRequest() {
    return GetQuotaStatesRequest.getDefaultInstance();
  }

  public static DecommissionRegionServersRequest
    buildDecommissionRegionServersRequest(List<ServerName> servers, boolean offload) {
    return DecommissionRegionServersRequest.newBuilder()
      .addAllServerName(toProtoServerNames(servers)).setOffload(offload).build();
  }

  private static List<HBaseProtos.ServerName> toProtoServerNames(List<ServerName> servers) {
    List<HBaseProtos.ServerName> pbServers = new ArrayList<>(servers.size());
    for (ServerName server : servers) {
      pbServers.add(ProtobufUtil.toServerName(server));
    }
    return pbServers;
  }

}
