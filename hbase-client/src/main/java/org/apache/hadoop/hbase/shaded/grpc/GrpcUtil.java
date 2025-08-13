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

import static org.apache.hadoop.hbase.protobuf.ProtobufMagic.PB_MAGIC;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ServerTask;
import org.apache.hadoop.hbase.ServerTaskBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ClientInternalHelper;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.QueryMetrics;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLoadStats;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.SlowLogParams;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufMessageConverter;
import org.apache.hadoop.hbase.quotas.QuotaScope;
import org.apache.hadoop.hbase.quotas.QuotaType;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.ReflectedFunctionCache;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Parser;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HFileProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;



import com.google.protobuf.UnsafeByteOperations;
/**
 * Protobufs utility. Be aware that a class named org.apache.hadoop.hbase.protobuf.ProtobufUtil
 * (i.e. no 'shaded' in the package name) carries a COPY of a subset of this class for non-shaded
 * users; e.g. Coprocessor Endpoints. If you make change in here, be sure to make change in the
 * companion class too (not the end of the world, especially if you are adding new functionality but
 * something to be aware of.
 */
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class
public final class GrpcUtil {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcUtil.class.getName());

  private GrpcUtil() {
  }

  /**
   * Many results are simple: no cell, exists true or false. To save on object creations, we reuse
   * them across calls.
   */
  private final static ExtendedCell[] EMPTY_CELL_ARRAY = new ExtendedCell[0];
  private final static Result EMPTY_RESULT = Result.create(EMPTY_CELL_ARRAY);
  final static Result EMPTY_RESULT_EXISTS_TRUE = Result.create(null, true);
  final static Result EMPTY_RESULT_EXISTS_FALSE = Result.create(null, false);
  private final static Result EMPTY_RESULT_STALE = Result.create(EMPTY_CELL_ARRAY, null, true);
  private final static Result EMPTY_RESULT_EXISTS_TRUE_STALE =
    Result.create((Cell[]) null, true, true);
  private final static Result EMPTY_RESULT_EXISTS_FALSE_STALE =
    Result.create((Cell[]) null, false, true);

  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB;
  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB_EXISTS_TRUE;
  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB_EXISTS_FALSE;
  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB_STALE;
  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB_EXISTS_TRUE_STALE;
  private final static org.apache.hadoop.hbase.shaded.grpc.generated.Result EMPTY_RESULT_PB_EXISTS_FALSE_STALE;

  static {
    org.apache.hadoop.hbase.shaded.grpc.generated.Result.Builder builder = org.apache.hadoop.hbase.shaded.grpc.generated.Result.newBuilder();

    builder.setExists(true);
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB_EXISTS_TRUE = builder.build();

    builder.setStale(true);
    EMPTY_RESULT_PB_EXISTS_TRUE_STALE = builder.build();
    builder.clear();

    builder.setExists(false);
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB_EXISTS_FALSE = builder.build();
    builder.setStale(true);
    EMPTY_RESULT_PB_EXISTS_FALSE_STALE = builder.build();

    builder.clear();
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB = builder.build();
    builder.setStale(true);
    EMPTY_RESULT_PB_STALE = builder.build();
  }

  private static volatile boolean classLoaderLoaded = false;

  /**
   * Dynamic class loader to load filter/comparators
   */
  private final static class ClassLoaderHolder {
    private final static ClassLoader CLASS_LOADER;

    static {
      ClassLoader parent = GrpcUtil.class.getClassLoader();
      Configuration conf = HBaseConfiguration.create();
      CLASS_LOADER = AccessController
        .doPrivileged((PrivilegedAction<ClassLoader>) () -> new DynamicClassLoader(conf, parent));
      classLoaderLoaded = true;
    }
  }

  public static boolean isClassLoaderLoaded() {
    return classLoaderLoaded;
  }

  private static final String PARSE_FROM = "parseFrom";

  // We don't bother using the dynamic CLASS_LOADER above, because currently we can't support
  // optimizing dynamically loaded classes. We can do it once we build for java9+, see the todo
  // in ReflectedFunctionCache
  private static final ReflectedFunctionCache<byte[], Filter> FILTERS =
    new ReflectedFunctionCache<>(Filter.class, byte[].class, PARSE_FROM);
  private static final ReflectedFunctionCache<byte[], ByteArrayComparable> COMPARATORS =
    new ReflectedFunctionCache<>(ByteArrayComparable.class, byte[].class, PARSE_FROM);

  private static volatile boolean ALLOW_FAST_REFLECTION_FALLTHROUGH = true;

  // Visible for tests
  public static void setAllowFastReflectionFallthrough(boolean val) {
    ALLOW_FAST_REFLECTION_FALLTHROUGH = val;
  }

  /**
   * Prepend the passed bytes with four bytes of magic, {@link ProtobufMagic#PB_MAGIC}, to flag what
   * follows as a protobuf in hbase. Prepend these bytes to all content written to znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</code> with magic prepended (Creates a new byte array that is
   *         <code>bytes.length</code> plus {@link ProtobufMagic#PB_MAGIC}.length.
   */
  public static byte[] prependPBMagic(final byte[] bytes) {
    return Bytes.add(PB_MAGIC, bytes);
  }

  /** Returns True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix. */
  public static boolean isPBMagicPrefix(final byte[] bytes) {
    return ProtobufMagic.isPBMagicPrefix(bytes);
  }

  /** Returns True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix. */
  public static boolean isPBMagicPrefix(final byte[] bytes, int offset, int len) {
    return ProtobufMagic.isPBMagicPrefix(bytes, offset, len);
  }

  /**
   * Expect the {@link ProtobufMagic#PB_MAGIC} or throw an exception.
   * @param bytes bytes to check
   * @throws DeserializationException if we are missing the pb magic prefix
   */
  public static void expectPBMagicPrefix(final byte[] bytes) throws DeserializationException {
    if (!isPBMagicPrefix(bytes)) {
      String bytesPrefix = bytes == null ? "null" : Bytes.toStringBinary(bytes, 0, PB_MAGIC.length);
      throw new DeserializationException(
        "Missing pb magic " + Bytes.toString(PB_MAGIC) + " prefix" + ", bytes: " + bytesPrefix);
    }
  }

  /** Returns Length of {@link ProtobufMagic#lengthOfPBMagic()} */
  public static int lengthOfPBMagic() {
    return ProtobufMagic.lengthOfPBMagic();
  }

  public static org.apache.hadoop.hbase.shaded.grpc.generated.ComparatorProtos.ByteArrayComparable toByteArrayComparable(final byte[] value) {
    org.apache.hadoop.hbase.shaded.grpc.generated.ComparatorProtos.ByteArrayComparable.Builder builder =
      org.apache.hadoop.hbase.shaded.grpc.generated.ComparatorProtos.ByteArrayComparable.newBuilder();
    if (value != null) builder.setValue(com.google.protobuf.UnsafeByteOperations.unsafeWrap(value));
    return builder.build();
  }

  /**
   * Return the IOException thrown by the remote server wrapped in ServiceException as cause.
   * @param se ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or a new IOException that wraps the unexpected
   *         ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    return makeIOExceptionOfException(se);
  }

  /**
   * Like {@link #getRemoteException(ServiceException)} but more generic, able to handle more than
   * just {@link ServiceException}. Prefer this method to
   * {@link #getRemoteException(ServiceException)} because trying to contain direct protobuf
   * references.
   */
  public static IOException handleRemoteException(Throwable e) {
    return makeIOExceptionOfException(e);
  }

  private static IOException makeIOExceptionOfException(Throwable e) {
    Throwable t = e;
    if (e instanceof ServiceException) {
      t = e.getCause();
    }
    if (ExceptionUtil.isInterrupt(t)) {
      return ExceptionUtil.asInterrupt(t);
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    return t instanceof IOException ? (IOException) t : new HBaseIOException(t);
  }


  /**
   * Convert a protocol buffer Get to a client Get
   * @param proto the protocol buffer Get to convert
   * @return the converted client Get
   */
  public static Get toGet(final org.apache.hadoop.hbase.shaded.grpc.generated.Get proto) throws IOException {
    if (proto == null) return null;
    byte[] row = proto.getRow().toByteArray();
    Get get = new Get(row);
    get.setCacheBlocks(proto.getCacheBlocks());
    get.readVersions(proto.getMaxVersions());
    get.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    get.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    if (proto.getCfTimeRangeCount() > 0) {
      for (org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.ColumnFamilyTimeRange cftr : proto.getCfTimeRangeList()) {
        TimeRange timeRange = toTimeRange(cftr.getTimeRange());
        get.setColumnFamilyTimeRange(cftr.getColumnFamily().toByteArray(), timeRange.getMin(),
          timeRange.getMax());
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = toTimeRange(proto.getTimeRange());
      get.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }
    if (proto.hasFilter()) {
      org.apache.hadoop.hbase.shaded.grpc.generated.FilterProtos.Filter filter = proto.getFilter();
      get.setFilter(GrpcUtil.toFilter(filter));
    }
    for (org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.NameBytesPair attribute : proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      for (org.apache.hadoop.hbase.shaded.grpc.generated.Column column : proto.getColumnList()) {
        byte[] family = column.getFamily().toByteArray();
        if (column.getQualifierCount() > 0) {
          for (com.google.protobuf.ByteString qualifier : column.getQualifierList()) {
            get.addColumn(family, qualifier.toByteArray());
          }
        } else {
          get.addFamily(family);
        }
      }
    }
    if (proto.getExistenceOnly()) {
      get.setCheckExistenceOnly(true);
    }
      get.setConsistency(toConsistency(proto.getConsistency()));
      get.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
      get.setQueryMetricsEnabled(proto.getQueryMetricsEnabled());
    return get;
  }

  public static Consistency toConsistency(org.apache.hadoop.hbase.shaded.grpc.generated.Consistency consistency) {
    switch (consistency) {
      case STRONG:
        return Consistency.STRONG;
      case TIMELINE:
        return Consistency.TIMELINE;
      default:
        return Consistency.STRONG;
    }
  }

  public static org.apache.hadoop.hbase.shaded.grpc.generated.Consistency toConsistency(Consistency consistency) {
    switch (consistency) {
      case STRONG:
        return org.apache.hadoop.hbase.shaded.grpc.generated.Consistency.STRONG;
      case TIMELINE:
        return org.apache.hadoop.hbase.shaded.grpc.generated.Consistency.TIMELINE;
      default:
        return org.apache.hadoop.hbase.shaded.grpc.generated.Consistency.STRONG;
    }
  }

  @FunctionalInterface
  private interface ConsumerWithException<T, U> {
    void accept(T t, U u) throws IOException;
  }

  private static long cellTimestampOrLatest(QualifierValue cell) {
    if (cell.hasTimestamp()) {
      return cell.getTimestamp();
    } else {
      return HConstants.LATEST_TIMESTAMP;
    }
  }

  /**
   * Create a protocol buffer Get based on a client Get.
   * @param get the client Get
   * @return a protocol buffer Get
   */
  public static  org.apache.hadoop.hbase.shaded.grpc.generated.Get toGet(final Get get) throws IOException {
    org.apache.hadoop.hbase.shaded.grpc.generated.Get.Builder builder =  org.apache.hadoop.hbase.shaded.grpc.generated.Get.newBuilder();
    builder.setRow(UnsafeByteOperations.unsafeWrap(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getFilter() != null) {
      builder.setFilter(GrpcUtil.toFilter(get.getFilter()));
    }
    get.getColumnFamilyTimeRange().forEach((cf, timeRange) -> {
      builder.addCfTimeRange(org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.ColumnFamilyTimeRange.newBuilder()
        .setColumnFamily(UnsafeByteOperations.unsafeWrap(cf)).setTimeRange(toTimeRange(timeRange))
        .build());
    });
    builder.setTimeRange(GrpcUtil.toTimeRange(get.getTimeRange()));
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.NameBytesPair.Builder attributeBuilder = org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(UnsafeByteOperations.unsafeWrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      org.apache.hadoop.hbase.shaded.grpc.generated.Column.Builder columnBuilder = org.apache.hadoop.hbase.shaded.grpc.generated.Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family : families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(UnsafeByteOperations.unsafeWrap(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier : qualifiers) {
            columnBuilder.addQualifier(UnsafeByteOperations.unsafeWrap(qualifier));
          }
        }
        builder.addColumn(columnBuilder.build());
      }
    }
    if (get.getMaxResultsPerColumnFamily() >= 0) {
      builder.setStoreLimit(get.getMaxResultsPerColumnFamily());
    }
    if (get.getRowOffsetPerColumnFamily() > 0) {
      builder.setStoreOffset(get.getRowOffsetPerColumnFamily());
    }
    if (get.isCheckExistenceOnly()) {
      builder.setExistenceOnly(true);
    }
    if (get.getConsistency() != null && get.getConsistency() != Consistency.STRONG) {
      builder.setConsistency(toConsistency(get.getConsistency()));
    }

    Boolean loadColumnFamiliesOnDemand = get.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      builder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
    }
    builder.setQueryMetricsEnabled(get.isQueryMetricsEnabled());
    return builder.build();
  }


  /**
   * Convert a client Result to a protocol buffer Result
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static org.apache.hadoop.hbase.shaded.grpc.generated.Result toResult(final Result result) {
    return toResult(result, false);
  }

  /**
   * Convert a client Result to a protocol buffer Result
   * @param result     the client Result to convert
   * @param encodeTags whether to includeTags in converted protobuf result or not When @encodeTags
   *                   is set to true, it will return all the tags in the response. These tags may
   *                   contain some sensitive data like acl permissions, etc. Only the tools like
   *                   Export, Import which needs to take backup needs to set it to true so that
   *                   cell tags are persisted in backup. Refer to HBASE-25246 for more context.
   * @return the converted protocol buffer Result
   */
  public static org.apache.hadoop.hbase.shaded.grpc.generated.Result toResult(final Result result, boolean encodeTags) {
    if (result.getExists() != null) {
      return toResult(result.getExists(), result.isStale());
    }

    ExtendedCell[] cells = ClientInternalHelper.getExtendedRawCells(result);
    if (cells == null || cells.length == 0) {
      return result.isStale() ? EMPTY_RESULT_PB_STALE : EMPTY_RESULT_PB;
    }

    org.apache.hadoop.hbase.shaded.grpc.generated.Result.Builder builder = org.apache.hadoop.hbase.shaded.grpc.generated.Result.newBuilder();
    for (ExtendedCell c : cells) {
      builder.addCell(toCell(c, encodeTags));
    }

    builder.setStale(result.isStale());
    builder.setPartial(result.mayHaveMoreCellsInRow());

    if (result.getMetrics() != null) {
      builder.setMetrics(toQueryMetrics(result.getMetrics()));
    }

    return builder.build();
  }

  /**
   * Convert a client Result to a protocol buffer Result
   * @param existence the client existence to send
   * @return the converted protocol buffer Result
   */
  public static org.apache.hadoop.hbase.shaded.grpc.generated.Result toResult(final boolean existence, boolean stale) {
    if (stale) {
      return existence ? EMPTY_RESULT_PB_EXISTS_TRUE_STALE : EMPTY_RESULT_PB_EXISTS_FALSE_STALE;
    } else {
      return existence ? EMPTY_RESULT_PB_EXISTS_TRUE : EMPTY_RESULT_PB_EXISTS_FALSE;
    }
  }
  /**
   * Convert a client Result to a protocol buffer Result. The pb Result does not include the Cell
   * data. That is for transport otherwise.
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static org.apache.hadoop.hbase.shaded.grpc.generated.Result toResultNoData(final Result result) {
    if (result.getExists() != null) return toResult(result.getExists(), result.isStale());
    int size = result.size();
    if (size == 0) return result.isStale() ? EMPTY_RESULT_PB_STALE : EMPTY_RESULT_PB;
    org.apache.hadoop.hbase.shaded.grpc.generated.Result.Builder builder = org.apache.hadoop.hbase.shaded.grpc.generated.Result.newBuilder();
    builder.setAssociatedCellCount(size);
    builder.setStale(result.isStale());
    if (result.getMetrics() != null) {
      builder.setMetrics(toQueryMetrics(result.getMetrics()));
    }
    return builder.build();
  }


  /**
   * Convert a protocol buffer Result to a client Result
   * @param proto   the protocol buffer Result to convert
   * @param scanner Optional cell scanner.
   * @return the converted client Result
   */
  public static Result toResult(final org.apache.hadoop.hbase.shaded.grpc.generated.Result proto, final CellScanner scanner)
    throws IOException {
    List<org.apache.hadoop.hbase.shaded.grpc.generated.Cell> values = proto.getCellList();
    if (proto.getExists()) {
      if (
        (values != null && !values.isEmpty())
          || proto.getAssociatedCellCount() > 0) {
        throw new IllegalArgumentException("bad proto: exists with cells is no allowed " + proto);
      }
      if (proto.getStale()) {
        return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE_STALE : EMPTY_RESULT_EXISTS_FALSE_STALE;
      }
      return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE : EMPTY_RESULT_EXISTS_FALSE;
    }

    // TODO: Unit test that has some Cells in scanner and some in the proto.
    List<Cell> cells = null;
      int count = proto.getAssociatedCellCount();
      if(count > 0) {
        cells = new ArrayList<>(count + values.size());
        for (int i = 0; i < count; i++) {
          if (!scanner.advance()) throw new IOException("Failed get " + i + " of " + count);
          cells.add(scanner.current());
        }
      }
    if (!values.isEmpty()) {
      if (cells == null) cells = new ArrayList<>(values.size());
      ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
      for (org.apache.hadoop.hbase.shaded.grpc.generated.Cell c : values) {
        cells.add(toCell(builder, c, false));
      }
    }

    Result r = (cells == null || cells.isEmpty())
      ? (proto.getStale() ? EMPTY_RESULT_STALE : EMPTY_RESULT)
      : Result.create(cells, null, proto.getStale());

    if (proto.hasMetrics()) {
      r.setMetrics(toQueryMetrics(proto.getMetrics()));
    }

    return r;
  }

  /**
   * Convert a ByteArrayComparable to a protocol buffer Comparator
   * @param comparator the ByteArrayComparable to convert
   * @return the converted protocol buffer Comparator
   */
  

  /**
   * Convert a protocol buffer Comparator to a ByteArrayComparable
   * @param proto the protocol buffer Comparator to convert
   * @return the converted ByteArrayComparable
   */
  
//
  /**
   * Convert a protocol buffer Filter to a client Filter
   * @param proto the protocol buffer Filter to convert
   * @return the converted Filter
   */
  @SuppressWarnings("unchecked")
  public static Filter toFilter(org.apache.hadoop.hbase.shaded.grpc.generated.FilterProtos.Filter proto) throws IOException {
    String type = proto.getName();
    final byte[] value = proto.getSerializedFilter().toByteArray();

    try {
      Filter result = FILTERS.getAndCallByName(type, value);
      if (result != null) {
        return result;
      }

      if (!ALLOW_FAST_REFLECTION_FALLTHROUGH) {
        throw new IllegalStateException("Failed to deserialize comparator " + type
          + " because fast reflection returned null and fallthrough is disabled");
      }

      Class<?> c = Class.forName(type, true, ClassLoaderHolder.CLASS_LOADER);
      Method parseFrom = c.getMethod(PARSE_FROM, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + PARSE_FROM + " in type: " + type);
      }
      return (Filter) parseFrom.invoke(c, value);
    } catch (Exception e) {
      // Either we couldn't instantiate the method object, or "parseFrom" failed.
      // In either case, let's not retry.
      throw new DoNotRetryIOException(e);
    }
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static  org.apache.hadoop.hbase.shaded.grpc.generated.FilterProtos.Filter toFilter(Filter filter) throws IOException {
    org.apache.hadoop.hbase.shaded.grpc.generated.FilterProtos.Filter.Builder builder =  org.apache.hadoop.hbase.shaded.grpc.generated.FilterProtos.Filter.newBuilder();
    builder.setName(filter.getClass().getName());
    builder.setSerializedFilter(UnsafeByteOperations.unsafeWrap(filter.toByteArray()));
    return builder.build();
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   * @return protocol buffer DeleteType
   */
  public static DeleteType toDeleteType(KeyValue.Type type) throws IOException {
    switch (type) {
      case Delete:
        return DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamily:
        return DeleteType.DELETE_FAMILY;
      case DeleteFamilyVersion:
        return DeleteType.DELETE_FAMILY_VERSION;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }

  /**
   * Convert a protocol buffer DeleteType to delete KeyValue type.
   * @param type The DeleteType
   * @return The type.
   */
  public static KeyValue.Type fromDeleteType(DeleteType type) throws IOException {
    switch (type) {
      case DELETE_ONE_VERSION:
        return KeyValue.Type.Delete;
      case DELETE_MULTIPLE_VERSIONS:
        return KeyValue.Type.DeleteColumn;
      case DELETE_FAMILY:
        return KeyValue.Type.DeleteFamily;
      case DELETE_FAMILY_VERSION:
        return KeyValue.Type.DeleteFamilyVersion;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }

  /**
   * Convert a stringified protocol buffer exception Parameter to a Java Exception
   * @param parameter the protocol buffer Parameter to convert
   * @return the converted Exception
   * @throws IOException if failed to deserialize the parameter
   */
  @SuppressWarnings("unchecked")
  public static Throwable toException(final NameBytesPair parameter) throws IOException {
    if (parameter == null || !parameter.hasValue()) return null;
    String desc = parameter.getValue().toStringUtf8();
    String type = parameter.getName();
    try {
      Class<? extends Throwable> c =
        (Class<? extends Throwable>) Class.forName(type, true, ClassLoaderHolder.CLASS_LOADER);
      Constructor<? extends Throwable> cn = null;
      try {
        cn = c.getDeclaredConstructor(String.class);
        return cn.newInstance(desc);
      } catch (NoSuchMethodException e) {
        // Could be a raw RemoteException. See HBASE-8987.
        cn = c.getDeclaredConstructor(String.class, String.class);
        return cn.newInstance(type, desc);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  // Start helpers for Client

  @SuppressWarnings("unchecked")
  public static <T extends Service> T newServiceStub(Class<T> service, RpcChannel channel)
    throws Exception {
    return (T) Methods.call(service, null, "newStub", new Class[] { RpcChannel.class },
      new Object[] { channel });
  }

  // Start helpers for Admin


  public static byte[] toDelimitedByteArray(final Message m) throws IOException {
    // Allocate arbitrary big size so we avoid resizing.
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    baos.write(PB_MAGIC);
    m.writeDelimitedTo(baos);
    return baos.toByteArray();
  }

  /**
   * Find the HRegion encoded name based on a region specifier
   * @param regionSpecifier the region specifier
   * @return the corresponding region's encoded name
   * @throws DoNotRetryIOException if the specifier type is unsupported
   */
  public static String getRegionEncodedName(final org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier regionSpecifier)
    throws DoNotRetryIOException {
    ByteString value = regionSpecifier.getValue();
    org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType
      type = regionSpecifier.getType();
    switch (type) {
      case REGION_NAME:
        return org.apache.hadoop.hbase.client.RegionInfo.encodeRegionName(value.toByteArray());
      case ENCODED_REGION_NAME:
        return value.toStringUtf8();
      default:
        throw new DoNotRetryIOException("Unsupported region specifier type: " + type);
    }
  }

  public static ScanMetrics toScanMetrics(final byte[] bytes) {
    MapReduceProtos.ScanMetrics pScanMetrics = null;
    try {
      pScanMetrics = MapReduceProtos.ScanMetrics.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      // Ignored there are just no key values to add.
    }
    ScanMetrics scanMetrics = new ScanMetrics();
    if (pScanMetrics != null) {
      for (HBaseProtos.NameInt64Pair pair : pScanMetrics.getMetricsList()) {
        if (pair.hasName() && pair.hasValue()) {
          scanMetrics.setCounter(pair.getName(), pair.getValue());
        }
      }
    }
    return scanMetrics;
  }

  public static MapReduceProtos.ScanMetrics toScanMetrics(ScanMetrics scanMetrics, boolean reset) {
    MapReduceProtos.ScanMetrics.Builder builder = MapReduceProtos.ScanMetrics.newBuilder();
    Map<String, Long> metrics = scanMetrics.getMetricsMap(reset);
    for (Entry<String, Long> e : metrics.entrySet()) {
      HBaseProtos.NameInt64Pair nameInt64Pair =
        HBaseProtos.NameInt64Pair.newBuilder().setName(e.getKey()).setValue(e.getValue()).build();
      builder.addMetrics(nameInt64Pair);
    }
    return builder.build();
  }

  /**
   * Unwraps an exception from a protobuf service into the underlying (expected) IOException. This
   * method will <strong>always</strong> throw an exception.
   * @param se the {@code ServiceException} instance to convert into an {@code IOException}
   * @throws NullPointerException if {@code se} is {@code null}
   */
  public static void toIOException(ServiceException se) throws IOException {
    Objects.requireNonNull(se, "Service exception cannot be null");

    Throwable cause = se.getCause();
    if (cause != null && cause instanceof IOException) {
      throw (IOException) cause;
    }
    throw new IOException(se);
  }

  public static org.apache.hadoop.hbase.shaded.grpc.generated.Cell toCell(final ExtendedCell kv, boolean encodeTags) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    org.apache.hadoop.hbase.shaded.grpc.generated.Cell.Builder kvbuilder = org.apache.hadoop.hbase.shaded.grpc.generated.Cell.newBuilder();
    if (kv instanceof ByteBufferExtendedCell) {
      kvbuilder.setRow(wrap(((ByteBufferExtendedCell) kv).getRowByteBuffer(),
        ((ByteBufferExtendedCell) kv).getRowPosition(), kv.getRowLength()));
      kvbuilder.setFamily(wrap(((ByteBufferExtendedCell) kv).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) kv).getFamilyPosition(), kv.getFamilyLength()));
      kvbuilder.setQualifier(wrap(((ByteBufferExtendedCell) kv).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) kv).getQualifierPosition(), kv.getQualifierLength()));
      kvbuilder.setCellType(org.apache.hadoop.hbase.shaded.grpc.generated.CellType.forNumber(kv.getTypeByte()));
      kvbuilder.setTimestamp(kv.getTimestamp());
      kvbuilder.setValue(wrap(((ByteBufferExtendedCell) kv).getValueByteBuffer(),
        ((ByteBufferExtendedCell) kv).getValuePosition(), kv.getValueLength()));
      if (encodeTags) {
        kvbuilder.setTags(wrap(((ByteBufferExtendedCell) kv).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) kv).getTagsPosition(), kv.getTagsLength()));
      }
    } else {
      kvbuilder.setRow(
        UnsafeByteOperations.unsafeWrap(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
      kvbuilder.setFamily(UnsafeByteOperations.unsafeWrap(kv.getFamilyArray(), kv.getFamilyOffset(),
        kv.getFamilyLength()));
      kvbuilder.setQualifier(UnsafeByteOperations.unsafeWrap(kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength()));
      kvbuilder.setCellType( org.apache.hadoop.hbase.shaded.grpc.generated.CellType.forNumber(kv.getTypeByte()));
      kvbuilder.setTimestamp(kv.getTimestamp());
      kvbuilder.setValue(UnsafeByteOperations.unsafeWrap(kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      if (encodeTags) {
        kvbuilder.setTags(UnsafeByteOperations.unsafeWrap(kv.getTagsArray(), kv.getTagsOffset(),
          kv.getTagsLength()));
      }
    }
    return kvbuilder.build();
  }

  private static ByteString wrap(ByteBuffer b, int offset, int length) {
    ByteBuffer dup = b.duplicate();
    dup.position(offset);
    dup.limit(offset + length);
    return UnsafeByteOperations.unsafeWrap(dup);
  }

  public static ExtendedCell toCell(ExtendedCellBuilder cellBuilder, final org.apache.hadoop.hbase.shaded.grpc.generated.Cell cell,
    boolean decodeTags) {
    ExtendedCellBuilder builder = cellBuilder.clear().setRow(cell.getRow().toByteArray())
      .setFamily(cell.getFamily().toByteArray()).setQualifier(cell.getQualifier().toByteArray())
      .setTimestamp(cell.getTimestamp()).setType((byte) cell.getCellType().getNumber())
      .setValue(cell.getValue().toByteArray());
    if (decodeTags && cell.getTags() != null && cell.getTags().size() > 0) {
      builder.setTags(cell.getTags().toByteArray());
    }
    return builder.build();
  }

  
//    HBaseProtos.NamespaceDescriptor.Builder b =
//      HBaseProtos.NamespaceDescriptor.newBuilder().setName(ByteString.copyFromUtf8(ns.getName()));
//    for (Map.Entry<String, String> entry : ns.getConfiguration().entrySet()) {
//      b.addConfiguration(
//        HBaseProtos.NameStringPair.newBuilder().setName(entry.getKey()).setValue(entry.getValue()));
//    }
//    return b.build();
//  }

  
//    NamespaceDescriptor.Builder b = NamespaceDescriptor.create(desc.getName().toStringUtf8());
//    for (HBaseProtos.NameStringPair prop : desc.getConfigurationList()) {
//      b.addConfiguration(prop.getName(), prop.getValue());
//    }
//    return b.build();
//  }

  
//    return toCompactionDescriptor(info, null, family, inputPaths, outputPaths, storeDir);
//  }

  
//    // compaction descriptor contains relative paths.
//    // input / output paths are relative to the store dir
//    // store dir is relative to region dir
//    CompactionDescriptor.Builder builder = CompactionDescriptor.newBuilder()
//      .setTableName(UnsafeByteOperations.unsafeWrap(info.getTable().toBytes()))
//      .setEncodedRegionName(UnsafeByteOperations
//        .unsafeWrap(regionName == null ? info.getEncodedNameAsBytes() : regionName))
//      .setFamilyName(UnsafeByteOperations.unsafeWrap(family)).setStoreHomeDir(storeDir.getName()); // make
//                                                                                                   // relative
//    for (Path inputPath : inputPaths) {
//      builder.addCompactionInput(inputPath.getName()); // relative path
//    }
//    for (Path outputPath : outputPaths) {
//      builder.addCompactionOutput(outputPath.getName());
//    }
//    builder.setRegionName(UnsafeByteOperations.unsafeWrap(info.getRegionName()));
//    return builder.build();
//  }

  
//    FlushDescriptor.Builder desc = FlushDescriptor.newBuilder().setAction(action)
//      .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(hri.getEncodedNameAsBytes()))
//      .setRegionName(UnsafeByteOperations.unsafeWrap(hri.getRegionName()))
//      .setFlushSequenceNumber(flushSeqId)
//      .setTableName(UnsafeByteOperations.unsafeWrap(hri.getTable().getName()));
//
//    for (Map.Entry<byte[], List<Path>> entry : committedFiles.entrySet()) {
//      WALProtos.FlushDescriptor.StoreFlushDescriptor.Builder builder =
//        WALProtos.FlushDescriptor.StoreFlushDescriptor.newBuilder()
//          .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
//          .setStoreHomeDir(Bytes.toString(entry.getKey())); // relative to region
//      if (entry.getValue() != null) {
//        for (Path path : entry.getValue()) {
//          builder.addFlushOutput(path.getName());
//        }
//      }
//      desc.addStoreFlushes(builder);
//    }
//    return desc.build();
//  }

  
//    final byte[] tableNameAsBytes = hri.getTable().getName();
//    final byte[] encodedNameAsBytes = hri.getEncodedNameAsBytes();
//    final byte[] regionNameAsBytes = hri.getRegionName();
//    return toRegionEventDescriptor(eventType, tableNameAsBytes, encodedNameAsBytes,
//      regionNameAsBytes, seqId,
//
//      server, storeFiles);
//  }

  
//    ServerName server, Map<byte[], List<Path>> storeFiles) {
//    RegionEventDescriptor.Builder desc = RegionEventDescriptor.newBuilder().setEventType(eventType)
//      .setTableName(UnsafeByteOperations.unsafeWrap(tableNameAsBytes))
//      .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(encodedNameAsBytes))
//      .setRegionName(UnsafeByteOperations.unsafeWrap(regionNameAsBytes)).setLogSequenceNumber(seqId)
//      .setServer(toServerName(server));
//
//    for (Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
//      StoreDescriptor.Builder builder =
//        StoreDescriptor.newBuilder().setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
//          .setStoreHomeDir(Bytes.toString(entry.getKey()));
//      for (Path path : entry.getValue()) {
//        builder.addStoreFile(path.getName());
//      }
//
//      desc.addStores(builder);
//    }
//    return desc.build();
//  }

  /**
   * Return short version of Message toString'd, shorter than TextFormat#shortDebugString. Tries to
   * NOT print out data both because it can be big but also so we do not have data in our logs. Use
   * judiciously.
   * @return toString of passed <code>m</code>
   */
  
//    if (m == null) {
//      return "null";
//    }
//    if (m instanceof ScanRequest) {
//      // This should be small and safe to output. No data.
//      return TextFormat.shortDebugString(m);
//    } else if (m instanceof RegionServerReportRequest) {
//      // Print a short message only, just the servername and the requests, not the full load.
//      RegionServerReportRequest r = (RegionServerReportRequest) m;
//      return "server " + TextFormat.shortDebugString(r.getServer()) + " load { numberOfRequests: "
//        + r.getLoad().getNumberOfRequests() + " }";
//    } else if (m instanceof RegionServerStartupRequest) {
//      // Should be small enough.
//      return TextFormat.shortDebugString(m);
//    } else if (m instanceof MutationProto) {
//      return toShortString((MutationProto) m);
//    } else if (m instanceof GetRequest) {
//      GetRequest r = (GetRequest) m;
//      return "region= " + getStringForByteString(r.getRegion().getValue()) + ", row="
//        + getStringForByteString(r.getGet().getRow());
//    } else if (m instanceof ClientProtos.MultiRequest) {
//      ClientProtos.MultiRequest r = (ClientProtos.MultiRequest) m;
//
//      // Get the number of Actions
//      int actionsCount =
//        r.getRegionActionList().stream().mapToInt(ClientProtos.RegionAction::getActionCount).sum();
//
//      // Get first set of Actions.
//      ClientProtos.RegionAction actions = r.getRegionActionList().get(0);
//      String row = actions.getActionCount() <= 0
//        ? ""
//        : getStringForByteString(actions.getAction(0).hasGet()
//          ? actions.getAction(0).getGet().getRow()
//          : actions.getAction(0).getMutation().getRow());
//      return "region= " + getStringForByteString(actions.getRegion().getValue()) + ", for "
//        + actionsCount + " action(s) and 1st row key=" + row;
//    } else if (m instanceof ClientProtos.MutateRequest) {
//      ClientProtos.MutateRequest r = (ClientProtos.MutateRequest) m;
//      return "region= " + getStringForByteString(r.getRegion().getValue()) + ", row="
//        + getStringForByteString(r.getMutation().getRow());
//    } else if (m instanceof ClientProtos.CoprocessorServiceRequest) {
//      ClientProtos.CoprocessorServiceRequest r = (ClientProtos.CoprocessorServiceRequest) m;
//      return "coprocessorService= " + r.getCall().getServiceName() + ":"
//        + r.getCall().getMethodName();
//    } else if (m instanceof MasterProtos.MoveRegionRequest) {
//      return TextFormat.shortDebugString(m);
//    }
//    return "TODO: " + m.getClass().toString();
//  }

  private static String getStringForByteString(ByteString bs) {
    return Bytes.toStringBinary(bs.toByteArray());
  }

  /**
   * Return SlowLogParams to maintain recent online slowlog responses
   * @param message Message object {@link Message}
   * @return SlowLogParams with regionName(for filter queries) and params
   */
  
//    if (message == null) {
//      return null;
//    }
//    if (message instanceof ScanRequest) {
//      ScanRequest scanRequest = (ScanRequest) message;
//      String regionName = getStringForByteString(scanRequest.getRegion().getValue());
//      String params = TextFormat.shortDebugString(message);
//      if (slowLogScanPayloadEnabled) {
//        return new SlowLogParams(regionName, params, scanRequest.getScan());
//      } else {
//        return new SlowLogParams(regionName, params);
//      }
//    } else if (message instanceof MutationProto) {
//      MutationProto mutationProto = (MutationProto) message;
//      String params = "type= " + mutationProto.getMutateType().toString() + ", row= "
//        + getStringForByteString(mutationProto.getRow());
//      return new SlowLogParams(params);
//    } else if (message instanceof GetRequest) {
//      GetRequest getRequest = (GetRequest) message;
//      String regionName = getStringForByteString(getRequest.getRegion().getValue());
//      String params =
//        "region= " + regionName + ", row= " + getStringForByteString(getRequest.getGet().getRow());
//      return new SlowLogParams(regionName, params);
//    } else if (message instanceof MultiRequest) {
//      MultiRequest multiRequest = (MultiRequest) message;
//      int actionsCount = multiRequest.getRegionActionList().stream()
//        .mapToInt(ClientProtos.RegionAction::getActionCount).sum();
//      RegionAction actions = multiRequest.getRegionActionList().get(0);
//      String regionName = getStringForByteString(actions.getRegion().getValue());
//      String params = "region= " + regionName + ", for " + actionsCount + " action(s)";
//      return new SlowLogParams(regionName, params);
//    } else if (message instanceof MutateRequest) {
//      MutateRequest mutateRequest = (MutateRequest) message;
//      String regionName = getStringForByteString(mutateRequest.getRegion().getValue());
//      String params = "region= " + regionName + ", row= "
//        + getStringForByteString(mutateRequest.getMutation().getRow());
//      return new SlowLogParams(regionName, params);
//    } else if (message instanceof CoprocessorServiceRequest) {
//      CoprocessorServiceRequest coprocessorServiceRequest = (CoprocessorServiceRequest) message;
//      String params = "coprocessorService= " + coprocessorServiceRequest.getCall().getServiceName()
//        + ":" + coprocessorServiceRequest.getCall().getMethodName();
//      return new SlowLogParams(params);
//    }
//    String params = message.getClass().toString();
//    return new SlowLogParams(params);
//  }

  /**
   * Convert a list of NameBytesPair to a more readable CSV
   */
  public static String convertAttributesToCsv(List<NameBytesPair> attributes) {
    if (attributes.isEmpty()) {
      return HConstants.EMPTY_STRING;
    }
    return deserializeAttributes(convertNameBytesPairsToMap(attributes)).entrySet().stream()
      .map(entry -> entry.getKey() + " = " + entry.getValue()).collect(Collectors.joining(", "));
  }

  /**
   * Convert a map of byte array attributes to a more readable map of binary string representations
   */
  public static Map<String, String> deserializeAttributes(Map<String, byte[]> attributes) {
    return attributes.entrySet().stream().collect(
      Collectors.toMap(Map.Entry::getKey, entry -> Bytes.toStringBinary(entry.getValue())));
  }

  /**
   * Print out some subset of a MutationProto rather than all of it and its data
   * @param proto Protobuf to print out
   * @return Short String of mutation proto
   */
  static String toShortString(final MutationProto proto) {
    return "row=" + Bytes.toString(proto.getRow().toByteArray()) + ", type="
      + proto.getMutateType().toString();
  }

  public static TableName toTableName(HBaseProtos.TableName tableNamePB) {
    return TableName.valueOf(tableNamePB.getNamespace().asReadOnlyByteBuffer(),
      tableNamePB.getQualifier().asReadOnlyByteBuffer());
  }

  /**
   * Converts a protocol buffer SpaceViolationPolicy to a client SpaceViolationPolicy.
   * @param proto The protocol buffer space violation policy.
   * @return The corresponding client SpaceViolationPolicy.
   */
  public static SpaceViolationPolicy
    toViolationPolicy(final QuotaProtos.SpaceViolationPolicy proto) {
    switch (proto) {
      case DISABLE:
        return SpaceViolationPolicy.DISABLE;
      case NO_WRITES_COMPACTIONS:
        return SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
      case NO_WRITES:
        return SpaceViolationPolicy.NO_WRITES;
      case NO_INSERTS:
        return SpaceViolationPolicy.NO_INSERTS;
    }
    throw new RuntimeException("Invalid SpaceViolationPolicy " + proto);
  }

  /**
   * Converts a client SpaceViolationPolicy to a protocol buffer SpaceViolationPolicy.
   * @param policy The client SpaceViolationPolicy object.
   * @return The corresponding protocol buffer SpaceViolationPolicy.
   */
  public static QuotaProtos.SpaceViolationPolicy
    toProtoViolationPolicy(final SpaceViolationPolicy policy) {
    switch (policy) {
      case DISABLE:
        return QuotaProtos.SpaceViolationPolicy.DISABLE;
      case NO_WRITES_COMPACTIONS:
        return QuotaProtos.SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
      case NO_WRITES:
        return QuotaProtos.SpaceViolationPolicy.NO_WRITES;
      case NO_INSERTS:
        return QuotaProtos.SpaceViolationPolicy.NO_INSERTS;
    }
    throw new RuntimeException("Invalid SpaceViolationPolicy " + policy);
  }

  /**
   * Build a protocol buffer TimedQuota
   * @param limit    the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope    the quota scope
   * @return the protocol buffer TimedQuota
   */
  
//      .setTimeUnit(toProtoTimeUnit(timeUnit)).setScope(toProtoQuotaScope(scope)).build();
//  }

  /**
   * Builds a protocol buffer SpaceQuota.
   * @param limit           The maximum space usage for the quota in bytes.
   * @param violationPolicy The policy to apply when the quota is violated.
   * @return The protocol buffer SpaceQuota.
   */
  public static QuotaProtos.SpaceQuota toProtoSpaceQuota(final long limit,
    final SpaceViolationPolicy violationPolicy) {
    return QuotaProtos.SpaceQuota.newBuilder().setSoftLimit(limit)
      .setViolationPolicy(toProtoViolationPolicy(violationPolicy)).build();
  }

  /**
   * This version of protobuf's mergeDelimitedFrom avoid the hard-coded 64MB limit for decoding
   * buffers
   * @param builder current message builder
   * @param in      Inputsream with delimited protobuf data
   */
  public static void mergeDelimitedFrom(Message.Builder builder, InputStream in)
    throws IOException {
    // This used to be builder.mergeDelimitedFrom(in);
    // but is replaced to allow us to bump the protobuf size limit.
    final int firstByte = in.read();
    if (firstByte != -1) {
      final int size = CodedInputStream.readRawVarint32(firstByte, in);
      final InputStream limitedInput = ByteStreams.limit(in, size);
      final CodedInputStream codedInput = CodedInputStream.newInstance(limitedInput);
      codedInput.setSizeLimit(size);
      builder.mergeFrom(codedInput);
      codedInput.checkLastTagWas(0);
    }
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding buffers
   * where the message size is known
   * @param builder current message builder
   * @param in      InputStream containing protobuf data
   * @param size    known size of protobuf data
   */
  public static void mergeFrom(Message.Builder builder, InputStream in, int size)
    throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(size);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding buffers
   * where the message size is not known
   * @param builder current message builder
   * @param in      InputStream containing protobuf data
   */
//  public static void mergeFrom(Message.Builder builder, InputStream in) throws IOException {
//    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
//    codedInput.setSizeLimit(Integer.MAX_VALUE);
//    builder.mergeFrom(codedInput);
//    codedInput.checkLastTagWas(0);
//  }


  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding buffers when
   * working with byte arrays
   * @param builder current message builder
   * @param b       byte array
   */
  public static void mergeFrom(Message.Builder builder, byte[] b) throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b);
    codedInput.setSizeLimit(b.length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }


  /**
   * Get a protocol buffer VersionInfo
   * @return the converted protocol buffer VersionInfo
   */
  public static HBaseProtos.VersionInfo getVersionInfo() {
    HBaseProtos.VersionInfo.Builder builder = HBaseProtos.VersionInfo.newBuilder();
    String version = VersionInfo.getVersion();
    builder.setVersion(version);
    String[] components = version.split("\\.");
    if (components != null && components.length > 2) {
      builder.setVersionMajor(Integer.parseInt(components[0]));
      builder.setVersionMinor(Integer.parseInt(components[1]));
    }
    builder.setUrl(VersionInfo.getUrl());
    builder.setRevision(VersionInfo.getRevision());
    builder.setUser(VersionInfo.getUser());
    builder.setDate(VersionInfo.getDate());
    builder.setSrcChecksum(VersionInfo.getSrcChecksum());
    return builder.build();
  }

  /**
   * Convert SecurityCapabilitiesResponse.Capability to SecurityCapability
   * @param capabilities capabilities returned in the SecurityCapabilitiesResponse message
   * @return the converted list of SecurityCapability elements
   */
  public static List<SecurityCapability> toSecurityCapabilityList(
    List<MasterProtos.SecurityCapabilitiesResponse.Capability> capabilities) {
    List<SecurityCapability> scList = new ArrayList<>(capabilities.size());
    for (MasterProtos.SecurityCapabilitiesResponse.Capability c : capabilities) {
      try {
        scList.add(SecurityCapability.valueOf(c.getNumber()));
      } catch (IllegalArgumentException e) {
        // Unknown capability, just ignore it. We don't understand the new capability
        // but don't care since by definition we cannot take advantage of it.
      }
    }
    return scList;
  }

  public static TimeRange toTimeRange(org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.TimeRange timeRange) {
    if (timeRange == null) {
      return TimeRange.allTime();
    }
    if (timeRange.getFrom() != -1) {
      if (timeRange.getTo() != -1) {
        return TimeRange.between(timeRange.getFrom(), timeRange.getTo());
      } else {
        return TimeRange.from(timeRange.getFrom());
      }
    } else {
      return TimeRange.until(timeRange.getTo());
    }
  }

  /**
   * Converts a ColumnFamilySchema to ColumnFamilyDescriptor
   * @param cfs the ColumnFamilySchema
   * @return An {@link ColumnFamilyDescriptor} made from the passed in <code>cfs</code>
   */
  public static ColumnFamilyDescriptor toColumnFamilyDescriptor(final ColumnFamilySchema cfs) {
    // Use the empty constructor so we preserve the initial values set on construction for things
    // like maxVersion. Otherwise, we pick up wrong values on deserialization which makes for
    // unrelated-looking test failures that are hard to trace back to here.
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(cfs.getName().toByteArray());
    cfs.getAttributesList()
      .forEach(a -> builder.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray()));
    cfs.getConfigurationList().forEach(a -> builder.setConfiguration(a.getName(), a.getValue()));
    return builder.build();
  }


  /**
   * Converts a TableSchema to TableDescriptor
   * @param ts A pb TableSchema instance.
   * @return An {@link TableDescriptor} made from the passed in pb <code>ts</code>.
   */
  public static TableDescriptor toTableDescriptor(final TableSchema ts) {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(GrpcUtil.toTableName(ts.getTableName()));
    ts.getColumnFamiliesList().stream().map(GrpcUtil::toColumnFamilyDescriptor)
      .forEach(builder::setColumnFamily);
    ts.getAttributesList()
      .forEach(a -> builder.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray()));
    ts.getConfigurationList().forEach(a -> builder.setValue(a.getName(), a.getValue()));
    return builder.build();
  }

  /**
   * Creates {@link CompactionState} from {@link GetRegionInfoResponse.CompactionState} state
   * @param state the protobuf CompactionState
   */
  public static CompactionState createCompactionState(GetRegionInfoResponse.CompactionState state) {
    return CompactionState.valueOf(state.toString());
  }

  public static GetRegionInfoResponse.CompactionState createCompactionState(CompactionState state) {
    return GetRegionInfoResponse.CompactionState.valueOf(state.toString());
  }

  /**
   * Creates {@link CompactionState} from {@link RegionLoad.CompactionState} state
   * @param state the protobuf CompactionState
   */
  public static CompactionState
    createCompactionStateForRegionLoad(RegionLoad.CompactionState state) {
    return CompactionState.valueOf(state.toString());
  }

  public static RegionLoad.CompactionState
    createCompactionStateForRegionLoad(CompactionState state) {
    return RegionLoad.CompactionState.valueOf(state.toString());
  }

  public static Optional<Long> toOptionalTimestamp(MajorCompactionTimestampResponse resp) {
    long timestamp = resp.getCompactionTimestamp();
    return timestamp == 0 ? Optional.empty() : Optional.of(timestamp);
  }

  /**
   * Creates {@link SnapshotProtos.SnapshotDescription.Type} from {@link SnapshotType}
   * @param type the SnapshotDescription type
   * @return the protobuf SnapshotDescription type
   */
  public static SnapshotProtos.SnapshotDescription.Type
    createProtosSnapShotDescType(SnapshotType type) {
    return SnapshotProtos.SnapshotDescription.Type.valueOf(type.name());
  }

  /**
   * Creates {@link SnapshotProtos.SnapshotDescription.Type} from the type of SnapshotDescription
   * string
   * @param snapshotDesc string representing the snapshot description type
   * @return the protobuf SnapshotDescription type
   */
  public static SnapshotProtos.SnapshotDescription.Type
    createProtosSnapShotDescType(String snapshotDesc) {
    return SnapshotProtos.SnapshotDescription.Type.valueOf(snapshotDesc.toUpperCase(Locale.ROOT));
  }

  /**
   * Creates {@link SnapshotType} from the {@link SnapshotProtos.SnapshotDescription.Type}
   * @param type the snapshot description type
   * @return the protobuf SnapshotDescription type
   */
  public static SnapshotType createSnapshotType(SnapshotProtos.SnapshotDescription.Type type) {
    return SnapshotType.valueOf(type.toString());
  }

  /**
   * Convert from {@link SnapshotDescription} to {@link SnapshotProtos.SnapshotDescription}
   * @param snapshotDesc the POJO SnapshotDescription
   * @return the protobuf SnapshotDescription
   */
  public static SnapshotProtos.SnapshotDescription
    createHBaseProtosSnapshotDesc(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription.Builder builder =
      SnapshotProtos.SnapshotDescription.newBuilder();
    if (snapshotDesc.getTableName() != null) {
      builder.setTable(snapshotDesc.getTableNameAsString());
    }
    if (snapshotDesc.getName() != null) {
      builder.setName(snapshotDesc.getName());
    }
    if (snapshotDesc.getOwner() != null) {
      builder.setOwner(snapshotDesc.getOwner());
    }
    if (snapshotDesc.getCreationTime() != -1L) {
      builder.setCreationTime(snapshotDesc.getCreationTime());
    }
    if (
      snapshotDesc.getTtl() != -1L
        && snapshotDesc.getTtl() < TimeUnit.MILLISECONDS.toSeconds(Long.MAX_VALUE)
    ) {
      builder.setTtl(snapshotDesc.getTtl());
    }
    if (snapshotDesc.getVersion() != -1) {
      builder.setVersion(snapshotDesc.getVersion());
    }
    if (snapshotDesc.getMaxFileSize() != -1) {
      builder.setMaxFileSize(snapshotDesc.getMaxFileSize());
    }
    builder.setType(GrpcUtil.createProtosSnapShotDescType(snapshotDesc.getType()));
    return builder.build();
  }

  /**
   * Convert from {@link SnapshotProtos.SnapshotDescription} to {@link SnapshotDescription}
   * @param snapshotDesc the protobuf SnapshotDescription
   * @return the POJO SnapshotDescription
   */
  public static SnapshotDescription
    createSnapshotDesc(SnapshotProtos.SnapshotDescription snapshotDesc) {
    final Map<String, Object> snapshotProps = new HashMap<>();
    snapshotProps.put("TTL", snapshotDesc.getTtl());
    snapshotProps.put(TableDescriptorBuilder.MAX_FILESIZE, snapshotDesc.getMaxFileSize());
    return new SnapshotDescription(snapshotDesc.getName(),
      snapshotDesc.hasTable() ? TableName.valueOf(snapshotDesc.getTable()) : null,
      createSnapshotType(snapshotDesc.getType()), snapshotDesc.getOwner(),
      snapshotDesc.getCreationTime(), snapshotDesc.getVersion(), snapshotProps);
  }

  public static RegionLoadStats createRegionLoadStats(ClientProtos.RegionLoadStats stats) {
    return new RegionLoadStats(stats.getMemStoreLoad(), stats.getHeapOccupancy(),
      stats.getCompactionPressure());
  }

  /** Returns A String version of the passed in <code>msg</code> */
  public static String toText(Message msg) {
    return TextFormat.shortDebugString(msg);
  }

  public static byte[] toBytes(ByteString bs) {
    return bs.toByteArray();
  }

  /**
   * Contain ServiceException inside here. Take a callable that is doing our pb rpc and run it.
   */
  public static <T> T call(Callable<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (Exception e) {
      throw GrpcUtil.handleRemoteException(e);
    }
  }



  public static ProcedureDescription buildProcedureDescription(String signature, String instance,
    Map<String, String> props) {
    ProcedureDescription.Builder builder =
      ProcedureDescription.newBuilder().setSignature(signature).setInstance(instance);
    if (props != null && !props.isEmpty()) {
      props.entrySet().forEach(entry -> builder.addConfiguration(
        NameStringPair.newBuilder().setName(entry.getKey()).setValue(entry.getValue()).build()));
    }
    return builder.build();
  }

  /**
   * Get the Meta region state from the passed data bytes. Can handle both old and new style server
   * names.
   * @param data      protobuf serialized data with meta server name.
   * @param replicaId replica ID for this region
   * @return RegionState instance corresponding to the serialized data.
   * @throws DeserializationException if the data is invalid.
   */
  public static RegionState parseMetaRegionStateFrom(final byte[] data, int replicaId)
    throws DeserializationException {
    RegionState.State state = RegionState.State.OPEN;
    ServerName serverName;
    if (data != null && data.length > 0 && GrpcUtil.isPBMagicPrefix(data)) {
      try {
        int prefixLen = GrpcUtil.lengthOfPBMagic();
        ZooKeeperProtos.MetaRegionServer rl = ZooKeeperProtos.MetaRegionServer.parser()
          .parseFrom(data, prefixLen, data.length - prefixLen);
        if (rl.hasState()) {
          state = RegionState.State.convert(rl.getState());
        }
        HBaseProtos.ServerName sn = rl.getServer();
        serverName = ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException("Unable to parse meta region location");
      }
    } else {
      // old style of meta region location?
      serverName = parseServerNameFrom(data);
    }
    if (serverName == null) {
      state = RegionState.State.OFFLINE;
    }
    return new RegionState(
      RegionReplicaUtil.getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId),
      state, serverName);
  }

  /**
   * Get a ServerName from the passed in data bytes.
   * @param data Data with a serialize server name in it; can handle the old style servername where
   *             servername was host and port. Works too with data that begins w/ the pb 'PBUF'
   *             magic and that is then followed by a protobuf that has a serialized
   *             {@link ServerName} in it.
   * @return Returns null if <code>data</code> is null else converts passed data to a ServerName
   *         instance.
   */
  public static ServerName parseServerNameFrom(final byte[] data) throws DeserializationException {
    if (data == null || data.length <= 0) return null;
    if (ProtobufMagic.isPBMagicPrefix(data)) {
      int prefixLen = ProtobufMagic.lengthOfPBMagic();
      try {
        ZooKeeperProtos.Master rss =
          ZooKeeperProtos.Master.parser().parseFrom(data, prefixLen, data.length - prefixLen);
        org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName sn =
          rss.getMaster();
        return ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (/* InvalidProtocolBufferException */IOException e) {
        // A failed parse of the znode is pretty catastrophic. Rather than loop
        // retrying hoping the bad bytes will changes, and rather than change
        // the signature on this method to add an IOE which will send ripples all
        // over the code base, throw a RuntimeException. This should "never" happen.
        // Fail fast if it does.
        throw new DeserializationException(e);
      }
    }
    // The str returned could be old style -- pre hbase-1502 -- which was
    // hostname and port seperated by a colon rather than hostname, port and
    // startcode delimited by a ','.
    String str = Bytes.toString(data);
    int index = str.indexOf(ServerName.SERVERNAME_SEPARATOR);
    if (index != -1) {
      // Presume its ServerName serialized with versioned bytes.
      return ServerName.parseVersionedServerName(data);
    }
    // Presume it a hostname:port format.
    String hostname = Addressing.parseHostname(str);
    int port = Addressing.parsePort(str);
    return ServerName.valueOf(hostname, port, -1L);
  }

  /**
   * Helper to convert the protobuf Procedure to JSON String
   * @return Convert the current Protocol Buffers Procedure to JSON String
   */
  public static String toProcedureJson(List<ProcedureProtos.Procedure> procProtos) {
    JsonArray procJsons = new JsonArray(procProtos.size());
    for (ProcedureProtos.Procedure procProto : procProtos) {
      try {
        JsonElement procJson = ProtobufMessageConverter.toJsonElement(procProto);
        procJsons.add(procJson);
      } catch (InvalidProtocolBufferException e) {
        procJsons.add(e.toString());
      }
    }
    return procJsons.toString();
  }

  public static String toLockJson(List<LockServiceProtos.LockedResource> lockedResourceProtos) {
    JsonArray lockedResourceJsons = new JsonArray(lockedResourceProtos.size());
    for (LockServiceProtos.LockedResource lockedResourceProto : lockedResourceProtos) {
      try {
        JsonElement lockedResourceJson =
          ProtobufMessageConverter.toJsonElement(lockedResourceProto);
        lockedResourceJsons.add(lockedResourceJson);
      } catch (InvalidProtocolBufferException e) {
        lockedResourceJsons.add(e.toString());
      }
    }
    return lockedResourceJsons.toString();
  }

  public static List<SnapshotDescription>
    toSnapshotDescriptionList(GetCompletedSnapshotsResponse response, Pattern pattern) {
    return response.getSnapshotsList().stream().map(GrpcUtil::createSnapshotDesc)
      .filter(snap -> pattern != null ? pattern.matcher(snap.getName()).matches() : true)
      .collect(Collectors.toList());
  }

  public static CacheEvictionStats toCacheEvictionStats(HBaseProtos.CacheEvictionStats stats)
    throws IOException {
    CacheEvictionStatsBuilder builder = CacheEvictionStats.builder();
    builder.withEvictedBlocks(stats.getEvictedBlocks()).withMaxCacheSize(stats.getMaxCacheSize());
    if (stats.getExceptionCount() > 0) {
      for (HBaseProtos.RegionExceptionMessage exception : stats.getExceptionList()) {
        HBaseProtos.RegionSpecifier rs = exception.getRegion();
        byte[] regionName = rs.getValue().toByteArray();
        builder.addException(regionName, GrpcUtil.toException(exception.getException()));
      }
    }
    return builder.build();
  }


  public static org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.TimeRange toTimeRange(TimeRange timeRange) {
    if (timeRange == null) {
      timeRange = TimeRange.allTime();
    }
    return org.apache.hadoop.hbase.shaded.grpc.generated.HBaseProtos.TimeRange.newBuilder().setFrom(timeRange.getMin()).setTo(timeRange.getMax())
      .build();
  }


  public static ClusterStatusProtos.RegionStatesCount
    toTableRegionStatesCount(RegionStatesCount regionStatesCount) {
    int openRegions = 0;
    int splitRegions = 0;
    int closedRegions = 0;
    int regionsInTransition = 0;
    int totalRegions = 0;
    if (regionStatesCount != null) {
      openRegions = regionStatesCount.getOpenRegions();
      splitRegions = regionStatesCount.getSplitRegions();
      closedRegions = regionStatesCount.getClosedRegions();
      regionsInTransition = regionStatesCount.getRegionsInTransition();
      totalRegions = regionStatesCount.getTotalRegions();
    }
    return ClusterStatusProtos.RegionStatesCount.newBuilder().setOpenRegions(openRegions)
      .setSplitRegions(splitRegions).setClosedRegions(closedRegions)
      .setRegionsInTransition(regionsInTransition).setTotalRegions(totalRegions).build();
  }

  public static RegionStatesCount
    toTableRegionStatesCount(ClusterStatusProtos.RegionStatesCount regionStatesCount) {
    int openRegions = 0;
    int splitRegions = 0;
    int closedRegions = 0;
    int regionsInTransition = 0;
    int totalRegions = 0;
    if (regionStatesCount != null) {
      closedRegions = regionStatesCount.getClosedRegions();
      regionsInTransition = regionStatesCount.getRegionsInTransition();
      openRegions = regionStatesCount.getOpenRegions();
      splitRegions = regionStatesCount.getSplitRegions();
      totalRegions = regionStatesCount.getTotalRegions();
    }
    return new RegionStatesCount.RegionStatesCountBuilder().setOpenRegions(openRegions)
      .setSplitRegions(splitRegions).setClosedRegions(closedRegions)
      .setRegionsInTransition(regionsInTransition).setTotalRegions(totalRegions).build();
  }


  private static Map<String, byte[]>
    convertNameBytesPairsToMap(List<NameBytesPair> nameBytesPairs) {
    return nameBytesPairs.stream().collect(Collectors.toMap(NameBytesPair::getName,
      nameBytesPair -> nameBytesPair.getValue().toByteArray()));
  }


  /**
   * Convert {@link ClearSlowLogResponses} to boolean
   * @param clearSlowLogResponses Clear slowlog response protobuf instance
   * @return boolean representing clear slowlog response
   */
  public static boolean toClearSlowLogPayload(final ClearSlowLogResponses clearSlowLogResponses) {
    return clearSlowLogResponses.getIsCleaned();
  }

  public static void populateBalanceRSGroupResponse(
    RSGroupAdminProtos.BalanceRSGroupResponse.Builder responseBuilder, BalanceResponse response) {
    responseBuilder.setBalanceRan(response.isBalancerRan())
      .setMovesCalculated(response.getMovesCalculated())
      .setMovesExecuted(response.getMovesExecuted());
  }

  public static BalanceResponse
    toBalanceResponse(RSGroupAdminProtos.BalanceRSGroupResponse response) {
    return BalanceResponse.newBuilder().setBalancerRan(response.getBalanceRan())
      .setMovesExecuted(response.hasMovesExecuted() ? response.getMovesExecuted() : 0)
      .setMovesCalculated(response.hasMovesCalculated() ? response.getMovesCalculated() : 0)
      .build();
  }

  public static RSGroupAdminProtos.BalanceRSGroupRequest
    createBalanceRSGroupRequest(String groupName, BalanceRequest request) {
    return RSGroupAdminProtos.BalanceRSGroupRequest.newBuilder().setRSGroupName(groupName)
      .setDryRun(request.isDryRun()).setIgnoreRit(request.isIgnoreRegionsInTransition()).build();
  }

  public static BalanceRequest toBalanceRequest(RSGroupAdminProtos.BalanceRSGroupRequest request) {
    return BalanceRequest.newBuilder().setDryRun(request.hasDryRun() && request.getDryRun())
      .setIgnoreRegionsInTransition(request.hasIgnoreRit() && request.getIgnoreRit()).build();
  }

  public static RSGroupInfo toGroupInfo(RSGroupProtos.RSGroupInfo proto) {
    RSGroupInfo rsGroupInfo = new RSGroupInfo(proto.getName());

    Collection<Address> addresses = proto.getServersList().parallelStream()
      .map(serverName -> Address.fromParts(serverName.getHostName(), serverName.getPort()))
      .collect(Collectors.toList());
    rsGroupInfo.addAllServers(addresses);

    Collection<TableName> tables = proto.getTablesList().parallelStream()
      .map(GrpcUtil::toTableName).collect(Collectors.toList());
    rsGroupInfo.addAllTables(tables);

    proto.getConfigurationList()
      .forEach(pair -> rsGroupInfo.setConfiguration(pair.getName(), pair.getValue()));
    return rsGroupInfo;
  }

  public static org.apache.hadoop.hbase.shaded.grpc.generated.QueryMetrics toQueryMetrics(QueryMetrics metrics) {
    return org.apache.hadoop.hbase.shaded.grpc.generated.QueryMetrics.newBuilder()
      .setBlockBytesScanned(metrics.getBlockBytesScanned()).build();
  }

  public static QueryMetrics toQueryMetrics(org.apache.hadoop.hbase.shaded.grpc.generated.QueryMetrics metrics) {
    return new QueryMetrics(metrics.getBlockBytesScanned());
  }

  /**
   * Check whether this IPBE indicates EOF or not.
   * <p/>
   * We will check the exception message, if it is likely the one of
   * InvalidProtocolBufferException.truncatedMessage, we will consider it as EOF, otherwise not.
   */
  public static boolean isEOF(InvalidProtocolBufferException e) {
    return e.getMessage().contains("input has been truncated");
  }

  /**
   * This is a wrapper of the PB message's parseDelimitedFrom. The difference is, if we can not
   * determine whether there are enough bytes in stream, i.e, the available method does not have a
   * valid return value, we will try to read all the bytes to a byte array first, and then parse the
   * pb message with {@link Parser#parseFrom(byte[])} instead of call
   * {@link Parser#parseDelimitedFrom(InputStream)} directly. This is because even if the bytes are
   * not enough bytes, {@link Parser#parseDelimitedFrom(InputStream)} could still return without any
   * errors but just leave us a partial PB message.
   * @return The PB message if we can parse it successfully, otherwise there will always be an
   *         exception thrown, will never return {@code null}.
   */
  public static <T extends Message> T parseDelimitedFrom(InputStream in, Parser<T> parser)
    throws IOException {
    int firstByte = in.read();
    if (firstByte < 0) {
      throw new EOFException("EOF while reading message size");
    }
    int size = CodedInputStream.readRawVarint32(firstByte, in);
    int available = in.available();
    if (available > 0) {
      if (available < size) {
        throw new EOFException("Available bytes not enough for parsing PB message, expect at least "
          + size + " bytes, but only " + available + " bytes available");
      }
      // this piece of code is copied from GeneratedMessageV3.parseFrom
      try {
        return parser.parseFrom(ByteStreams.limit(in, size));
      } catch (InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      }
    } else {
      // this usually means the stream does not have a proper available implementation, let's read
      // the content to an byte array before parsing.
      byte[] bytes = new byte[size];
      ByteStreams.readFully(in, bytes);
      return parser.parseFrom(bytes);
    }
  }
}
