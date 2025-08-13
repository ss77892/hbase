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

import java.util.concurrent.CompletableFuture;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Interface for gRPC client service to avoid dependency issues.
 * This allows the main client code to work with gRPC optionally.
 */
@InterfaceAudience.Private
public interface GrpcClientServiceInterface extends AutoCloseable {

  /**
   * Check if gRPC client is enabled.
   */
  boolean isEnabled();

  /**
   * Perform a Get operation using gRPC.
   */
  CompletableFuture<org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse> get(ServerName serverName, org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest request);
}

