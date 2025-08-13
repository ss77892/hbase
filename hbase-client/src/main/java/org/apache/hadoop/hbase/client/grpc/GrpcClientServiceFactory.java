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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating gRPC client service instances with fallback support.
 * This handles the case where gRPC dependencies may not be available.
 */
@InterfaceAudience.Private
public class GrpcClientServiceFactory {

  // Intentionally no logger usage for now; keep for potential future diagnostics
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(GrpcClientServiceFactory.class);

  /**
   * Create a gRPC client service.
   */
  public static GrpcClientServiceInterface create(Configuration conf) {
    return new GrpcClientService(conf);
  }

  /**
   * Disabled implementation that always returns unsupported operation exceptions.
   */
  // Retained only for compatibility with older call sites if any; not used by current factory
  @SuppressWarnings("unused")
  private static class DisabledGrpcClientService implements GrpcClientServiceInterface {

    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public CompletableFuture<org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse> get(ServerName serverName, org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest request) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("gRPC client is not available"));
    }

    @Override
    public void close() {
      // Nothing to close
    }
  }
}

