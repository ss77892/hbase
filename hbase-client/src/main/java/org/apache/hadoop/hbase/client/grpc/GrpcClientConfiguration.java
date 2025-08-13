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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration constants and utilities for gRPC client support.
 */
@InterfaceAudience.Private
public final class GrpcClientConfiguration {

  // Configuration keys for gRPC client
  public static final String GRPC_CLIENT_ENABLED_KEY = "hbase.client.grpc.enabled";
  public static final boolean GRPC_CLIENT_ENABLED_DEFAULT = false;
  
  public static final String GRPC_CLIENT_CONNECTION_TIMEOUT_KEY = "hbase.client.grpc.connection.timeout.ms";
  public static final int GRPC_CLIENT_CONNECTION_TIMEOUT_DEFAULT = 30000; // 30 seconds
  
  public static final String GRPC_CLIENT_REQUEST_TIMEOUT_KEY = "hbase.client.grpc.request.timeout.ms";
  public static final int GRPC_CLIENT_REQUEST_TIMEOUT_DEFAULT = 6000000; // 6000 seconds
  
  public static final String GRPC_CLIENT_RETRY_ENABLED_KEY = "hbase.client.grpc.retry.enabled";
  public static final boolean GRPC_CLIENT_RETRY_ENABLED_DEFAULT = true;
  
  public static final String GRPC_CLIENT_MAX_MESSAGE_SIZE_KEY = "hbase.client.grpc.max.message.size";
  public static final int GRPC_CLIENT_MAX_MESSAGE_SIZE_DEFAULT = 4 * 1024 * 1024; // 4MB

  // Server configuration keys (for region server gRPC port discovery)
  public static final String GRPC_REGIONSERVER_PORT_KEY = "hbase.regionserver.grpc.port";
  public static final int GRPC_REGIONSERVER_PORT_DEFAULT = 16070;
  
  public static final String GRPC_REGIONSERVER_ENABLED_KEY = "hbase.regionserver.grpc.enabled";
  public static final boolean GRPC_REGIONSERVER_ENABLED_DEFAULT = false;

  private GrpcClientConfiguration() {
    // Utility class
  }

  /**
   * Check if gRPC client is enabled in the configuration.
   */
  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(GRPC_CLIENT_ENABLED_KEY, GRPC_CLIENT_ENABLED_DEFAULT);
  }

  /**
   * Check if gRPC is enabled on region servers.
   */
  public static boolean isRegionServerGrpcEnabled(Configuration conf) {
    return conf.getBoolean(GRPC_REGIONSERVER_ENABLED_KEY, GRPC_REGIONSERVER_ENABLED_DEFAULT);
  }

  /**
   * Get the gRPC port for region servers.
   */
  public static int getRegionServerGrpcPort(Configuration conf) {
    return conf.getInt(GRPC_REGIONSERVER_PORT_KEY, GRPC_REGIONSERVER_PORT_DEFAULT);
  }

  /**
   * Get connection timeout for gRPC client.
   */
  public static int getConnectionTimeout(Configuration conf) {
    return conf.getInt(GRPC_CLIENT_CONNECTION_TIMEOUT_KEY, GRPC_CLIENT_CONNECTION_TIMEOUT_DEFAULT);
  }

  /**
   * Get request timeout for gRPC client.
   */
  public static int getRequestTimeout(Configuration conf) {
    return conf.getInt(GRPC_CLIENT_REQUEST_TIMEOUT_KEY, GRPC_CLIENT_REQUEST_TIMEOUT_DEFAULT);
  }

  /**
   * Get maximum message size for gRPC client.
   */
  public static int getMaxMessageSize(Configuration conf) {
    return conf.getInt(GRPC_CLIENT_MAX_MESSAGE_SIZE_KEY, GRPC_CLIENT_MAX_MESSAGE_SIZE_DEFAULT);
  }

  /**
   * Check if retry is enabled for gRPC client.
   */
  public static boolean isRetryEnabled(Configuration conf) {
    return conf.getBoolean(GRPC_CLIENT_RETRY_ENABLED_KEY, GRPC_CLIENT_RETRY_ENABLED_DEFAULT);
  }

  /**
   * Create a configuration with gRPC client enabled.
   * This is a convenience method for testing or demonstration purposes.
   */
  public static Configuration createWithGrpcEnabled(Configuration baseConf) {
    Configuration conf = new Configuration(baseConf);
    conf.setBoolean(GRPC_CLIENT_ENABLED_KEY, true);
    conf.setBoolean(GRPC_REGIONSERVER_ENABLED_KEY, true);
    return conf;
  }
}

