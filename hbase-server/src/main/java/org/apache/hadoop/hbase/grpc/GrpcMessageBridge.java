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
package org.apache.hadoop.hbase.grpc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility to bridge between two protobuf generated message packages by
 * serializing and parsing bytes. This allows delegating between the existing
 * shaded protobuf messages (package org.apache.hadoop.hbase.shaded.protobuf.generated)
 * and the new gRPC-generated messages (package org.apache.hadoop.hbase.shaded.grpc.generated)
 * without duplicating conversion logic per type.
 */
@InterfaceAudience.Private
public final class GrpcMessageBridge {

  private GrpcMessageBridge() {}

  /**
   * Convert a protobuf message to another generated message class using the
   * target class' static parseFrom(byte[]) method.
   *
   * @param source the source protobuf message (any generated message type)
   * @param targetClass the target generated message class
   * @param <T> type of target message
   * @return parsed instance of {@code targetClass}
   * @throws IllegalArgumentException if the conversion fails
   */
  @SuppressWarnings("unchecked")
  public static <T> T convert(Object source, Class<T> targetClass) {
    if (source == null) return null;
    try {
      Method toByteArray = source.getClass().getMethod("toByteArray");
      byte[] bytes = (byte[]) toByteArray.invoke(source);
      Method parseFrom = targetClass.getMethod("parseFrom", byte[].class);
      return (T) parseFrom.invoke(null, bytes);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(
        "Failed to convert message from " + source.getClass().getName() + " to "
          + targetClass.getName(),
        e);
    }
  }
}


