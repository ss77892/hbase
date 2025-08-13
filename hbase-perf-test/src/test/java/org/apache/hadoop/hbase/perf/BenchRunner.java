/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.perf;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public final class BenchRunner {
  private BenchRunner() {}

  public static void main(String[] args) throws RunnerException {
    String include = args.length > 0 && args[0] != null && !args[0].isEmpty() ? args[0] : ".*";
    int forks = parseInt(args, 1, 1);
    int warmup = parseInt(args, 2, 5);
    int measurement = parseInt(args, 3, 5);
    Options opt = new OptionsBuilder()
        .include(include)
        .forks(forks)
        .warmupIterations(warmup)
        .measurementIterations(measurement)
        .build();
    new Runner(opt).run();
  }

  private static int parseInt(String[] args, int idx, int def) {
    try {
      return args.length > idx && args[idx] != null && !args[idx].isEmpty() ? Integer.parseInt(args[idx]) : def;
    } catch (NumberFormatException e) {
      return def;
    }
  }
}


