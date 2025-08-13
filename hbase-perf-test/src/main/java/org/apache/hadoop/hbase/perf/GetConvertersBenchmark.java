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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.grpc.GrpcRequestConverter;
import org.apache.hadoop.hbase.shaded.grpc.GrpcUtil;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class GetConvertersBenchmark {

	@State(Scope.Thread)
	public static class Data {
		byte[] regionName;
		Get get;
		Result result100x1k;

		@Setup(Level.Trial)
		public void setup() throws Exception {
			regionName = "tbl,\\x00,00000000000000.01234567".getBytes();
			byte[] row = "row-0001".getBytes();
			get = new Get(row).setCacheBlocks(false).readVersions(1);
			for (int i = 0; i < 5; i++) {
				get.addColumn(("f" + i).getBytes(), ("q" + i).getBytes());
			}
			// Build a client Result with 100 cells, 1k values each
			List<Cell> cells = new ArrayList<>(100);
			ExtendedCellBuilder b = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
			byte[] family = "f".getBytes();
			byte[] qualifierPrefix = "q".getBytes();
			byte[] value = new byte[1024];
			for (int i = 0; i < value.length; i++) value[i] = (byte) (i & 0xFF);
			for (int i = 0; i < 100; i++) {
				byte[] qual = ("q" + i).getBytes();
				cells.add(
					b.clear()
						.setRow(row)
						.setFamily(family)
						.setQualifier(qual)
						.setTimestamp(1L)
						.setType(org.apache.hadoop.hbase.Cell.Type.Put)
						.setValue(value)
						.build()
				);
			}
			result100x1k = Result.create(cells);
		}
	}

	// GetRequest conversions
	@Benchmark
	public Object rpc_convert_only(Data d) throws IOException {
		return RequestConverter.buildGetRequest(d.regionName, d.get);
	}

	@Benchmark
	public byte[] rpc_convert_and_serialize(Data d) throws IOException {
		ClientProtos.GetRequest req = RequestConverter.buildGetRequest(d.regionName, d.get);
		return req.toByteArray();
	}

	@Benchmark
	public Object grpc_convert_only(Data d) throws IOException {
		return GrpcRequestConverter.buildGetRequest(d.regionName, d.get);
	}

	@Benchmark
	public byte[] grpc_convert_and_serialize(Data d) throws IOException {
		org.apache.hadoop.hbase.shaded.grpc.generated.GetRequest req =
			GrpcRequestConverter.buildGetRequest(d.regionName, d.get);
		return req.toByteArray();
	}

	// Result conversions (100 cells x 1k values)
	@Benchmark
	public Object rpc_result_convert(Data d) {
		return ProtobufUtil.toResult(d.result100x1k);
	}

	@Benchmark
	public byte[] rpc_result_convert_and_serialize(Data d) {
		ClientProtos.Result r = ProtobufUtil.toResult(d.result100x1k);
		return r.toByteArray();
	}

	@Benchmark
	public Object grpc_result_convert(Data d) {
		return GrpcUtil.toResult(d.result100x1k, false);
	}

	@Benchmark
	public byte[] grpc_result_convert_and_serialize(Data d) {
		org.apache.hadoop.hbase.shaded.grpc.generated.Result r = GrpcUtil.toResult(d.result100x1k, false);
		return r.toByteArray();
	}

	// Wrap converted Result into GetResponse
	@Benchmark
	public byte[] rpc_result_into_getResponse_and_serialize(Data d) {
		ClientProtos.Result r = ProtobufUtil.toResult(d.result100x1k);
		ClientProtos.GetResponse resp = ClientProtos.GetResponse.newBuilder().setResult(r).build();
		return resp.toByteArray();
	}

	@Benchmark
	public byte[] grpc_result_into_getResponse_and_serialize(Data d) {
		org.apache.hadoop.hbase.shaded.grpc.generated.Result r = GrpcUtil.toResult(d.result100x1k, false);
		org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse resp =
			org.apache.hadoop.hbase.shaded.grpc.generated.GetResponse.newBuilder().setResult(r).build();
		return resp.toByteArray();
	}
}
