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

import { DataStreamReader } from "../../../../../src/org/apache/spark/sql/DataStreamReader";
import { DataStreamWriter } from "../../../../../src/org/apache/spark/sql/DataStreamWriter";
import { SparkSession } from "../../../../../src/org/apache/spark/sql/SparkSession";
import { Client } from "../../../../../src/org/apache/spark/sql/grpc/Client";

/*eslint @typescript-eslint/no-explicit-any : "off"*/

// Create a mock SparkSession for testing API structure without network calls
const mockClient = {} as Client;
const mockSpark = new SparkSession(mockClient);

test("SparkSession has readStream property", () => {
  expect(mockSpark.readStream).toBeInstanceOf(DataStreamReader);
});

test("DataStreamReader format", () => {
  const reader = mockSpark.readStream;
  expect((reader as any).source).toBe(undefined);
  ["parquet", "json", "csv", "kafka", "rate"].forEach(format => {
    expect((reader.format(format) as any).source).toBe(format);
  });
});

test("DataStreamReader option", () => {
  const reader = mockSpark.readStream;
  expect((reader as any).extraOptions.size).toBe(0);
  expect((reader.option("key", "value") as any).extraOptions.size).toBe(1);
  expect((reader as any).extraOptions.get("key")).toBe("value");
  expect((reader.option("key2", "value2") as any).extraOptions.size).toBe(2);
  expect((reader as any).extraOptions.get("key2")).toBe("value2");
  expect((reader.option("key3", 1) as any).extraOptions.size).toBe(3);
  expect((reader as any).extraOptions.get("key3")).toBe("1");
  expect((reader.option("key4", 1.1) as any).extraOptions.size).toBe(4);
  expect((reader as any).extraOptions.get("key4")).toBe("1.1");
  expect((reader.option("key5", true) as any).extraOptions.size).toBe(5);
  expect((reader as any).extraOptions.get("key5")).toBe("true");
  expect((reader.options({ key6: "value6", key7: "2" }) as any).extraOptions.size).toBe(7);
  expect((reader as any).extraOptions.get("key6")).toBe("value6");
  expect((reader as any).extraOptions.get("key7")).toBe("2");
});

test("DataFrame has writeStream property", () => {
  const df = mockSpark.emptyDataFrame;
  expect(df.writeStream).toBeInstanceOf(DataStreamWriter);
});

test("DataStreamWriter format", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).source_).toBe(undefined);
  ["parquet", "json", "csv", "console", "memory"].forEach(format => {
    expect((writer.format(format) as any).source_).toBe(format);
  });
});

test("DataStreamWriter option", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).extraOptions_.size).toBe(0);
  expect((writer.option("key", "value") as any).extraOptions_.size).toBe(1);
  expect((writer as any).extraOptions_.get("key")).toBe("value");
  expect((writer.option("key2", "value2") as any).extraOptions_.size).toBe(2);
  expect((writer as any).extraOptions_.get("key2")).toBe("value2");
});

test("DataStreamWriter outputMode", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).outputMode_).toBe("append");
  expect((writer.outputMode("complete") as any).outputMode_).toBe("complete");
  expect((writer.outputMode("update") as any).outputMode_).toBe("update");
  expect((writer.outputMode("append") as any).outputMode_).toBe("append");
});

test("DataStreamWriter queryName", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).queryName_).toBe(undefined);
  expect((writer.queryName("myQuery") as any).queryName_).toBe("myQuery");
});

test("DataStreamWriter trigger", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).trigger_).toBe(undefined);
  expect((writer.trigger({ processingTime: "5 seconds" }) as any).trigger_).toStrictEqual({
    case: "processingTimeInterval",
    value: "5 seconds"
  });
  expect((writer.trigger({ once: true }) as any).trigger_).toStrictEqual({
    case: "once",
    value: true
  });
  expect((writer.trigger({ availableNow: true }) as any).trigger_).toStrictEqual({
    case: "availableNow",
    value: true
  });
  expect((writer.trigger({ continuous: "1 second" }) as any).trigger_).toStrictEqual({
    case: "continuousCheckpointInterval",
    value: "1 second"
  });
});

test("DataStreamWriter partitionBy", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).partitioningColumns_.length).toBe(0);
  expect((writer.partitionBy("col1") as any).partitioningColumns_).toStrictEqual(["col1"]);
  expect((writer.partitionBy("col1", "col2") as any).partitioningColumns_).toStrictEqual(["col1", "col2"]);
});

test("DataStreamWriter clusterBy", () => {
  const df = mockSpark.emptyDataFrame;
  const writer = df.writeStream;
  expect((writer as any).clusteringColumns_.length).toBe(0);
  expect((writer.clusterBy("col1") as any).clusteringColumns_).toStrictEqual(["col1"]);
  expect((writer.clusterBy("col1", "col2") as any).clusteringColumns_).toStrictEqual(["col1", "col2"]);
});
