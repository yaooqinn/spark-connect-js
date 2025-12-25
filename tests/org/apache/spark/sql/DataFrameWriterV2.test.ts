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

import { WriteOperationV2_Mode } from "../../../../../src/gen/spark/connect/commands_pb";
import { Column } from "../../../../../src/org/apache/spark/sql/Column";
import { sharedSpark } from "../../../../helpers";

/*eslint @typescript-eslint/no-explicit-any : "off"*/
test("DataFrameWriterV2 basic construction", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect(writerV2).toBeDefined();
  expect((writerV2 as any).tableName_).toBe("test_table");
  expect((writerV2 as any).df_).toBe(df);
});

test("DataFrameWriterV2 using()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).provider_).toBeUndefined();
  
  writerV2.using("parquet");
  expect((writerV2 as any).provider_).toBe("parquet");
  
  writerV2.using("iceberg");
  expect((writerV2 as any).provider_).toBe("iceberg");
  
  writerV2.using("delta");
  expect((writerV2 as any).provider_).toBe("delta");
});

test("DataFrameWriterV2 option()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).options_.size).toBe(0);
  
  writerV2.option("key1", "value1");
  expect((writerV2 as any).options_.size).toBe(1);
  expect((writerV2 as any).options_.get("key1")).toBe("value1");
  
  writerV2.option("key2", "value2");
  expect((writerV2 as any).options_.size).toBe(2);
  expect((writerV2 as any).options_.get("key2")).toBe("value2");
});

test("DataFrameWriterV2 options()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).options_.size).toBe(0);
  
  writerV2.options({ key1: "value1", key2: "value2" });
  expect((writerV2 as any).options_.size).toBe(2);
  expect((writerV2 as any).options_.get("key1")).toBe("value1");
  expect((writerV2 as any).options_.get("key2")).toBe("value2");
  
  writerV2.options({ key3: "value3" });
  expect((writerV2 as any).options_.size).toBe(3);
  expect((writerV2 as any).options_.get("key3")).toBe("value3");
});

test("DataFrameWriterV2 tableProperty()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).tableProperties_.size).toBe(0);
  
  writerV2.tableProperty("compression", "snappy");
  expect((writerV2 as any).tableProperties_.size).toBe(1);
  expect((writerV2 as any).tableProperties_.get("compression")).toBe("snappy");
  
  writerV2.tableProperty("format", "parquet");
  expect((writerV2 as any).tableProperties_.size).toBe(2);
  expect((writerV2 as any).tableProperties_.get("format")).toBe("parquet");
});

test("DataFrameWriterV2 partitionBy() with strings", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).partitionColumns_.length).toBe(0);
  
  writerV2.partitionBy("year");
  expect((writerV2 as any).partitionColumns_.length).toBe(1);
  expect((writerV2 as any).partitionColumns_[0]).toBeInstanceOf(Column);
  
  writerV2.partitionBy("year", "month");
  expect((writerV2 as any).partitionColumns_.length).toBe(2);
  
  writerV2.partitionBy("year", "month", "day");
  expect((writerV2 as any).partitionColumns_.length).toBe(3);
});

test("DataFrameWriterV2 partitionBy() with Columns", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  const col1 = new Column("year");
  const col2 = new Column("month");
  
  writerV2.partitionBy(col1, col2);
  expect((writerV2 as any).partitionColumns_.length).toBe(2);
  expect((writerV2 as any).partitionColumns_[0]).toBe(col1);
  expect((writerV2 as any).partitionColumns_[1]).toBe(col2);
});

test("DataFrameWriterV2 partitionBy() with mixed types", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  const col1 = new Column("year");
  
  writerV2.partitionBy(col1, "month");
  expect((writerV2 as any).partitionColumns_.length).toBe(2);
  expect((writerV2 as any).partitionColumns_[0]).toBe(col1);
  expect((writerV2 as any).partitionColumns_[1]).toBeInstanceOf(Column);
});

test("DataFrameWriterV2 clusterBy()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).clusteringColumns_.length).toBe(0);
  
  writerV2.clusterBy("user_id");
  expect((writerV2 as any).clusteringColumns_).toStrictEqual(["user_id"]);
  
  writerV2.clusterBy("user_id", "event_type");
  expect((writerV2 as any).clusteringColumns_).toStrictEqual(["user_id", "event_type"]);
  
  writerV2.clusterBy("user_id", "event_type", "timestamp");
  expect((writerV2 as any).clusteringColumns_).toStrictEqual(["user_id", "event_type", "timestamp"]);
});

test("DataFrameWriterV2 method chaining", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  
  const writerV2 = df.writeTo("test_table")
    .using("parquet")
    .option("compression", "snappy")
    .options({ key1: "value1", key2: "value2" })
    .tableProperty("format.version", "2")
    .partitionBy("year", "month")
    .clusterBy("user_id");
  
  expect((writerV2 as any).provider_).toBe("parquet");
  expect((writerV2 as any).options_.size).toBe(3);
  expect((writerV2 as any).tableProperties_.size).toBe(1);
  expect((writerV2 as any).partitionColumns_.length).toBe(2);
  expect((writerV2 as any).clusteringColumns_.length).toBe(1);
});

test("DataFrameWriterV2 getModeProto()", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  const writerV2 = df.writeTo("test_table");
  
  expect((writerV2 as any).getModeProto("create")).toBe(WriteOperationV2_Mode.CREATE);
  expect((writerV2 as any).getModeProto("replace")).toBe(WriteOperationV2_Mode.REPLACE);
  expect((writerV2 as any).getModeProto("createOrReplace")).toBe(WriteOperationV2_Mode.CREATE_OR_REPLACE);
  expect((writerV2 as any).getModeProto("append")).toBe(WriteOperationV2_Mode.APPEND);
  expect((writerV2 as any).getModeProto("overwrite")).toBe(WriteOperationV2_Mode.OVERWRITE);
  expect((writerV2 as any).getModeProto("overwritePartitions")).toBe(WriteOperationV2_Mode.OVERWRITE_PARTITIONS);
  
  expect(() => (writerV2 as any).getModeProto("invalid")).toThrow("INVALID_WRITE_MODE_V2");
  expect(() => (writerV2 as any).getModeProto("invalid")).toThrow("invalid");
});
