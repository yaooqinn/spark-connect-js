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

import { DataFrame } from "../../../../../src/org/apache/spark/sql/DataFrame";
import { Row } from "../../../../../src/org/apache/spark/sql/Row";
import { DataTypes } from "../../../../../src/org/apache/spark/sql/types";
import { StructType } from "../../../../../src/org/apache/spark/sql/types/StructType";
import { sharedSpark } from "../../../../helpers";



test("empty data frame", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame;
  expect(df).toBeInstanceOf(DataFrame);
  const schema = await df.schema();
  expect(schema.fields.length).toBe(0);
  const result = await df.collect();
  expect(result.length).toBe(0);
});

test("create data frame", async () => {
  const spark = await sharedSpark;
  const rows: Row[] = [];
  const schema = new StructType().add("name", DataTypes.StringType).add("age", DataTypes.IntegerType);
  const r1 = new Row(schema)
  r1[0] = "Alice";
  r1[1] = 10;
  rows.push(r1);
  const r2 = new Row(schema);
  r2[0] = "Bob";
  r2[1] = 20;
  rows.push(r2);
  const df = spark.createDataFrame(rows, schema);
  const result = await df.collect();
  expect(result.length).toBe(2);
  expect(result[0].getString(0)).toBe("Alice");
  expect(result[0].getInt(1)).toBe(10);
  expect(result[1].getString(0)).toBe("Bob");
  expect(result[1].getInt(1)).toBe(20);
  const newSchema = await df.schema();
  expect(newSchema).toStrictEqual(schema);
});

test("range", async () => {
  const spark = await sharedSpark;
  const df1 = spark.range(10);
  const schema = await df1.schema();
  expect(schema).toStrictEqual(new StructType().add("id", DataTypes.LongType, false));
  const result = await df1.collect();
  expect(result.length).toBe(10);
  for (let i = 0; i < 10; i++) {
    expect(result[i].getLong(0)).toBe(BigInt(i));
  }
  const df2 = spark.range(15n);
  const result2 = await df2.collect();
  expect(result2.length).toBe(15);
  for (let i = 0; i < 10; i++) {
    expect(result2[i].getLong(0)).toBe(BigInt(i));
  }

  const df3 = spark.range(5, 10);
  const result3 = await df3.collect();
  expect(result3.length).toBe(5);
  for (let i = 0; i < 5; i++) {
    expect(result3[i].getLong(0)).toBe(BigInt(i + 5));
  }

  const df4 = spark.range(5n, 10n);
  const result4 = await df4.collect();
  expect(result4.length).toBe(5);
  for (let i = 0; i < 5; i++) {
    expect(result4[i].getLong(0)).toBe(BigInt(i + 5));
  }

  const df5 = spark.range(5, 10, 2);
  const result5 = await df5.collect();
  expect(result5.length).toBe(3);
  expect(result5[0].getLong(0)).toBe(5n);
  expect(result5[1].getLong(0)).toBe(7n);
  expect(result5[2].getLong(0)).toBe(9n);

  const df6 = spark.range(5n, 10n, 2n);
  const result6 = await df6.collect();
  expect(result6.length).toBe(3);
  expect(result6[0].getLong(0)).toBe(5n);
  expect(result6[1].getLong(0)).toBe(7n);
  expect(result6[2].getLong(0)).toBe(9n);

  const df7 = spark.range(5, 10, 2, 2);
  const result7 = await df7.collect();
  expect(result7.length).toBe(3);
  expect(result7[0].getLong(0)).toBe(5n);
  expect(result7[1].getLong(0)).toBe(7n);
  expect(result7[2].getLong(0)).toBe(9n);
});
