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

import { Metadata, MetadataBuilder } from "../../../../../../src/org/apache/spark/sql/types/metadata";

test("metadata", () => {
  const builder = new MetadataBuilder();
  const metadata1 = builder.build();
  expect(metadata1.isEmpty()).toBe(true);
  const metadata2 = builder.putString("key1", "value1").build();
  expect(metadata2.isEmpty()).toBe(false);
  expect(metadata2.getString("key1")).toBe("value1");
  expect(() => metadata2.getLong("key1")).toThrow("Value 'value1' for 'key1' is not a number");

  const longArr = [1, 2, 3];
  const doubleArr = [1.0, 2.0, 3.0];
  const booleanArr = [true, false, true];
  const stringArr = ["value1", "value2", "value3"];
  const metadataArr = [metadata1, metadata2];
  const metadata = builder
    .putNull("key1")
    .putString("key2", "value1")
    .putLong("key3", 1)
    .putDouble("key4", 1.0)
    .putBoolean("key5", true)
    .putMetadata("key6", metadata1)
    .putMetadata("key7", metadata2)
    .putLongArray("key8", longArr)
    .putDoubleArray("key9", doubleArr)
    .putBooleanArray("key10", booleanArr)
    .putStringArray("key11", stringArr)
    .putMetadataArray("key12", metadataArr)
    .build();

  expect(metadata.contains("key1")).toBe(true);
  expect(metadata.getString("key2")).toBe("value1");
  expect(metadata.getLong("key3")).toBe(1);
  expect(metadata.getDouble("key4")).toBe(1.0);
  expect(metadata.getBoolean("key5")).toBe(true);
  expect(metadata.getMetadata("key6").metadata).toBe(metadata1.metadata);
  expect(metadata.getMetadata("key7").metadata).toBe(metadata2.metadata);
  expect(metadata.getLongArray("key8")).toBe(longArr);
  expect(metadata.getDoubleArray("key9")).toBe(doubleArr);
  expect(metadata.getBooleanArray("key10")).toBe(booleanArr);
  expect(metadata.getStringArray("key11")).toBe(stringArr);
  expect(metadata.getMetadataArray("key12")).toStrictEqual(metadataArr);


  expect(() => metadata.getBoolean("key1")).toThrow("Value 'null' for 'key1' is not a boolean");
  expect(() => metadata.getDouble("key1")).toThrow("Value 'null' for 'key1' is not a number");
  expect(() => metadata.getLong("key1")).toThrow("Value 'null' for 'key1' is not a number");
  expect(() => metadata.getString("key1")).toThrow("Value 'null' for 'key1' is not a string");
  expect(() => metadata.getMetadata("key2")).toThrow("Value 'value1' for 'key2' is not a Metadata");
  expect(() => metadata.getLongArray("key1")).toThrow("Value 'null' for 'key1' is not a number array");
  expect(() => metadata.getDoubleArray("key1")).toThrow("Value 'null' for 'key1' is not a number array");
  expect(() => metadata.getBooleanArray("key1")).toThrow("Value 'null' for 'key1' is not a boolean array");
  expect(() => metadata.getStringArray("key1")).toThrow("Value 'null' for 'key1' is not a string array");
  expect(() => metadata.getMetadataArray("key1")).toThrow("Value 'null' for 'key1' is not a Metadata array");

  expect(Metadata.fromJson(metadata.json())).toStrictEqual(metadata);

  builder.withMetadata(Metadata.fromJson("{\"kent\": \"yao\"}"));

  expect(builder.build().getString("kent")).toBe("yao");

  builder.remove("kent");
  expect(builder.build().contains("kent")).toBe(false);
  builder.remove("kent")
  expect(() => builder.build().get("kent")).toThrow("Key 'kent' not found");

  expect(Metadata.empty().isEmpty()).toBe(true);
});