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

import { tableFromArrays, tableToIPC } from "apache-arrow";
import { createLocalRelation, createLocalRelationFromArrowTable, toJoinTypePB, toLateralJoinTypePB } from "../../../../../../src/org/apache/spark/sql/proto/ProtoUtils";
import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types";
import { StructType } from "../../../../../../src/org/apache/spark/sql/types/StructType";
import { Join_JoinType, LateralJoin_JoinType } from "../../../../../../src/gen/spark/connect/relations_pb";

test("createLocalRelation from empty object", () => {
  const schema = DataTypes.createStructType([]);
  const t0 = tableFromArrays({});
  expect(t0.schema.fields.length).toBe(0);
  expect(DataTypes.fromArrowSchema(t0.schema)).toBeInstanceOf(StructType);
  expect(DataTypes.fromArrowSchema(t0.schema)).toStrictEqual(schema);
  expect(DataTypes.fromArrowSchema(t0.schema).toDDL()).toBe("");
  const l1 = createLocalRelationFromArrowTable(t0, schema);
  expect(l1.schema).toBe("");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("");
  expect(l2.data).toStrictEqual(l1.data);
});

test("createLocalRelation from object with one field", () => {
  const schema = DataTypes.createStructType([DataTypes.createStructField("a", DataTypes.DoubleType, true)]);
  const t0 = tableFromArrays({ a: [1, 2, 3]});
  expect(t0.schema.fields.length).toBe(1);
  expect(DataTypes.fromArrowSchema(t0.schema)).toBeInstanceOf(StructType);
  expect(DataTypes.fromArrowSchema(t0.schema)).toStrictEqual(schema);
  expect(DataTypes.fromArrowSchema(t0.schema).toDDL()).toBe("a DOUBLE");
  const l1 = createLocalRelationFromArrowTable(t0, schema);
  expect(l1.schema).toBe("a DOUBLE");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("a DOUBLE");
  expect(l2.data).toStrictEqual(l1.data);
});

test("createLocalRelation from object with two fields", () => {
  const schema = DataTypes.createStructType([DataTypes.createStructField("a", DataTypes.DoubleType, true), DataTypes.createStructField("b", DataTypes.ByteType, false)]);
  const t0 = tableFromArrays({ a: [1, 2, 3], b: new Int8Array([4, 5, 256])});
  expect(t0.schema.fields.length).toBe(2);
  expect(DataTypes.fromArrowSchema(t0.schema)).toBeInstanceOf(StructType);
  expect(DataTypes.fromArrowSchema(t0.schema)).toStrictEqual(schema);
  expect(DataTypes.fromArrowSchema(t0.schema).toDDL()).toBe("a DOUBLE, b TINYINT NOT NULL");
  const l1 = createLocalRelationFromArrowTable(t0, DataTypes.createStructType([DataTypes.createStructField("a", DataTypes.DoubleType, true), DataTypes.createStructField("b", DataTypes.ByteType, false)]));
  expect(l1.schema).toBe("a DOUBLE, b TINYINT NOT NULL");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("a DOUBLE, b TINYINT NOT NULL");
  expect(l2.data).toStrictEqual(l1.data);
});

test("createLocalRelation from object with three fields", () => {
  const schema = DataTypes.createStructType([DataTypes.createStructField("a", DataTypes.DoubleType, true), DataTypes.createStructField("b", DataTypes.ByteType, false), DataTypes.createStructField("c", DataTypes.StringType, true)]);
  const t0 = tableFromArrays({ a: [1, 2, 3], b: new Int8Array([4, 5, 256]), c: ["x", "y", "z"]});
  expect(t0.schema.fields.length).toBe(3);
  expect(DataTypes.fromArrowSchema(t0.schema)).toBeInstanceOf(StructType);
  expect(DataTypes.fromArrowSchema(t0.schema)).toStrictEqual(schema);
  expect(DataTypes.fromArrowSchema(t0.schema).toDDL()).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING");
  const l1 = createLocalRelationFromArrowTable(t0, schema);
  expect(l1.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING");
  expect(l2.data).toStrictEqual(l1.data);
});

test("createLocalRelation from object with four fields", () => {
  const schema = DataTypes.createStructType([
    DataTypes.createStructField("a", DataTypes.DoubleType, true),
    DataTypes.createStructField("b", DataTypes.ByteType, false),
    DataTypes.createStructField("c", DataTypes.StringType, true),
    DataTypes.createStructField("d", DataTypes.BooleanType, true)]);
  const t0 = tableFromArrays({ a: [1, 2, 3], b: new Int8Array([4, 5, 256]), c: ["x", "y", "z"], d: [true, false, true]});
  expect(t0.schema.fields.length).toBe(4);
  expect(DataTypes.fromArrowSchema(t0.schema)).toBeInstanceOf(StructType);
  expect(DataTypes.fromArrowSchema(t0.schema)).toStrictEqual(schema);
  expect(DataTypes.fromArrowSchema(t0.schema).toDDL()).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING, d BOOLEAN");
  const l1 = createLocalRelationFromArrowTable(t0, schema);
  expect(l1.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING, d BOOLEAN");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING, d BOOLEAN");
  expect(l2.data).toStrictEqual(l1.data);
});

test("createLocalRelation from object with five fields", () => {
  const schema = DataTypes.createStructType(
    [
      DataTypes.createStructField("a", DataTypes.DoubleType, true),
      DataTypes.createStructField("b", DataTypes.ByteType, false),
      DataTypes.createStructField("c", DataTypes.StringType, true),
      DataTypes.createStructField("d", DataTypes.BooleanType, true),
      DataTypes.createStructField("e", DataTypes.DateType, true)
    ]);
  const t0 = tableFromArrays(
    { a: [1, 2, 3],
      b: new Int8Array([4, 5, 256]),
      c: ["x", "y", "z"],
      d: [true, false, true],
      e: [new Date(0), new Date(1), new Date(2)]
    });
  expect(t0.schema.fields.length).toBe(5);
  const l1 = createLocalRelationFromArrowTable(t0, schema);
  expect(l1.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING, d BOOLEAN, e DATE");
  expect(l1.data).toStrictEqual(tableToIPC(t0));
  const l2 = createLocalRelation(l1.schema, l1.data);
  expect(l2).toStrictEqual(l1);
  expect(l2.schema).toBe("a DOUBLE, b TINYINT NOT NULL, c STRING, d BOOLEAN, e DATE");
  expect(l2.data).toStrictEqual(l1.data);
});

test("toJoinTypePB - inner join", () => {
  expect(toJoinTypePB("inner")).toBe(Join_JoinType.INNER);
});

test("toJoinTypePB - full outer join variations", () => {
  expect(toJoinTypePB("outer")).toBe(Join_JoinType.FULL_OUTER);
  expect(toJoinTypePB("full")).toBe(Join_JoinType.FULL_OUTER);
  expect(toJoinTypePB("fullouter")).toBe(Join_JoinType.FULL_OUTER);
  expect(toJoinTypePB("full_outer")).toBe(Join_JoinType.FULL_OUTER);
  expect(toJoinTypePB("FULL")).toBe(Join_JoinType.FULL_OUTER);
  expect(toJoinTypePB("FULLOUTER")).toBe(Join_JoinType.FULL_OUTER);
});

test("toJoinTypePB - left outer join variations", () => {
  expect(toJoinTypePB("left")).toBe(Join_JoinType.LEFT_OUTER);
  expect(toJoinTypePB("leftouter")).toBe(Join_JoinType.LEFT_OUTER);
  expect(toJoinTypePB("left_outer")).toBe(Join_JoinType.LEFT_OUTER);
  expect(toJoinTypePB("LEFT")).toBe(Join_JoinType.LEFT_OUTER);
  expect(toJoinTypePB("LEFTOUTER")).toBe(Join_JoinType.LEFT_OUTER);
});

test("toJoinTypePB - right outer join variations", () => {
  expect(toJoinTypePB("right")).toBe(Join_JoinType.RIGHT_OUTER);
  expect(toJoinTypePB("rightouter")).toBe(Join_JoinType.RIGHT_OUTER);
  expect(toJoinTypePB("right_outer")).toBe(Join_JoinType.RIGHT_OUTER);
  expect(toJoinTypePB("RIGHT")).toBe(Join_JoinType.RIGHT_OUTER);
});

test("toJoinTypePB - semi join variations", () => {
  expect(toJoinTypePB("semi")).toBe(Join_JoinType.LEFT_SEMI);
  expect(toJoinTypePB("leftsemi")).toBe(Join_JoinType.LEFT_SEMI);
  expect(toJoinTypePB("left_semi")).toBe(Join_JoinType.LEFT_SEMI);
  expect(toJoinTypePB("SEMI")).toBe(Join_JoinType.LEFT_SEMI);
});

test("toJoinTypePB - anti join variations", () => {
  expect(toJoinTypePB("anti")).toBe(Join_JoinType.LEFT_ANTI);
  expect(toJoinTypePB("leftanti")).toBe(Join_JoinType.LEFT_ANTI);
  expect(toJoinTypePB("left_anti")).toBe(Join_JoinType.LEFT_ANTI);
  expect(toJoinTypePB("ANTI")).toBe(Join_JoinType.LEFT_ANTI);
});

test("toJoinTypePB - cross join", () => {
  expect(toJoinTypePB("cross")).toBe(Join_JoinType.CROSS);
  expect(toJoinTypePB("CROSS")).toBe(Join_JoinType.CROSS);
});

test("toJoinTypePB - default to inner for undefined or unknown", () => {
  expect(toJoinTypePB(undefined)).toBe(Join_JoinType.INNER);
  expect(toJoinTypePB("unknown")).toBe(Join_JoinType.INNER);
});

test("toLateralJoinTypePB - inner join", () => {
  expect(toLateralJoinTypePB("inner")).toBe(LateralJoin_JoinType.INNER);
  expect(toLateralJoinTypePB("INNER")).toBe(LateralJoin_JoinType.INNER);
});

test("toLateralJoinTypePB - left outer join variations", () => {
  expect(toLateralJoinTypePB("left")).toBe(LateralJoin_JoinType.LEFT_OUTER);
  expect(toLateralJoinTypePB("leftouter")).toBe(LateralJoin_JoinType.LEFT_OUTER);
  expect(toLateralJoinTypePB("left_outer")).toBe(LateralJoin_JoinType.LEFT_OUTER);
  expect(toLateralJoinTypePB("LEFT")).toBe(LateralJoin_JoinType.LEFT_OUTER);
});

test("toLateralJoinTypePB - cross join", () => {
  expect(toLateralJoinTypePB("cross")).toBe(LateralJoin_JoinType.CROSS);
  expect(toLateralJoinTypePB("CROSS")).toBe(LateralJoin_JoinType.CROSS);
});

test("toLateralJoinTypePB - default to inner for undefined or unknown", () => {
  expect(toLateralJoinTypePB(undefined)).toBe(LateralJoin_JoinType.INNER);
  expect(toLateralJoinTypePB("unknown")).toBe(LateralJoin_JoinType.INNER);
});

