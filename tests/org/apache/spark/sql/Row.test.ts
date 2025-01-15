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

import { compareSchemas } from "apache-arrow/visitor/typecomparator";
import { bigintToDecimalBigNum, tableFromRows, tableToRows } from "../../../../../src/org/apache/spark/sql/arrow/ArrowUtils";
import { Row } from "../../../../../src/org/apache/spark/sql/Row";
import { DataTypes } from "../../../../../src/org/apache/spark/sql/types/DataTypes";

const { createStructField, createStructType } = DataTypes;

test("Row test", () => {
  const decimalType = DataTypes.createDecimalType(10, 2)
  const schema = createStructType([
    createStructField("f0", DataTypes.NullType, true),
    createStructField("f1", DataTypes.BooleanType, true),
    createStructField("f2", DataTypes.ByteType, true),
    createStructField("f3", DataTypes.ShortType, true),
    createStructField("f4", DataTypes.IntegerType, true),
    createStructField("f5", DataTypes.LongType, true),
    createStructField("f6", DataTypes.FloatType, true),
    createStructField("f7", DataTypes.DoubleType, true),
    createStructField("f8", DataTypes.StringType, true),
    createStructField("f9", DataTypes.DateType, true),
    createStructField("f10", DataTypes.TimestampType, true),
    createStructField("f11", DataTypes.TimestampNTZType, true),
    createStructField("f12", DataTypes.BinaryType, true),
    createStructField("f13", decimalType, true),
  ])
  const row0 = new Row(schema);
  const row1 = new Row(schema);
  const row2 = new Row(schema);

  for (let i = 0; i < schema.fields.length; i++) {
    row0[i] = null;
    switch(schema.fields[i].dataType) {
      case DataTypes.BooleanType:
        row1[i] = true;
        row2[i] = false;
        break;
      case DataTypes.ByteType:
        row1[i] = -0x80;
        row2[i] = 0x7F;
        break;
      case DataTypes.ShortType:
        row1[i] = -0x8000;
        row2[i]= 0x7FFF;
        break;
      case DataTypes.IntegerType:
        row1[i] = -0x80000000;
        row2[i] = 0x7FFFFFFF;
        break;
      case DataTypes.LongType:
        row1[i] = -0x8000000000000000n;
        row2[i] = 0x7FFFFFFFFFFFFFFFn;
        break;
      case DataTypes.FloatType:
        row1[i] = -1.0;
        row2[i] = 2.0;
        break;
      case DataTypes.DoubleType:
        row1[i] = -3.0;
        row2[i] = 4.0;
        break;

      case decimalType:
        row1[i] = bigintToDecimalBigNum(-123n);
        row2[i] = bigintToDecimalBigNum(456n);
        break;
      case DataTypes.StringType:
        row1[i] = "a";
        row2[i] = "b";
        break;
      case DataTypes.DateType:
        row1[i] = new Date(0).getTime();
        row2[i] = new Date(86400000).getTime();
        break;
      case DataTypes.TimestampType:
        row1[i] = new Date("2021-01-01T00:00:00.000Z").getTime();
        row2[i] = new Date("2018-11-17T13:13:33.000Z").getTime();
        break;
      case DataTypes.TimestampNTZType:
        row1[i] = new Date("2021-01-01T00:00:00.000Z").getTime();
        row2[i] = new Date("2018-11-17T13:13:33.000Z").getTime();
        break;
      case DataTypes.BinaryType:
        row1[i] = new Uint8Array([1, 2, 3]);
        row2[i] = new Uint8Array([4, 5, 6]);
        break;
      default:
        row1[i] = null;
        row2[i] = null;
    }
  }
  expect(() => row0.getAs<number>(100)).toThrow("does not exist");
  const table = tableFromRows([row0, row1, row2], schema);
  expect(table.numCols).toBe(schema.fields.length);
  expect(table.numRows).toBe(3);
  expect(compareSchemas(table.schema, DataTypes.toArrowSchema(schema))).toBe(true);
  const rows = tableToRows(table, schema);
  expect(rows).toStrictEqual([row0, row1, row2]);
  expect(row0.size()).toBe(schema.fields.length);
  for (let i = 0; i < row0.length; i++) {
    expect(row0.isNullAt(i)).toBe(true);
    expect(row0.get(0)).toBeNull();
    expect(rows[0].isNullAt(i)).toBe(true);
    expect(rows[0].get(i)).toBeNull();
  }
  expect(row1.getBoolean(1)).toBe(true);
  expect(row2.getBoolean(1)).toBe(false);
  expect(row1.getByte(2)).toBe(-0x80);
  expect(row2.getByte(2)).toBe(0x7F);
  expect(row1.getShort(3)).toBe(-0x8000);
  expect(row2.getShort(3)).toBe(0x7FFF);
  expect(row1.getInt(4)).toBe(-0x80000000);
  expect(row2.getInt(4)).toBe(0x7FFFFFFF);
  expect(row1.getLong(5)).toBe(-0x8000000000000000n);
  expect(row2.getLong(5)).toBe(0x7FFFFFFFFFFFFFFFn);
  expect(row1.getFloat(6)).toBe(-1.0);
  expect(row2.getFloat(6)).toBe(2.0);
  expect(row1.getDouble(7)).toBe(-3.0);
  expect(row2.getDouble(7)).toBe(4.0);
  expect(row1.getDecimal(13)).toBe(-1.23);
  expect(row2.getDecimal(13)).toBe(4.56);
  expect(row1.getString(8)).toBe("a");
  expect(row2.getString(8)).toBe("b");
  expect(row1.getDate(9)).toStrictEqual(new Date(0));
  expect(row2.getDate(9)).toStrictEqual(new Date(86400000));
  expect(row1.getAs<Date>(10)).toStrictEqual(new Date("2021-01-01T00:00:00.000Z"));
  expect(row2.getAs<Date>(10)).toStrictEqual(new Date("2018-11-17T13:13:33.000Z"));
  expect(row1.getAs<Date>(11)).toStrictEqual(new Date("2021-01-01T00:00:00.000Z"));
  expect(row2.getAs<Date>(11)).toStrictEqual(new Date("2018-11-17T13:13:33.000Z"));
  expect(row1.getUint8Array(12)).toStrictEqual(new Uint8Array([1, 2, 3]));
  expect(row2.getUint8Array(12)).toStrictEqual(new Uint8Array([4, 5, 6]));
});

    