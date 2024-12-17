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


import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types/DataTypes";
import { NullType } from "../../../../../../src/org/apache/spark/sql/types/NullType";
import { BooleanType } from "../../../../../../src/org/apache/spark/sql/types/BooleanType";
import { ByteType } from "../../../../../../src/org/apache/spark/sql/types/ByteType";
import { ShortType } from "../../../../../../src/org/apache/spark/sql/types/ShortType";
import { IntegerType } from "../../../../../../src/org/apache/spark/sql/types/IntegerType";
import { LongType } from "../../../../../../src/org/apache/spark/sql/types/LongType";
import { FloatType } from "../../../../../../src/org/apache/spark/sql/types/FloatType";
import { DoubleType } from "../../../../../../src/org/apache/spark/sql/types/DoubleType";
import { DecimalType } from "../../../../../../src/org/apache/spark/sql/types/DecimalType";
import { StringType } from "../../../../../../src/org/apache/spark/sql/types/StringType";
import { BinaryType } from "../../../../../../src/org/apache/spark/sql/types/BinaryType";
import { VariantType } from "../../../../../../src/org/apache/spark/sql/types/VariantType";
import { DateType } from "../../../../../../src/org/apache/spark/sql/types/DateType";
import { TimestampType } from "../../../../../../src/org/apache/spark/sql/types/TimestampType";
import { TimestampNTZType } from "../../../../../../src/org/apache/spark/sql/types/TimestampNTZType";
import { Metadata } from "../../../../../../src/org/apache/spark/sql/types/metadata";

test ("NullType", () => {
  expect(DataTypes.NullType).toBe(NullType.INSTANCE);
  expect(DataTypes.NullType.typeName()).toBe("void");
  expect(DataTypes.NullType.simpleString()).toBe("void");
  expect(DataTypes.NullType.catalogString()).toBe("void");
  expect(DataTypes.NullType.defaultSize()).toBe(1);
  expect(DataTypes.NullType.sql()).toBe("VOID");
});

test ("BooleanType", () => {
  expect(DataTypes.BooleanType).toBe(BooleanType.INSTANCE);
  expect(DataTypes.BooleanType.typeName()).toBe("boolean");
  expect(DataTypes.BooleanType.simpleString()).toBe("boolean");
  expect(DataTypes.BooleanType.catalogString()).toBe("boolean");
  expect(DataTypes.BooleanType.defaultSize()).toBe(1);
  expect(DataTypes.BooleanType.sql()).toBe("BOOLEAN");
});

test("ByteType", () => {
  expect(DataTypes.ByteType).toBe(ByteType.INSTANCE);
  expect(DataTypes.ByteType.typeName()).toBe("byte");
  expect(DataTypes.ByteType.simpleString()).toBe("tinyint");
  expect(DataTypes.ByteType.catalogString()).toBe("tinyint");
  expect(DataTypes.ByteType.defaultSize()).toBe(1);
  expect(DataTypes.ByteType.sql()).toBe("TINYINT");
});

test("ShortType", () => {
  expect(DataTypes.ShortType).toBe(ShortType.INSTANCE);
  expect(DataTypes.ShortType.typeName()).toBe("short");
  expect(DataTypes.ShortType.simpleString()).toBe("smallint");
  expect(DataTypes.ShortType.catalogString()).toBe("smallint");
  expect(DataTypes.ShortType.defaultSize()).toBe(2);
  expect(DataTypes.ShortType.sql()).toBe("SMALLINT");
});

test("IntegerType", () => {
  expect(DataTypes.IntegerType).toBe(IntegerType.INSTANCE);
  expect(DataTypes.IntegerType.typeName()).toBe("integer");
  expect(DataTypes.IntegerType.simpleString()).toBe("int");
  expect(DataTypes.IntegerType.catalogString()).toBe("int");
  expect(DataTypes.IntegerType.defaultSize()).toBe(4);
  expect(DataTypes.IntegerType.sql()).toBe("INT");
});

test("LongType", () => {
  expect(DataTypes.LongType).toBe(LongType.INSTANCE);
  expect(DataTypes.LongType.typeName()).toBe("long");
  expect(DataTypes.LongType.simpleString()).toBe("bigint");
  expect(DataTypes.LongType.catalogString()).toBe("bigint");
  expect(DataTypes.LongType.defaultSize()).toBe(8);
  expect(DataTypes.LongType.sql()).toBe("BIGINT");
});

test("FloatType", () => {
  expect(DataTypes.FloatType).toBe(FloatType.INSTANCE);
  expect(DataTypes.FloatType.typeName()).toBe("float");
  expect(DataTypes.FloatType.simpleString()).toBe("float");
  expect(DataTypes.FloatType.catalogString()).toBe("float");
  expect(DataTypes.FloatType.defaultSize()).toBe(4);
  expect(DataTypes.FloatType.sql()).toBe("FLOAT");
});

test("DoubleType", () => {
  expect(DataTypes.DoubleType).toBe(DoubleType.INSTANCE);
  expect(DataTypes.DoubleType.typeName()).toBe("double");
  expect(DataTypes.DoubleType.simpleString()).toBe("double");
  expect(DataTypes.DoubleType.catalogString()).toBe("double");
  expect(DataTypes.DoubleType.defaultSize()).toBe(8);
  expect(DataTypes.DoubleType.sql()).toBe("DOUBLE");
});

test("DecimalType", () => {
  expect(DataTypes.createDecimalType()).toBe(DecimalType.USER_DEFAULT);
  expect(DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE)).toBe(DecimalType.SYSTEM_DEFAULT);
  expect(DataTypes.createDecimalType(10, 1)).toEqual(new DecimalType(10, 1));
  expect(DataTypes.createDecimalType(10, 1).precision).toBe(10);
  expect(DataTypes.createDecimalType(10, 1).scale).toBe(1);
  expect(DataTypes.createDecimalType(10, 1) === DataTypes.createDecimalType(10, 2)).toBe(false);
  expect(DataTypes.createDecimalType(10, 1).typeName()).toBe("decimal(10,1)");
  expect(DataTypes.createDecimalType(10, 1).simpleString()).toBe("decimal(10,1)");
  expect(DataTypes.createDecimalType(10, 1).catalogString()).toBe("decimal(10,1)");
  expect(DataTypes.createDecimalType(10, 1).defaultSize()).toBe(8);
  expect(DataTypes.createDecimalType(22, 1).defaultSize()).toBe(16);
  expect(DataTypes.createDecimalType(10, 1).sql()).toBe("DECIMAL(10,1)");
});

test("StringType", () => {
  expect(DataTypes.StringType).toBe(StringType.INSTANCE);
  expect(DataTypes.StringType.typeName()).toBe("string");
  expect(DataTypes.StringType.simpleString()).toBe("string");
  expect(DataTypes.StringType.catalogString()).toBe("string");
  expect(DataTypes.StringType.defaultSize()).toBe(20);
  expect(DataTypes.StringType.sql()).toBe("STRING");
  expect(new StringType("UTF8_BINARY")).toEqual(DataTypes.StringType);
  expect(new StringType("af").typeName()).toBe("string collate af");
  expect(new StringType("af").toString()).toBe("StringType(af)");
  expect(new StringType("UTF8_BINARY").toString()).toBe("StringType");
});

test("BinaryType", () => {
  expect(DataTypes.BinaryType).toBe(BinaryType.INSTANCE);
  expect(DataTypes.BinaryType.typeName()).toBe("binary");
  expect(DataTypes.BinaryType.simpleString()).toBe("binary");
  expect(DataTypes.BinaryType.catalogString()).toBe("binary");
  expect(DataTypes.BinaryType.defaultSize()).toBe(100);
  expect(DataTypes.BinaryType.sql()).toBe("BINARY");
});

test("CharType", () => {
  const charType = DataTypes.createCharType(10);
  expect(charType.typeName()).toBe("char(10)");
  expect(charType.simpleString()).toBe("char(10)");
  expect(charType.catalogString()).toBe("char(10)");
  expect(charType.defaultSize()).toBe(10);
  expect(charType.sql()).toBe("CHAR(10)");
  expect(charType).toEqual(DataTypes.createCharType(10));
  expect(charType.toString()).toBe("CharType(10)");

  expect(() => DataTypes.createCharType(0)).toThrow("CharType requires the length to be greater than 0");
});

test("VarcharType", () => {
  const varcharType = DataTypes.createVarCharType(10);
  expect(varcharType.typeName()).toBe("varchar(10)");
  expect(varcharType.simpleString()).toBe("varchar(10)");
  expect(varcharType.catalogString()).toBe("varchar(10)");
  expect(varcharType.defaultSize()).toBe(10);
  expect(varcharType.sql()).toBe("VARCHAR(10)");
  expect(varcharType).toEqual(DataTypes.createVarCharType(10));
  expect(varcharType.toString()).toBe("VarcharType(10)");

  expect(() => DataTypes.createVarCharType(0)).toThrow("VarcharType requires the length to be greater than 0");
});

test("VariantType", () => {
  const variantType = DataTypes.VariantType;
  expect(variantType.typeName()).toBe("variant");
  expect(variantType.simpleString()).toBe("variant");
  expect(variantType.catalogString()).toBe("variant");
  expect(variantType.defaultSize()).toBe(2048);
  expect(variantType.sql()).toBe("VARIANT");
  expect(variantType).toBe(VariantType.INSTANCE);
});

test("DateType", () => {
  expect(DataTypes.DateType).toBe(DateType.INSTANCE);
  expect(DataTypes.DateType.typeName()).toBe("date");
  expect(DataTypes.DateType.simpleString()).toBe("date");
  expect(DataTypes.DateType.catalogString()).toBe("date");
  expect(DataTypes.DateType.defaultSize()).toBe(4);
  expect(DataTypes.DateType.sql()).toBe("DATE");
});

test("TimestampType", () => {
  expect(DataTypes.TimestampType).toBe(TimestampType.INSTANCE);
  expect(DataTypes.TimestampType.typeName()).toBe("timestamp");
  expect(DataTypes.TimestampType.simpleString()).toBe("timestamp");
  expect(DataTypes.TimestampType.catalogString()).toBe("timestamp");
  expect(DataTypes.TimestampType.defaultSize()).toBe(8);
  expect(DataTypes.TimestampType.sql()).toBe("TIMESTAMP");
});

test("TimestampNTZType", () => {
  const timestampNTZ = DataTypes.TimestampNTZType;
  expect(DataTypes.TimestampNTZType).toBe(TimestampNTZType.INSTANCE);
  expect(timestampNTZ.typeName()).toBe("timestamp_ntz");
  expect(timestampNTZ.simpleString()).toBe("timestamp_ntz");
  expect(timestampNTZ.catalogString()).toBe("timestamp_ntz");
  expect(timestampNTZ.defaultSize()).toBe(8);
  expect(timestampNTZ.sql()).toBe("TIMESTAMP_NTZ");
});

test("CalendarIntervalType", () => {
  const calendarIntervalType = DataTypes.CalendarIntervalType;
  expect(calendarIntervalType.typeName()).toBe("interval");
  expect(calendarIntervalType.simpleString()).toBe("interval");
  expect(calendarIntervalType.catalogString()).toBe("interval");
  expect(calendarIntervalType.defaultSize()).toBe(16);
  expect(calendarIntervalType.sql()).toBe("INTERVAL");
});

test("DayTimeIntervalType", () => {
  const dayTimeIntervalType = DataTypes.createDayTimeIntervalType();
  expect(dayTimeIntervalType.typeName()).toBe("interval day to second");
  expect(dayTimeIntervalType.simpleString()).toBe("interval day to second");
  expect(dayTimeIntervalType.catalogString()).toBe("interval day to second");
  expect(dayTimeIntervalType.defaultSize()).toBe(8);
  expect(dayTimeIntervalType.sql()).toBe("INTERVAL DAY TO SECOND");

  expect(DataTypes.createDayTimeIntervalType(0, 0).typeName()).toBe("interval day");
  expect(DataTypes.createDayTimeIntervalType(1, 1).typeName()).toBe("interval hour");
  expect(DataTypes.createDayTimeIntervalType(2, 2).typeName()).toBe("interval minute");
  expect(DataTypes.createDayTimeIntervalType(3, 3).typeName()).toBe("interval second");
  expect(DataTypes.createDayTimeIntervalType(0, 1).typeName()).toBe("interval day to hour");
  expect(DataTypes.createDayTimeIntervalType(0, 2).typeName()).toBe("interval day to minute");
  expect(DataTypes.createDayTimeIntervalType(0, 3).typeName()).toBe("interval day to second");
  expect(DataTypes.createDayTimeIntervalType(1, 2).typeName()).toBe("interval hour to minute");
  expect(DataTypes.createDayTimeIntervalType(1, 3).typeName()).toBe("interval hour to second");
  expect(DataTypes.createDayTimeIntervalType(2, 3).typeName()).toBe("interval minute to second");
  expect(() => DataTypes.createDayTimeIntervalType(3, 2)).toThrow("startField must be less than or equal to endField");
  expect(() => DataTypes.createDayTimeIntervalType(4, 4).typeName()).toThrow("Invalid field value");
});

test("YearMonthIntervalType", () => {
  const yearMonthIntervalType = DataTypes.createYearMonthIntervalType();
  expect(yearMonthIntervalType.typeName()).toBe("interval year to month");
  expect(yearMonthIntervalType.simpleString()).toBe("interval year to month");
  expect(yearMonthIntervalType.catalogString()).toBe("interval year to month");
  expect(yearMonthIntervalType.defaultSize()).toBe(4);
  expect(yearMonthIntervalType.sql()).toBe("INTERVAL YEAR TO MONTH");

  expect(DataTypes.createYearMonthIntervalType(0, 0).typeName()).toBe("interval year");
  expect(DataTypes.createYearMonthIntervalType(1, 1).typeName()).toBe("interval month");
  expect(DataTypes.createYearMonthIntervalType(0, 1).typeName()).toBe("interval year to month");

  expect(() => DataTypes.createYearMonthIntervalType(1, 0)).toThrow("startField must be less than or equal to endField");
  expect(() => DataTypes.createYearMonthIntervalType(2, 2).typeName()).toThrow("Invalid field value");
});

test("ArrayType", () => {
  const arrayType = DataTypes.createArrayType(DataTypes.IntegerType, false);
  expect(arrayType.elementType).toBe(DataTypes.IntegerType);
  expect(arrayType.containsNull).toBe(false);
  expect(arrayType.typeName()).toBe("array");
  expect(arrayType.simpleString()).toBe("array<int>");
  expect(arrayType.catalogString()).toBe("array<int>");
  expect(arrayType.defaultSize()).toBe(4);
  expect(arrayType.sql()).toBe("ARRAY<INT>");
  expect(arrayType.toString()).toBe("ArrayType(IntegerType, false)");

  const arrayType2 = DataTypes.createArrayType(DataTypes.IntegerType, true);
  expect(arrayType2 === arrayType).toBe(false);
});

test("MapType", () => {
  const mapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false);
  expect(mapType.keyType).toBe(DataTypes.StringType);
  expect(mapType.valueType).toBe(DataTypes.IntegerType);
  expect(mapType.valueContainsNull).toBe(false);
  expect(mapType.typeName()).toBe("map");
  expect(mapType.simpleString()).toBe("map<string,int>");
  expect(mapType.catalogString()).toBe("map<string,int>");
  expect(mapType.defaultSize()).toBe(20 + 4);
  expect(mapType.sql()).toBe("MAP<STRING,INT>");
  expect(mapType.toString()).toBe("MapType(StringType, IntegerType, false)");

  const mapType2 = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true);
  expect(mapType2 === mapType).toBe(false);
});

test("StructField", () => {
  const structField = DataTypes.createStructField("f1", DataTypes.IntegerType, false);
  expect(structField.name).toBe("f1");
  expect(structField.dataType).toBe(DataTypes.IntegerType);
  expect(structField.nullable).toBe(false);
  expect(structField.metadata).toBe(Metadata.empty());
  expect(structField.toString()).toBe("StructField(f1,IntegerType,false)");
  expect(structField.getComment()).toBe(undefined);
  expect(structField.sql()).toBe("f1: INT NOT NULL");

  const structField2 = DataTypes.createStructField("f1", DataTypes.IntegerType, false);
  expect(structField2).toEqual(structField);
});

test("StructType", () => {
  const structField1 = DataTypes.createStructField("f1", DataTypes.IntegerType, false);
  const structField2 = DataTypes.createStructField("f2", DataTypes.StringType, true);
  const structType = DataTypes.createStructType([structField1, structField2]);
  expect(structType.fields).toEqual([structField1, structField2]);
  expect(structType.defaultSize()).toBe(24);
  expect(structType.simpleString()).toBe("struct<f1:int, f2:string>");
  expect(structType.catalogString()).toBe("struct<f1:int, f2:string>");
  expect(structType.sql()).toBe("STRUCT<f1: INT NOT NULL, f2: STRING>");
  expect(structType.fieldNames()).toEqual(["f1", "f2"]);
  expect(structType.names()).toEqual(["f1", "f2"]);
  expect(structType.toString()).toBe("StructType(StructField(f1,IntegerType,false), StructField(f2,StringType,true))");

  const structType2 = DataTypes.createStructType([structField1, structField2]);
  expect(structType2).toEqual(structType);
});

test("UnparsedDataType", () => {

  const unparsedType = DataTypes.createUnparsedType("foo");
  expect(unparsedType.typeName()).toBe("unparsed(foo)");
  expect(unparsedType.simpleString()).toBe("unparsed(foo)");
  expect(unparsedType.catalogString()).toBe("unparsed(foo)");
  expect(unparsedType.defaultSize()).toBe(0);
  expect(unparsedType.sql()).toBe("UNPARSED(FOO)");
  expect(unparsedType.toString()).toBe("UnparsedDataType(foo)");
});