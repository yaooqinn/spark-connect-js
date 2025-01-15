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


import { DataType as ArrowDataType } from "apache-arrow";
import { createProtoArray, createProtoChar, createProtoDayTimeInterval, createProtoDecimal, createProtoMap, createProtoString, createProtoStruct, createProtoStructField, createProtoUdt, createProtoUnparsed, createProtoVarchar, createProtoYearMonthInterval, PROTO_BINARY, PROTO_BOOLEAN, PROTO_BYTE, PROTO_CALANDAR_INTERVAL, PROTO_DATE, PROTO_DOUBLE, PROTO_FLOAT, PROTO_INT, PROTO_LONG, PROTO_NULL, PROTO_SHORT, PROTO_STRING, PROTO_TIMESTAMP, PROTO_TIMESTAMP_NTZ, PROTO_VARIANT } from "../../../../../../src/org/apache/spark/sql/proto/types";
import { BinaryType } from "../../../../../../src/org/apache/spark/sql/types/BinaryType";
import { BooleanType } from "../../../../../../src/org/apache/spark/sql/types/BooleanType";
import { ByteType } from "../../../../../../src/org/apache/spark/sql/types/ByteType";
import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types/DataTypes";
import { DateType } from "../../../../../../src/org/apache/spark/sql/types/DateType";
import { DayTimeIntervalType } from "../../../../../../src/org/apache/spark/sql/types/DayTimeIntervalType";
import { DecimalType } from "../../../../../../src/org/apache/spark/sql/types/DecimalType";
import { DoubleType } from "../../../../../../src/org/apache/spark/sql/types/DoubleType";
import { FloatType } from "../../../../../../src/org/apache/spark/sql/types/FloatType";
import { IntegerType } from "../../../../../../src/org/apache/spark/sql/types/IntegerType";
import { LongType } from "../../../../../../src/org/apache/spark/sql/types/LongType";
import { Metadata, MetadataBuilder } from "../../../../../../src/org/apache/spark/sql/types/metadata";
import { NullType } from "../../../../../../src/org/apache/spark/sql/types/NullType";
import { ShortType } from "../../../../../../src/org/apache/spark/sql/types/ShortType";
import { StringType } from "../../../../../../src/org/apache/spark/sql/types/StringType";
import { TimestampNTZType } from "../../../../../../src/org/apache/spark/sql/types/TimestampNTZType";
import { TimestampType } from "../../../../../../src/org/apache/spark/sql/types/TimestampType";
import { VariantType } from "../../../../../../src/org/apache/spark/sql/types/VariantType";
import { DataType } from "../../../../../../src/org/apache/spark/sql/types";

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

test("fromProto", () => {
  expect(DataTypes.fromProtoType(PROTO_NULL)).toBe(DataTypes.NullType)
  expect(DataTypes.fromProtoType(PROTO_BINARY)).toBe(DataTypes.BinaryType)
  expect(DataTypes.fromProtoType(PROTO_BOOLEAN)).toBe(DataTypes.BooleanType)
  expect(DataTypes.fromProtoType(PROTO_BYTE)).toBe(DataTypes.ByteType)
  expect(DataTypes.fromProtoType(PROTO_CALANDAR_INTERVAL)).toEqual(DataTypes.CalendarIntervalType)
  expect(DataTypes.fromProtoType(PROTO_DATE)).toBe(DataTypes.DateType)
  expect(DataTypes.fromProtoType(PROTO_DOUBLE)).toBe(DataTypes.DoubleType)
  expect(DataTypes.fromProtoType(PROTO_FLOAT)).toBe(DataTypes.FloatType)
  expect(DataTypes.fromProtoType(PROTO_INT)).toBe(DataTypes.IntegerType)
  expect(DataTypes.fromProtoType(PROTO_LONG)).toBe(DataTypes.LongType)
  expect(DataTypes.fromProtoType(PROTO_SHORT)).toBe(DataTypes.ShortType)
  expect(DataTypes.fromProtoType(PROTO_TIMESTAMP)).toBe(DataTypes.TimestampType)
  expect(DataTypes.fromProtoType(PROTO_TIMESTAMP_NTZ)).toBe(DataTypes.TimestampNTZType)
  expect(DataTypes.fromProtoType(PROTO_VARIANT)).toBe(DataTypes.VariantType)
  expect(DataTypes.fromProtoType(createProtoDecimal(10, 1))).toEqual(DataTypes.createDecimalType(10, 1))
  expect(DataTypes.fromProtoType(createProtoDecimal(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE))).toBe(DecimalType.SYSTEM_DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDecimal(10))).toBe(DecimalType.USER_DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDecimal())).toBe(DecimalType.USER_DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDecimal(undefined, 0))).toBe(DecimalType.USER_DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDecimal(10, 0))).toBe(DecimalType.USER_DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDayTimeInterval(0, 3))).toBe(DayTimeIntervalType.DEFAULT)
  expect(DataTypes.fromProtoType(createProtoDayTimeInterval(0, 2))).toEqual(DataTypes.createDayTimeIntervalType(0, 2))
  expect(DataTypes.fromProtoType(createProtoDayTimeInterval(1, 3))).toEqual(DataTypes.createDayTimeIntervalType(1, 3))
  expect(DataTypes.fromProtoType(createProtoYearMonthInterval(0, 1))).toEqual(DataTypes.createYearMonthIntervalType(0, 1))
  expect(DataTypes.fromProtoType(createProtoYearMonthInterval(1, 1))).toEqual(DataTypes.createYearMonthIntervalType(1, 1))
  expect(DataTypes.fromProtoType(createProtoYearMonthInterval(0, 0))).toEqual(DataTypes.createYearMonthIntervalType(0, 0))
  expect(DataTypes.fromProtoType(createProtoChar(10))).toEqual(DataTypes.createCharType(10))
  expect(DataTypes.fromProtoType(createProtoChar(20))).toEqual(DataTypes.createCharType(20))
  expect(DataTypes.fromProtoType(createProtoVarchar(10))).toEqual(DataTypes.createVarCharType(10))
  expect(DataTypes.fromProtoType(createProtoVarchar(20))).toEqual(DataTypes.createVarCharType(20))
  expect(DataTypes.fromProtoType(createProtoString())).toBe(DataTypes.StringType)
  expect(DataTypes.fromProtoType(createProtoString("af"))).toEqual(new StringType("af"))
  expect(DataTypes.fromProtoType(createProtoString("UTF8_BINARY"))).toBe(DataTypes.StringType)
  expect(DataTypes.fromProtoType(createProtoArray(undefined))).toEqual(DataTypes.createArrayType(DataTypes.NullType, true))
  expect(DataTypes.fromProtoType(createProtoArray(PROTO_INT, false))).toEqual(DataTypes.createArrayType(DataTypes.IntegerType, false))
  expect(DataTypes.fromProtoType(createProtoArray(PROTO_INT, true))).toEqual(DataTypes.createArrayType(DataTypes.IntegerType, true))
  expect(DataTypes.fromProtoType(createProtoArray(PROTO_LONG, false))).toEqual(DataTypes.createArrayType(DataTypes.LongType, false))
  expect(DataTypes.fromProtoType(createProtoArray(PROTO_LONG, true))).toEqual(DataTypes.createArrayType(DataTypes.LongType, true))
  expect(DataTypes.fromProtoType(createProtoMap(PROTO_STRING, PROTO_INT, false))).toEqual(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false))
  expect(DataTypes.fromProtoType(createProtoMap(PROTO_STRING, PROTO_INT, true))).toEqual(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true))
  expect(DataTypes.fromProtoType(createProtoMap(PROTO_LONG, PROTO_INT, false))).toEqual(DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, false))
  expect(DataTypes.fromProtoType(createProtoMap(PROTO_LONG, PROTO_INT, true))).toEqual(DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, true))
  const f1 = createProtoStructField("f1", PROTO_INT, false);
  const f2 = createProtoStructField("f2", PROTO_STRING, true);
  expect(DataTypes.fromProtoType(createProtoStruct([f1, f2]))).toEqual(DataTypes.createStructType([
    DataTypes.createStructField("f1", DataTypes.IntegerType, false),
    DataTypes.createStructField("f2", DataTypes.StringType, true)]));
  expect(DataTypes.fromProtoType(createProtoUnparsed("foo"))).toEqual(DataTypes.createUnparsedType("foo"));
  expect(() => DataTypes.fromProtoType(createProtoUdt("MyUDT"))).toThrow("UNSUPPORTED_DATATYPE");
});

test("toProto", () => {
  expect(DataTypes.toProtoType(DataTypes.NullType)).toBe(PROTO_NULL)
  expect(DataTypes.toProtoType(DataTypes.BinaryType)).toBe(PROTO_BINARY)
  expect(DataTypes.toProtoType(DataTypes.BooleanType)).toBe(PROTO_BOOLEAN)
  expect(DataTypes.toProtoType(DataTypes.ByteType)).toBe(PROTO_BYTE)
  expect(DataTypes.toProtoType(DataTypes.CalendarIntervalType)).toBe(PROTO_CALANDAR_INTERVAL)
  expect(DataTypes.toProtoType(DataTypes.DateType)).toBe(PROTO_DATE)
  expect(DataTypes.toProtoType(DataTypes.DoubleType)).toBe(PROTO_DOUBLE)
  expect(DataTypes.toProtoType(DataTypes.FloatType)).toBe(PROTO_FLOAT)
  expect(DataTypes.toProtoType(DataTypes.IntegerType)).toBe(PROTO_INT)
  expect(DataTypes.toProtoType(DataTypes.LongType)).toBe(PROTO_LONG)
  expect(DataTypes.toProtoType(DataTypes.ShortType)).toBe(PROTO_SHORT)
  expect(DataTypes.toProtoType(DataTypes.TimestampType)).toBe(PROTO_TIMESTAMP)
  expect(DataTypes.toProtoType(DataTypes.TimestampNTZType)).toBe(PROTO_TIMESTAMP_NTZ)
  expect(DataTypes.toProtoType(DataTypes.VariantType)).toBe(PROTO_VARIANT)
  expect(DataTypes.toProtoType(DecimalType.USER_DEFAULT)).toEqual(createProtoDecimal(10, 0))
  expect(DataTypes.toProtoType(DecimalType.SYSTEM_DEFAULT)).toEqual(createProtoDecimal(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE))
  expect(DataTypes.toProtoType(DataTypes.createDecimalType(10, 1))).toEqual(createProtoDecimal(10, 1))
  expect(DataTypes.toProtoType(DayTimeIntervalType.DEFAULT)).toEqual(createProtoDayTimeInterval(0, 3))
  expect(DataTypes.toProtoType(DataTypes.createDayTimeIntervalType(0, 2))).toEqual(createProtoDayTimeInterval(0, 2))
  expect(DataTypes.toProtoType(DataTypes.createDayTimeIntervalType(1, 3))).toEqual(createProtoDayTimeInterval(1, 3)
  )
  expect(DataTypes.toProtoType(DataTypes.createYearMonthIntervalType(0, 1))).toEqual(createProtoYearMonthInterval(0, 1))
  expect(DataTypes.toProtoType(DataTypes.createYearMonthIntervalType(1, 1))).toEqual(createProtoYearMonthInterval(1, 1))
  expect(DataTypes.toProtoType(DataTypes.createYearMonthIntervalType(0, 0))).toEqual(createProtoYearMonthInterval(0, 0))
  expect(DataTypes.toProtoType(DataTypes.createCharType(10))).toEqual(createProtoChar(10))
  expect(DataTypes.toProtoType(DataTypes.createCharType(20))).toEqual(createProtoChar(20))
  expect(DataTypes.toProtoType(DataTypes.createVarCharType(10))).toEqual(createProtoVarchar(10))
  expect(DataTypes.toProtoType(DataTypes.createVarCharType(20))).toEqual(createProtoVarchar(20))
  expect(DataTypes.toProtoType(DataTypes.StringType)).toBe(PROTO_STRING)
  expect(DataTypes.toProtoType(new StringType("af"))).toEqual(createProtoString("af"))
  expect(DataTypes.toProtoType(DataTypes.createArrayType(DataTypes.NullType, true))).toEqual(createProtoArray(PROTO_NULL))
  expect(DataTypes.toProtoType(DataTypes.createArrayType(DataTypes.IntegerType, false))).toEqual(createProtoArray(PROTO_INT, false))
  expect(DataTypes.toProtoType(DataTypes.createArrayType(DataTypes.IntegerType, true))).toEqual(createProtoArray(PROTO_INT, true))
  expect(DataTypes.toProtoType(DataTypes.createArrayType(DataTypes.LongType, false))).toEqual(createProtoArray(PROTO_LONG, false))
  expect(DataTypes.toProtoType(DataTypes.createArrayType(DataTypes.LongType, true))).toEqual(createProtoArray(PROTO_LONG, true))
  expect(DataTypes.toProtoType(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false))).toEqual(createProtoMap(PROTO_STRING, PROTO_INT, false))
  expect(DataTypes.toProtoType(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true))).toEqual(createProtoMap(PROTO_STRING, PROTO_INT, true))
  expect(DataTypes.toProtoType(DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, false))).toEqual(createProtoMap(PROTO_LONG, PROTO_INT, false))
  expect(DataTypes.toProtoType(DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, true))).toEqual(createProtoMap(PROTO_LONG, PROTO_INT, true))
  const f1 = DataTypes.createStructField("f1", DataTypes.BinaryType, false);
  expect(DataTypes.toProtoType(DataTypes.createStructType([f1]))).toEqual(createProtoStruct([createProtoStructField("f1", PROTO_BINARY, false, '{}')]))
  expect(DataTypes.toProtoType(DataTypes.createUnparsedType("foo"))).toEqual(createProtoUnparsed("foo"))
  expect(() => DataTypes.toProtoType(new TestDataType())).toThrow("UNSUPPORTED_DATATYPE");
});

class TestDataType extends DataType {}

test("from / to Arrow", () => {
  const fields =
    [
      DataTypes.createStructField("f0", DataTypes.BinaryType, false),
      DataTypes.createStructField("f1", DataTypes.BooleanType, true),
      DataTypes.createStructField("f2", DataTypes.ByteType, false),
      DataTypes.createStructField("f3", DataTypes.ShortType, true),
      DataTypes.createStructField("f4", DataTypes.IntegerType, false),
      DataTypes.createStructField("f5", DataTypes.LongType, true),
      DataTypes.createStructField("f6", DataTypes.FloatType, false),
      DataTypes.createStructField("f7", DataTypes.DoubleType, true),
      DataTypes.createStructField("f8", DataTypes.StringType, false),
      DataTypes.createStructField("f9", DataTypes.DateType, true),
      DataTypes.createStructField("f10", DataTypes.TimestampType, false),
      DataTypes.createStructField("f11", DataTypes.TimestampNTZType, true),
      DataTypes.createStructField("f12", DataTypes.CalendarIntervalType, false),
      DataTypes.createStructField("f13", DecimalType.SYSTEM_DEFAULT, true),
      DataTypes.createStructField("f14", DataTypes.createArrayType(DataTypes.IntegerType, false), false),
      DataTypes.createStructField("f15", DataTypes.createArrayType(DataTypes.IntegerType, true), true),
      DataTypes.createStructField("f16", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false), false),
      DataTypes.createStructField("f17", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true),
      DataTypes.createStructField("f18", DataTypes.createStructType(
        [
          DataTypes.createStructField("i1", DataTypes.IntegerType, false),
          DataTypes.createStructField("i2", DataTypes.StringType, true, new MetadataBuilder().putString("comment", "nothing here").build())
        ]), false),
      DataTypes.createStructField("f19", DataTypes.createStructType([]), true),
      DataTypes.createStructField("f20", DataTypes.createDayTimeIntervalType(0, 3), false),
      DataTypes.createStructField("f21", DataTypes.createYearMonthIntervalType(0, 1), true),
      DataTypes.createStructField("f22", DataTypes.VariantType, false),
      DataTypes.createStructField("f23", DataTypes.NullType, true),
      DataTypes.createStructField("f24", DataTypes.createDecimalType(10, 1), false),
      DataTypes.createStructField("f25", DataTypes.createDecimalType(10, 2), true),
      DataTypes.createStructField("f26", DataTypes.createCharType(10), false),
      DataTypes.createStructField("f27", DataTypes.createVarCharType(20), true),
    ];

  const schema = DataTypes.createStructType(fields);
  const arrowSchema = DataTypes.toArrowSchema(schema);
  expect(arrowSchema.fields.length).toBe(28);
  const schemaBack = DataTypes.fromArrowSchema(arrowSchema)
  expect(schemaBack.fields.slice(0, 25)).toEqual(schema.fields.slice(0, 25));
  expect(schemaBack.fields[26].dataType).toBe(DataTypes.StringType);
  expect(schemaBack.fields[27].dataType).toBe(DataTypes.StringType);
  // fixed size binary
  expect(() => DataTypes.fromArrowType(15)).toThrow("UNSUPPORTED_ARROWTYPE");
  expect(() => DataTypes.toArrowType(DataTypes.createUnparsedType("foo"))).toThrow("UNSUPPORTED_DATATYPE");
});