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

import { DataType as ArrowDataType, DateUnit, Field, IntervalUnit, List, Map_, Precision, Schema, Struct, TimeUnit, Type, TypeMap } from 'apache-arrow';
import * as t from "../../../../../gen/spark/connect/types_pb";
import { ARROW_BINARY, ARROW_BOOL, ARROW_DATE, ARROW_FLOAT32, ARROW_FLOAT64, ARROW_INT16, ARROW_INT32, ARROW_INT64, ARROW_INT8, ARROW_INTERVAL, ARROW_INTERVAL_DT, ARROW_INTERVAL_YM, ARROW_NULL, ARROW_TIMESTAMP, ARROW_TIMESTAMP_NTZ, ARROW_UTF8, createArrowDecimal } from '../arrow/types';
import { SparkUnsupportedOperationException } from '../errors';
import { ArrayType } from './ArrayType';
import { BinaryType } from './BinaryType';
import { BooleanType } from './BooleanType';
import { ByteType } from './ByteType';
import { CalendarIntervalType } from './CalendarIntervalType';
import { CharType } from './CharType';
import { DataType } from './data_types';
import { DateType } from './DateType';
import { DayTimeIntervalType } from './DayTimeIntervalType';
import { DecimalType } from './DecimalType';
import { DoubleType } from './DoubleType';
import { FloatType } from './FloatType';
import { IntegerType } from './IntegerType';
import { LongType } from './LongType';
import { MapType } from './MapType';
import { Metadata, MetadataBuilder } from './metadata';
import { NullType } from './NullType';
import { ShortType } from './ShortType';
import { StringType } from './StringType';
import { StructField } from './StructField';
import { StructType } from './StructType';
import { TimestampNTZType } from './TimestampNTZType';
import { TimestampType } from './TimestampType';
import { UnparsedDataType } from './UnparsedDataType';
import { VarcharType } from './VarcharType';
import { VariantType } from './VariantType';
import { YearMonthIntervalType } from './YearMonthIntervalType';
import { createProtoArray, createProtoChar, createProtoDayTimeInterval, createProtoDecimal, createProtoMap, createProtoString, createProtoStruct, createProtoStructField, createProtoUnparsed, createProtoVarchar, createProtoYearMonthInterval, PROTO_BINARY, PROTO_BOOLEAN, PROTO_BYTE, PROTO_CALANDAR_INTERVAL, PROTO_DATE, PROTO_DOUBLE, PROTO_FLOAT, PROTO_INT, PROTO_LONG, PROTO_NULL, PROTO_SHORT, PROTO_TIMESTAMP, PROTO_TIMESTAMP_NTZ, PROTO_VARIANT } from '../proto/types';

/**
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 *
 * @since 1.0.0
 * @author Kent Yao <yao@apache.org>
 */
export class DataTypes {
  static readonly NullType = NullType.INSTANCE;

  static readonly BooleanType = BooleanType.INSTANCE;

  static readonly ByteType = ByteType.INSTANCE;
  static readonly ShortType = ShortType.INSTANCE;
  static readonly IntegerType = IntegerType.INSTANCE;
  static readonly LongType = LongType.INSTANCE;

  static readonly FloatType = FloatType.INSTANCE;
  static readonly DoubleType = DoubleType.INSTANCE;
  static createDecimalType(precision?: number, scale?: number): DecimalType {
    if (!precision) {
      precision = DecimalType.USER_DEFAULT.precision;
    }
    if (!scale) {
      scale = DecimalType.USER_DEFAULT.scale;
    }
    if (precision === DecimalType.USER_DEFAULT.precision && scale === DecimalType.USER_DEFAULT.scale) {
      return DecimalType.USER_DEFAULT;
    } else if (precision === DecimalType.MAX_PRECISION && scale === DecimalType.DEFAULT_SCALE) {
      return DecimalType.SYSTEM_DEFAULT;
    } else {
      return new DecimalType(precision, scale);
    }
  }

  static readonly StringType = StringType.INSTANCE;
  static readonly BinaryType = BinaryType.INSTANCE;
  static createStringType(collation?: string): StringType {
    if (!collation || this.StringType.collation === collation.toUpperCase()) {
      return this.StringType;
    }
    return new StringType(collation);
  }
  static createCharType(length: number): CharType {
    return new CharType(length);
  }
  static createVarCharType(length: number): VarcharType {
    return new VarcharType(length);
  }
  static readonly VariantType = VariantType.INSTANCE;

  static readonly DateType = DateType.INSTANCE;
  static readonly TimestampType = TimestampType.INSTANCE;
  static readonly TimestampNTZType = TimestampNTZType.INSTANCE;

  static readonly CalendarIntervalType = CalendarIntervalType.INSTANCE;
  static createDayTimeIntervalType(
      startField: number = DayTimeIntervalType.DAY,
      endField: number = DayTimeIntervalType.SECOND): DayTimeIntervalType {
    if (startField === DayTimeIntervalType.DAY && endField === DayTimeIntervalType.SECOND) {
      return DayTimeIntervalType.DEFAULT;
    }
    return new DayTimeIntervalType(startField, endField);
  }
  static createYearMonthIntervalType(
      startField: number = YearMonthIntervalType.YEAR,
      endField: number = YearMonthIntervalType.MONTH): YearMonthIntervalType {
    if (startField === YearMonthIntervalType.YEAR && endField === YearMonthIntervalType.MONTH) {
      return YearMonthIntervalType.DEFAULT;
    }
    return new YearMonthIntervalType(startField, endField);
  }

  static createArrayType(elementType: DataType, containsNull: boolean = true): ArrayType {
    return new ArrayType(elementType, containsNull);
  }

  static createMapType(keyType: DataType, valueType: DataType, valueContainsNull: boolean = true): MapType {
    return new MapType(keyType, valueType, valueContainsNull);
  }
  
  static createStructField(
      name: string,
      dataType: DataType,
      nullable: boolean,
      metadata: Metadata = Metadata.empty()): StructField {
    return new StructField(name, dataType, nullable, metadata);
  }

  static createStructType(fields: StructField[]): StructType {
    return new StructType(fields);
  }

  static createUnparsedType(typeName: string): UnparsedDataType {
    return new UnparsedDataType(typeName);
  }

  static fromProtoType(proto: t.DataType): DataType {
    switch(proto.kind.case) {
      case "null":
        return DataTypes.NullType;
      case "boolean":
        return DataTypes.BooleanType;
      case "byte":
        return DataTypes.ByteType;
      case "short":
        return DataTypes.ShortType;
      case "integer":
        return DataTypes.IntegerType;
      case "long":
        return DataTypes.LongType;
      case "float":
        return DataTypes.FloatType;
      case "double":
        return DataTypes.DoubleType;
      case "decimal":
        return DataTypes.createDecimalType(proto.kind.value.precision, proto.kind.value.scale);
      case "string":
        return DataTypes.createStringType(proto.kind.value.collation);
      case "binary":
        return DataTypes.BinaryType;
      case "date":
        return DataTypes.DateType;
      case "timestamp":
        return DataTypes.TimestampType;
      case "timestampNtz":
        return DataTypes.TimestampNTZType;
      case "variant":
        return DataTypes.VariantType;
      case "calendarInterval":
        return DataTypes.CalendarIntervalType;
      case "dayTimeInterval":
        return DataTypes.createDayTimeIntervalType(proto.kind.value.startField, proto.kind.value.endField);
      case "yearMonthInterval":
        return DataTypes.createYearMonthIntervalType(proto.kind.value.startField, proto.kind.value.endField);
      case "char":
        return DataTypes.createCharType(proto.kind.value.length);
      case "varChar":
        return DataTypes.createVarCharType(proto.kind.value.length);
      case "array":
        if (!proto.kind.value.elementType) {
          return DataTypes.createArrayType(DataTypes.NullType);
        } else {
          const et = this.fromProtoType(proto.kind.value.elementType);
          const containsNull = proto.kind.value.containsNull;
          return DataTypes.createArrayType(et, containsNull);
        }
      case "map":
        const kt = proto.kind.value.keyType ? this.fromProtoType(proto.kind.value.keyType) : DataTypes.NullType;
        const vt = proto.kind.value.valueType ? this.fromProtoType(proto.kind.value.valueType) : DataTypes.NullType;
        const containsNull = proto.kind.value.valueContainsNull
        return DataTypes.createMapType(kt, vt, containsNull);
      case "struct":
        const fields = proto.kind.value.fields.map(f => StructField.fromProto(f));
        return DataTypes.createStructType(fields);
      case "unparsed":
        return DataTypes.createUnparsedType(proto.kind.value.dataTypeString);
      case "udt":
      default: 
        throw new SparkUnsupportedOperationException(
          "UNSUPPORTED_DATATYPE",
          `Unsupported data type:  ${proto.kind.case}}`,
          "0A000");
    }
  };

  static toProtoType(dt: DataType): t.DataType {
    if (dt instanceof NullType) {
      return PROTO_NULL;
    } else if (dt instanceof BooleanType) {
      return PROTO_BOOLEAN;
    } else if (dt instanceof ByteType) {
      return PROTO_BYTE;
    } else if (dt instanceof ShortType) {
      return PROTO_SHORT;
    } else if (dt instanceof IntegerType) {
      return PROTO_INT;
    } else if (dt instanceof LongType) {
      return PROTO_LONG;
    } else if (dt instanceof FloatType) {
      return PROTO_FLOAT;
    } else if (dt instanceof DoubleType) {
      return PROTO_DOUBLE;
    } else if (dt instanceof DecimalType) {
      return createProtoDecimal(dt.precision, dt.scale);
    } else if (dt instanceof CharType) {
      return createProtoChar(dt.length);
    } else if (dt instanceof VarcharType) {
      return createProtoVarchar(dt.length);
    } else if (dt instanceof StringType) {
      return createProtoString(dt.collation);
    } else if (dt instanceof BinaryType) {
      return PROTO_BINARY;
    } else if (dt instanceof DateType) {
      return PROTO_DATE;
    } else if (dt instanceof TimestampType) {
      return PROTO_TIMESTAMP;
    } else if (dt instanceof TimestampNTZType) {
      return PROTO_TIMESTAMP_NTZ;
    } else if (dt instanceof CalendarIntervalType) {
      return PROTO_CALANDAR_INTERVAL;
    } else if (dt instanceof DayTimeIntervalType) {
      return createProtoDayTimeInterval(dt.startField, dt.endField);
    } else if (dt instanceof YearMonthIntervalType) {
      return createProtoYearMonthInterval(dt.startField, dt.endField);
    } else if (dt instanceof VariantType) {
      return PROTO_VARIANT;
    } else if (dt instanceof ArrayType) {
      return createProtoArray(this.toProtoType(dt.elementType), dt.containsNull);
    } else if (dt instanceof MapType) {
      return createProtoMap(this.toProtoType(dt.keyType), this.toProtoType(dt.valueType), dt.valueContainsNull);
    } else if (dt instanceof StructType) {
      const fields = dt.fields.map(f => createProtoStructField(f.name, this.toProtoType(f.dataType), f.nullable, f.metadata.json()));
      return createProtoStruct(fields);
    } else if (dt instanceof UnparsedDataType) {
      return createProtoUnparsed(dt.dataTypeString);
    } else {
      throw new SparkUnsupportedOperationException(
        "UNSUPPORTED_DATATYPE",
        `Unsupported data type: ${dt}`,
        "0A000");
    }
  };

  static fromArrowType(dt: Type): DataType {
    if (ArrowDataType.isBool(dt)) {
      return DataTypes.BooleanType;
    } else if (ArrowDataType.isInt(dt) && dt.isSigned && dt.bitWidth === 8) {
      return DataTypes.ByteType;
    } else if (ArrowDataType.isInt(dt) && dt.isSigned && dt.bitWidth === 16) {
      return DataTypes.ShortType;
    } else if (ArrowDataType.isInt(dt) && dt.isSigned && dt.bitWidth === 32) {
      return DataTypes.IntegerType;
    } else if (ArrowDataType.isInt(dt) && dt.isSigned && dt.bitWidth === 64) {
      return DataTypes.LongType;
    } else if (ArrowDataType.isFloat(dt) && dt.precision === Precision.SINGLE) {
      return DataTypes.FloatType;
    } else if (ArrowDataType.isFloat(dt) && dt.precision === Precision.DOUBLE) {
      return DataTypes.DoubleType;
    }  else if (ArrowDataType.isDecimal(dt)) {
      return DataTypes.createDecimalType(dt.precision, dt.scale);
    } else if (ArrowDataType.isUtf8(dt) || ArrowDataType.isLargeUtf8(dt)) {
      return DataTypes.StringType;
    } else if (ArrowDataType.isBinary(dt) || ArrowDataType.isLargeBinary(dt)) {
      return DataTypes.BinaryType;
    } else if (ArrowDataType.isDate(dt) && dt.unit === DateUnit.DAY) {
      return DataTypes.DateType;
    } else if (ArrowDataType.isTimestamp(dt) && dt.unit === TimeUnit.MICROSECOND && !dt.timezone) {
      return DataTypes.TimestampNTZType;
    } else if (ArrowDataType.isTimestamp(dt) && dt.unit === TimeUnit.MICROSECOND) {
      return DataTypes.TimestampType;
    } else if (ArrowDataType.isDuration(dt) && dt.unit === TimeUnit.MICROSECOND) {
      return DataTypes.createDayTimeIntervalType(); // default
    } else if (ArrowDataType.isInterval(dt) && dt.unit === IntervalUnit.YEAR_MONTH) {
      return DataTypes.createYearMonthIntervalType(); // default
    } else if (ArrowDataType.isInterval(dt) && dt.unit === IntervalUnit.MONTH_DAY_NANO) {
      return DataTypes.CalendarIntervalType;
    } else if (ArrowDataType.isNull(dt)) {
      return DataTypes.NullType;
    } else {
      throw new SparkUnsupportedOperationException(
        "UNSUPPORTED_ARROWTYPE",
        `Unsupported arrow type: ${dt}`,
        "0A000");
    }
  };

  private static isVariant(field: Field): boolean {
    if (ArrowDataType.isStruct(field.type)) {
      const children = field.type.children;
      if (children.length === 2) {
        if (children[0].name === 'metadata' && children[1].name === 'value') {
          return children[0].metadata.get('variant') === 'true';
        } else if (children[1].name === 'metadata' && children[0].name === 'value') {
          return children[1].metadata.get('variant') === 'true';
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  static fromArrowField(field: Field): DataType {
    if (ArrowDataType.isList(field.type)) {
      const et = DataTypes.fromArrowField(field.type.children[0]);
      return DataTypes.createArrayType(et, field.type.children[0].nullable);
    } else if (ArrowDataType.isDictionary(field.type)) {
      return DataTypes.fromArrowType(field.type.valueType);
    } else if (ArrowDataType.isMap(field.type)) {
      const entries = field.type.children[0];
      const keyType = DataTypes.fromArrowField(entries.type.children[0]);
      const valueType = DataTypes.fromArrowField(entries.type.children[1]);
      return DataTypes.createMapType(keyType, valueType, entries.type.children[1].nullable);
    } else if (this.isVariant(field)) {
      return DataTypes.VariantType;
    } else if (ArrowDataType.isStruct(field.type)) {
      const fields = field.type.children.map(f => {
        const metadata = new MetadataBuilder().putAll(f.metadata).build();
        return DataTypes.createStructField(f.name, DataTypes.fromArrowField(f), f.nullable, metadata);
      });
      return DataTypes.createStructType(fields);
    } else {
      return DataTypes.fromArrowType(field.type);
    }
  };

  /**
   * Convert an Arrow Schema to Spark Schema
   * @param schema an Arrow Schema
   * @returns a Spark StructType
   * @throws {SparkUnsupportedOperationException} if the data type is not supported
   */
  static fromArrowSchema(schema: Schema): StructType {
    const fields = schema.fields.map(f => {
      const metadata = new MetadataBuilder().putAll(f.metadata).build();
      return DataTypes.createStructField(f.name, DataTypes.fromArrowField(f), f.nullable, metadata);
    });
    return DataTypes.createStructType(fields);
  };

  /**
   * Convert a Spark DataType to Arrow DataType
   * @param dt a Spark DataType
   * @returns an Arrow DataType
   * @throws {SparkUnsupportedOperationException} if the data type is not supported
   */
  static toArrowType(dt: DataType): ArrowDataType {
    if (dt instanceof BooleanType) {
      return ARROW_BOOL;
    } else if (dt instanceof ByteType) {
      return ARROW_INT8;
    } else if (dt instanceof ShortType) {
      return ARROW_INT16;
    } else if (dt instanceof IntegerType) {
      return ARROW_INT32;
    } else if (dt instanceof LongType) {
      return ARROW_INT64;
    } else if (dt instanceof FloatType) {
      return ARROW_FLOAT32;
    } else if (dt instanceof DoubleType) {
      return ARROW_FLOAT64;
    } else if (dt instanceof DecimalType) {
      return createArrowDecimal(dt.precision, dt.scale);
    } else if (dt instanceof StringType || dt instanceof CharType || dt instanceof VarcharType) {
      return ARROW_UTF8; // TODO, support LargeUtf8
    } else if (dt instanceof BinaryType) {
      return ARROW_BINARY; // TODO, support LargeBinary
    } else if (dt instanceof DateType) {
      return ARROW_DATE;
    } else if (dt instanceof TimestampType) {
      return ARROW_TIMESTAMP;
    } else if (dt instanceof TimestampNTZType) {
      return ARROW_TIMESTAMP_NTZ;
    } else if (dt instanceof CalendarIntervalType) {
      return ARROW_INTERVAL;
    } else if (dt instanceof DayTimeIntervalType) {
      return ARROW_INTERVAL_DT
    } else if (dt instanceof YearMonthIntervalType) {
      return ARROW_INTERVAL_YM;
    } else if (dt instanceof ArrayType) {
      return new List(new Field('element', DataTypes.toArrowType(dt.elementType), dt.containsNull));
    } else if (dt instanceof MapType) {
      const kv = new Struct(
        [
          new Field('key', DataTypes.toArrowType(dt.keyType), false),
          new Field('value', DataTypes.toArrowType(dt.valueType), dt.valueContainsNull)
        ]
      );
      return new Map_(new Field('entries', kv, false));
    } else if (dt instanceof StructType) {
      // this.toArrowSchema(dt).fields.forEach(f => {
      //   console.log(f);
      // });
      return new Struct(this.toArrowSchema(dt).fields);
    } else if (dt instanceof VariantType) {
      return new Struct(
        [
          new Field('metadata', ARROW_BINARY, false, new Map<string, string>().set('variant', 'true')),
          new Field('value', ARROW_BINARY, true)
        ]
      );
    } else if (dt instanceof NullType) {
      return ARROW_NULL;
    } else {
      throw new SparkUnsupportedOperationException(
        "UNSUPPORTED_DATATYPE",
        `Unsupported data type: ${dt}`,
        "0A000");
    }
  };

  /**
   * Convert a Spark Schema to Arrow Schema
   * @param schema a Spark StructType
   * @returns an Arrow Schema
   * @throws {SparkUnsupportedOperationException} if the data type is not supported
   */
  static toArrowSchema<T extends TypeMap>(schema: StructType): Schema<T> {
    const fields = schema.fields.map(f => {
      return new Field<T[keyof T]>(f.name, this.toArrowType(f.dataType) as T[keyof T], f.nullable, f.metadataMap);
    });
    return new Schema<T>(fields);
  }
};
