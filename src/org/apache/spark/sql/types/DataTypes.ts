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

import { NullType } from './NullType';
import { BooleanType } from './BooleanType';
import { ByteType } from './ByteType';
import { ShortType } from './ShortType';
import { IntegerType } from './IntegerType';
import { LongType } from './LongType';
import { FloatType } from './FloatType';
import { DoubleType } from './DoubleType';
import { DecimalType } from './DecimalType';
import { StringType } from './StringType';
import { BinaryType } from './BinaryType';
import { VariantType } from './VariantType';
import { DateType } from './DateType';
import { TimestampType } from './TimestampType';
import { TimestampNTZType } from './TimestampNTZType';
import { ArrayType } from './ArrayType';
import { DataType } from './data_types';
import { MapType } from './MapType';
import { CalendarIntervalType } from './CalendarIntervalType';
import { DayTimeIntervalType } from './DayTimeIntervalType';
import { YearMonthIntervalType } from './YearMonthIntervalType';
import { CharType } from './CharType';
import { VarcharType } from './VarcharType';
import { Metadata } from './metadata';
import { StructField } from './StructField';
import { StructType } from './StructType';
import { UnparsedDataType } from './UnparsedDataType';

import * as t from "../../../../../gen/spark/connect/types_pb";

/**
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 *
 * @since 1.0.0
 * @author Kent Yao
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
    if (precision === undefined && scale === undefined) {
      return DecimalType.USER_DEFAULT;
    } else if (precision === DecimalType.MAX_PRECISION && scale === DecimalType.DEFAULT_SCALE) {
      return DecimalType.SYSTEM_DEFAULT;
    } else {
      return new DecimalType(precision, scale);
    }
  }

  static readonly StringType = StringType.INSTANCE;
  static readonly BinaryType = BinaryType.INSTANCE;
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


  static fromProto(proto: t.DataType): DataType {
    switch(proto.kind.case) {
      case undefined: 
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
        return DataTypes.StringType;
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
          const et = this.fromProto(proto.kind.value.elementType);
          const containsNull = proto.kind.value.containsNull;
          return DataTypes.createArrayType(et, containsNull);
        }
      case "map":
        const kt = proto.kind.value.keyType ? this.fromProto(proto.kind.value.keyType) : DataTypes.NullType;
        const vt = proto.kind.value.valueType ? this.fromProto(proto.kind.value.valueType) : DataTypes.NullType;
        const containsNull = proto.kind.value.valueContainsNull
        return DataTypes.createMapType(kt, vt, containsNull);
      case "struct":
        const fields = proto.kind.value.fields.map(f => StructField.fromProto(f));
        return DataTypes.createStructType(fields);
      case "unparsed":
        return DataTypes.createUnparsedType(proto.kind.value.dataTypeString);
      case "udt":
      default: throw new Error(`Unsupported type: ${proto.kind.case}`);
    }
  }
}
