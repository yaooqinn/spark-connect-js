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


import { create } from "@bufbuild/protobuf";
import { Expression_Literal, Expression_Literal_ArraySchema, Expression_Literal_CalendarIntervalSchema, Expression_Literal_DecimalSchema, Expression_Literal_MapSchema, Expression_Literal_StructSchema, Expression_LiteralSchema } from "../../../../../../gen/spark/connect/expressions_pb";
import { DataType, DataTypes } from "../../types";

export class LiteralBuilder {
  private literal: Expression_Literal = create(Expression_LiteralSchema, {});

  constructor() { }

  withNull(dt: DataType): LiteralBuilder {
    this.literal.literalType = { case: 'null', value: DataTypes.toProtoType(dt) };
    return this;
  }

  withBoolean(value: boolean): LiteralBuilder {
    this.literal.literalType = { case: 'boolean', value: value };
    return this;
  }

  withByte(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'byte', value: value };
    return this;
  }

  withShort(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'short', value: value };
    return this;
  }

  withInt(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'integer', value: value };
    return this;
  }

  withLong(value: number | bigint): LiteralBuilder {
    this.literal.literalType = { case: 'long', value: typeof value === 'number' ? BigInt(value) : value };
    return this;
  }

  withFloat(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'float', value: value };
    return this;
  }

  withDouble(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'double', value: value };
    return this;
  }

  withDeical(value: string, precision?: number, scale?: number): LiteralBuilder {
    const decimal = create(Expression_Literal_DecimalSchema, { value: value, precision: precision, scale: scale });
    this.literal.literalType = { case: 'decimal', value: decimal };
    return this;
  }

  withString(value: string): LiteralBuilder {
    this.literal.literalType = { case: 'string', value: value };
    return this;
  }

  withBinary(value: Uint8Array): LiteralBuilder {
    this.literal.literalType = { case: 'binary', value: value };
    return this;
  }

  withDate(value: number): LiteralBuilder {
    this.literal.literalType = { case: 'date', value: value };
    return this;
  }

  withTimestamp(value: bigint | number): LiteralBuilder {
    if (typeof value === 'number') {
      value = BigInt(value);
    }
    this.literal.literalType = { case: 'timestamp', value: value };
    return this;
  }

  withTimestampNtz(value: bigint): LiteralBuilder {
    this.literal.literalType = { case: 'timestampNtz', value: value };
    return this;
  }

  withCalendarInterval(months: number, days: number, microseconds: bigint): LiteralBuilder {
    const interval = create(Expression_Literal_CalendarIntervalSchema,
      {
        months: months,
        days: days,
        microseconds: microseconds
      });
    this.literal.literalType = { case: 'calendarInterval', value: interval };
    return this;
  }

  withDayTimeInterval(milliseconds: bigint): LiteralBuilder {
    this.literal.literalType = { case: 'dayTimeInterval', value: milliseconds };
    return this;
  }

  withYearMonthInterval(months: number): LiteralBuilder {
    this.literal.literalType = { case: 'yearMonthInterval', value: months };
    return this;
  }

  withArray(value: Expression_Literal[], elementType?: DataType): LiteralBuilder {
    const et = elementType ? DataTypes.toProtoType(elementType) : undefined;
    const arr = create(Expression_Literal_ArraySchema, { elements: value, elementType: et });
    this.literal.literalType = { case: 'array', value: arr };
    return this;
  }

  withMap(
      keys: Expression_Literal[],
      values: Expression_Literal[],
      keyType?: DataType,
      valueType?: DataType): LiteralBuilder {
    const kt = keyType ? DataTypes.toProtoType(keyType) : undefined;
    const vt = valueType ? DataTypes.toProtoType(valueType) : undefined;
    const map = create(Expression_Literal_MapSchema, { keys: keys, values: values, keyType: kt, valueType: vt });
    this.literal.literalType = { case: 'map', value: map };
    return this;
  }

  withStruct(fields: Expression_Literal[], dataType?: DataType): LiteralBuilder {
    const dt = dataType ? DataTypes.toProtoType(dataType) : undefined;
    const struct = create(Expression_Literal_StructSchema, { elements: fields, structType: dt });
    this.literal.literalType = { case: 'struct', value: struct };
    return this;
  }

  build(): Expression_Literal {
    if (!this.literal.literalType) {
      throw new Error('Literal type is not set');
    }
    return this.literal;
  }
}
