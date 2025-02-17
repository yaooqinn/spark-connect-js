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
import { Expression, Expression_Literal, Expression_SortOrder, Expression_SortOrder_NullOrdering, Expression_SortOrder_SortDirection, Expression_SortOrderSchema, JavaUDF, JavaUDFSchema, PythonUDF, PythonUDFSchema, ScalarScalaUDF, ScalarScalaUDFSchema } from "../../../../../../gen/spark/connect/expressions_pb";
import { Aggregate_GroupingSets, Aggregate_GroupingSetsSchema } from "../../../../../../gen/spark/connect/relations_pb";
import { Column } from "../../Column";
import { DataType, DataTypes } from "../../types";
import { LiteralBuilder } from "./LiteralBuilder";

export const sortOrder = (child: Expression, asc = true, nullsFirst = true): Expression_SortOrder => {
  return create(Expression_SortOrderSchema,
    {
      child: child,
      direction: asc ? Expression_SortOrder_SortDirection.ASCENDING : Expression_SortOrder_SortDirection.DESCENDING,
      nullOrdering: nullsFirst ? Expression_SortOrder_NullOrdering.SORT_NULLS_FIRST : Expression_SortOrder_NullOrdering.SORT_NULLS_LAST
    }
  );
};

export function scalarScalaUdf(
    payload: Uint8Array,
    inputTypes: DataType[],
    outputType: DataType,
    nullable: boolean,
    aggregate: boolean): ScalarScalaUDF {
  return create(ScalarScalaUDFSchema, {
    payload: payload,
    inputTypes: inputTypes.map(DataTypes.toProtoType),
    outputType: DataTypes.toProtoType(outputType),
    nullable: nullable,
    aggregate: aggregate
  });
}

export function javaUDF(
    className: string,
    outputType: DataType,
    aggregate: boolean): JavaUDF {
  return create(JavaUDFSchema, {
      className: className,
      outputType: DataTypes.toProtoType(outputType),
      aggregate: aggregate
  });
}

export function pythonUDF(
    outputType: DataType,
    evalType: number,
    command: Uint8Array,
    pythonVer: string,
    additionalIncludes: string[]): PythonUDF {
  return create(PythonUDFSchema, {
    outputType: DataTypes.toProtoType(outputType),
    evalType: evalType,
    command: command,
    pythonVer: pythonVer,
    additionalIncludes: additionalIncludes
  });
}

export function toLiteralBuilder(value: any): { builder: LiteralBuilder, dataType: DataType } {
  const builder = new LiteralBuilder();
  switch (typeof value) {
    case "string":
      return { builder: builder.withString(value), dataType: DataTypes.StringType };
    case "number":
      // if (DataType.isByte(value)) {
      //   return { builder: builder.withByte(value), dataType: DataTypes.ByteType };
      // } else if (DataType.isShort(value)) {
      //   return { builder: builder.withShort(value), dataType: DataTypes.ShortType };
      // } else 
      if (DataType.isInt32(value)) {
        return { builder: builder.withInt(value), dataType: DataTypes.IntegerType };
      } else if (DataType.isInt64(value)) {
        return { builder: builder.withLong(value), dataType: DataTypes.LongType };
      } else {
        return { builder: builder.withDouble(value), dataType: DataTypes.DoubleType };
      }
    case "boolean":
      return { builder: builder.withBoolean(value), dataType: DataTypes.BooleanType };
    case "bigint":
      return { builder: builder.withLong(value), dataType: DataTypes.LongType };
    case "undefined":
      return { builder: builder.withNull(DataTypes.NullType), dataType: DataTypes.NullType };
    default:
      if (value === null) {
        return { builder: builder.withNull(DataTypes.NullType), dataType: DataTypes.NullType };
      } else if (value instanceof Date) {
        return { builder: builder.withTimestamp(value.getTime()), dataType: DataTypes.TimestampType };
      } else if (value instanceof Uint8Array) {
        return { builder: builder.withBinary(value), dataType: DataTypes.BinaryType };
      } else if (Array.isArray(value)) {
        const elements = value.map(e => toLiteralBuilder(e));
        const et = elements[0] ? elements[0].dataType : DataTypes.NullType;
        return { builder: builder.withArray(elements.map(e => e.builder.build()), et), dataType: et};
      } else if (value instanceof Map) {
        const keys = [];
        const values = [];
        for (const [k, v] of value) {
          keys.push(toLiteralBuilder(k));
          values.push(toLiteralBuilder(v));
        }
        const kt = keys[0] ? keys[0].dataType : DataTypes.NullType;
        const vt = values[0] ? values[0].dataType : DataTypes.NullType;
        return {
          builder: builder.withMap(
            keys.map(k => k.builder.build()),
            values.map(v => v.builder.build()),
            kt,
            vt),
          dataType: DataTypes.createMapType(kt, vt, false)
        };
      } else {
        const fields: Expression_Literal[] = [];
        const dt = DataTypes.createStructType([]);
        Object.keys(value).forEach(key => {
          const field = toLiteralBuilder(value[key]);
          fields.push(field.builder.build());
          dt.add(key, field.dataType, true);
        });
        return { builder: builder.withStruct(fields, dt), dataType: dt };
      }
  }
}

export function toGroupingSetsPB(cols: Column[]): Aggregate_GroupingSets {
  return create(Aggregate_GroupingSetsSchema, {
    groupingSet: cols.map(c => c.expr)
  })
}
