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
import { DataType, DataType_ArraySchema, DataType_BinarySchema, DataType_BooleanSchema, DataType_ByteSchema, DataType_CalendarIntervalSchema, DataType_CharSchema, DataType_DateSchema, DataType_DayTimeIntervalSchema, DataType_DecimalSchema, DataType_DoubleSchema, DataType_FloatSchema, DataType_IntegerSchema, DataType_LongSchema, DataType_MapSchema, DataType_NULLSchema, DataType_ShortSchema, DataType_StringSchema, DataType_StructField, DataType_StructFieldSchema, DataType_StructSchema, DataType_TimestampNTZSchema, DataType_TimestampSchema, DataType_UDTSchema, DataType_UnparsedSchema, DataType_VarCharSchema, DataType_VariantSchema, DataType_YearMonthIntervalSchema, DataTypeSchema } from "../../../../../gen/spark/connect/types_pb";

export const PROTO_NULL: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'null',
        value: create(DataType_NULLSchema)
      }
  }
);

export const PROTO_BOOLEAN: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'boolean',
        value: create(DataType_BooleanSchema)
      }
  }
);

export const PROTO_BYTE: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'byte',
        value: create(DataType_ByteSchema)
      }
  }
);

export const PROTO_SHORT: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'short',
        value: create(DataType_ShortSchema)
      }
  }
);

export const PROTO_INT: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'integer',
        value: create(DataType_IntegerSchema)
      }
  }
);

export const PROTO_LONG: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'long',
        value: create(DataType_LongSchema)
      }
  }
);

export const PROTO_FLOAT: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'float',
        value: create(DataType_FloatSchema)
      }
  }
);

export const PROTO_DOUBLE: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'double',
        value: create(DataType_DoubleSchema)
      }
  }
);

export const PROTO_DATE: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'date',
        value: create(DataType_DateSchema)
      }
  }
);

export const PROTO_TIMESTAMP: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'timestamp',
        value: create(DataType_TimestampSchema)
      }
  }
);


export const PROTO_TIMESTAMP_NTZ: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'timestampNtz',
        value: create(DataType_TimestampNTZSchema)
      }
  }
);

export const PROTO_CALANDAR_INTERVAL: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'calendarInterval',
        value: create(DataType_CalendarIntervalSchema)
      }
  }
);

export const PROTO_BINARY: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'binary',
        value: create(DataType_BinarySchema)
      }
  }
);

export const PROTO_STRING: DataType = createProtoString0('UTF8_BINARY');

export function createProtoString(collation?: string): DataType {
  if (collation === undefined || 'UTF8_BINARY' === collation.toUpperCase()) {
    return PROTO_STRING;
  } else  {
    return createProtoString0(collation);
  }
}

function createProtoString0(collation: string): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'string',
          value: create(DataType_StringSchema, { collation: collation })
        }
    }
  )
}

export function createProtoChar(length?: number): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'char',
          value: create(DataType_CharSchema, { length: length })
        }
    }
  )
}

export function createProtoVarchar(length?: number): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'varChar',
          value: create(DataType_VarCharSchema, { length: length })
        }
    }
  )
}

export function createProtoDecimal(precision?: number, scale?: number): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'decimal',
          value: create(DataType_DecimalSchema, { precision: precision, scale: scale })
        }
    }
  )
}

export function createProtoDayTimeInterval(start: number, end: number): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'dayTimeInterval',
          value: create(DataType_DayTimeIntervalSchema, { startField: start, endField: end })
        }
    }
  )
}

export function createProtoYearMonthInterval(start: number, end: number): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'yearMonthInterval',
          value: create(DataType_YearMonthIntervalSchema, { startField: start, endField: end })
        }
    }
  )
}

export function createProtoArray(elementType: DataType | undefined, containsNull: boolean = true): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'array',
          value: create(DataType_ArraySchema, { elementType: elementType, containsNull: containsNull })
        }
    }
  )
}

export function createProtoMap(keyType: DataType, valueType: DataType, valueContainsNull: boolean): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'map',
          value: create(DataType_MapSchema, { keyType: keyType, valueType: valueType, valueContainsNull: valueContainsNull })
        }
    }
  )
}

export function createProtoStructField(
    fieldName: string,
    dataType: DataType,
    nullable: boolean,
    metadata?: string): DataType_StructField {
  return create(DataType_StructFieldSchema, { name: fieldName, dataType: dataType, nullable: nullable, metadata: metadata });
}

export function createProtoStruct(fields: DataType_StructField[]): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'struct',
          value: create(DataType_StructSchema, { fields: fields })
        }
    }
  )
}

export const PROTO_VARIANT: DataType = create(DataTypeSchema,
  {
    kind:
      {
        case: 'variant',
        value: create(DataType_VariantSchema)
      }
  }
);

export function createProtoUnparsed(typeStr: string): DataType {
  return create(DataTypeSchema,
    {
      kind:
        {
          case: 'unparsed',
          value: create(DataType_UnparsedSchema, { dataTypeString: typeStr })
        }
    }
  );
}

export function createProtoUdt(
    type: string,
    jvmClass?: string,
    pythonClass?: string,
    serializedPythonClass?: string,
    sqlType?: DataType): DataType {
  return create(DataTypeSchema,
    {
      kind: {
        case: 'udt',
        value: create(DataType_UDTSchema, {
          type: type,
          jvmClass: jvmClass,
          pythonClass: pythonClass,
          serializedPythonClass: serializedPythonClass,
          sqlType: sqlType
        })
      }
    });
}
