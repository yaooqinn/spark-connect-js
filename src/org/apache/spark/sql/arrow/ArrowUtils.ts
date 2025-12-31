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

import { Table as ArrowTable, util as AU, MapRow, StructRow, Vector, vectorFromArray } from "apache-arrow";
import { NamedRow, Row } from "../Row";
import { DataTypes } from "../types";
import { ArrayType } from "../types/ArrayType";
import { DataType, DatetimeType } from "../types/data_types";
import { DecimalType } from "../types/DecimalType";
import { MapType } from "../types/MapType";
import { StructType } from "../types/StructType";
import { bigIntToNumber } from "../../../../../utils";

const { isArrowBigNumSymbol, BN } = AU;

function visitRowObject(schema: StructType, data: NamedRow): NamedRow {
  const row: NamedRow = {}
  Object.keys(data).forEach((key, i) => {
    row[key] = getAsPlainJS(schema.fields[i].dataType, data[key]);
  });
  return row;
}

export function getAsPlainJS(dataType: DataType, data: any): any {
  if (data === null || data === undefined) {
    return data;
  }
  if (dataType instanceof StructType) {
    return visitRowObject(dataType, data);
  }

  if (dataType instanceof ArrayType && Array.isArray(data)) {
    const et = dataType.elementType;
    return data.map((v: any) => getAsPlainJS(et, v));
  }

  if (dataType instanceof MapType && data instanceof Map) {
    const map = new Map();
    for (const [key, value] of data) {
      map.set(
        getAsPlainJS(dataType.keyType, key),
        getAsPlainJS(dataType.valueType, value));
    }
    return map;
  }

  if (dataType instanceof DecimalType && data[isArrowBigNumSymbol]) {
    return bigNumToNumber(data, dataType.scale);
  }

  if ((dataType instanceof DatetimeType) && typeof data === 'number') {
    return new Date(data);
  }
  return data;
}

export const DecimalBigNumToNumber = (data: any, scale?: number) => {
  if (data[isArrowBigNumSymbol]) {
    return bigNumToNumber(data, scale);
  } else {
    throw new Error("Expected Decimal value to be a BigNum, but got " + data);
  }
}

export const bigintToDecimalBigNum = (val: bigint) => {
  const arr = new Uint32Array(4);
  if (val >= 0) {
    arr[0] = Number(val & 0xffffffffn);
    arr[1] = Number(val >> 32n & 0xffffffffn);
    arr[2] = Number(val >> 64n & 0xffffffffn);
    arr[3] = Number(val >> 96n & 0xffffffffn);
  } else {
    const negated = ~val;
    arr[0] = Number(val & 0xffffffffn);
    arr[1] = Number(~(negated >> 32n) & 0xffffffffn);
    arr[2] = Number(~(negated >> 64n) & 0xffffffffn);
    arr[3] = Number(~(negated >> 96n) & 0xffffffffn);
  }
  return BN.decimal(arr);
}

/** @ignore */
export function bigNumToNumber(bn: any, scale?: number) {
  const { buffer, byteOffset, byteLength, 'signed': signed } = bn;
  const words = new BigUint64Array(buffer, byteOffset, byteLength / 8);
  const lastWord = words.at(-1);
  if (lastWord === undefined) {
    throw new Error('Empty BigUint64Array in bigNumToNumber');
  }
  const negative = signed && lastWord & (BigInt(1) << BigInt(63));
  let number = BigInt(0);
  let i = 0;
  if (negative) {
      for (const word of words) {
          number |= (word ^ 0xFFFFFFFFFFFFFFFFn) * (BigInt(1) << BigInt(64 * i++));
      }
      number *= BigInt(-1);
      number -= BigInt(1);
  } else {
      for (const word of words) {
          number |= word * (BigInt(1) << BigInt(64 * i++));
      }
  }
  if (typeof scale === 'number') {
      const denominator = BigInt(10) ** BigInt(scale);
      const quotient = number / denominator;
      const remainder = negative ? -(number % denominator) : number % denominator;
      const integerPart = bigIntToNumber(quotient);
      const fractionPart = remainder.toString().padStart(scale, '0');
      const sign = negative && integerPart === 0 ? '-' : '';
      return Number(`${sign}${integerPart}.${fractionPart}`);
  }
  return bigIntToNumber(number);
}

/**
 * 
 * @param data 
 * @param dataType 
 * @returns 
 */
function handleArrowData(data: any, dataType: DataType): any {
  if (data === null || data === undefined) {
    return data;
  }
  if (dataType instanceof StructType && data instanceof StructRow) {
    const row: NamedRow = {}
    const dataJson = data.toJSON();
    for (let i = 0; i < dataType.fields.length; i++) {
      const field = dataType.fields[i];
      row[field.name] = handleArrowData(dataJson[field.name], field.dataType);
    }
    return row;
  }

  if (dataType instanceof MapType && data instanceof MapRow) {
    const map = new Map();
    for (const [key, value] of data) {
      map.set(
        handleArrowData(key, dataType.keyType),
        handleArrowData(value, dataType.valueType));
    }
    return map;
  }

  if (dataType instanceof ArrayType && data instanceof Vector) {
    const arr = new Array(data.length);
    for (let i = 0; i < data.length; i++) {
      arr[i] = handleArrowData(data.get(i),  dataType.elementType);
    }
    return arr;
  }
  return data;
}

/**
 * Converts an Arrow table to an array of rows.
 */
export function tableToRows(table: ArrowTable, schema: StructType): Row[] {
  const rows: Row[] = new Array<Row>(table.numRows);
  for(let i = 0; i < schema.fields.length; i++) {
    const colAtIdx = table.getChild(schema.fields[i].name);
    for (let j = 0; j < table.numRows; j++) {
      const row = rows[j] || (rows[j] = new Row(schema));
      row[i] = handleArrowData(colAtIdx?.get(j), schema.fields[i].dataType);
    }
  }
  return rows;
}


export function tableFromRows(rows: Row[], schema: StructType): ArrowTable {
  const vecs = {} as {[key: string]: Vector<any>}
  const arrowSchema = DataTypes.toArrowSchema(schema);
  arrowSchema.fields.forEach((f, i) => {
    const col = rows.map(row => row[i]);
    vecs[f.name] = vectorFromArray(col, f.type)
  });
  return new ArrowTable(arrowSchema, vecs);
}
