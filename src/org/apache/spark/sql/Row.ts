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

import { bigNumToNumber, getAsPlainJS } from "./arrow/ArrowUtils";
import { DecimalType } from "./types/DecimalType";
import { StructType } from "./types/StructType";
import { TimestampType } from "./types/TimestampType";

/**
 * Represents a row as a key-value mapping where keys are column names.
 */
export type NamedRow = { [name: string]: unknown }

export interface IRow {
  size(): number;
  schema(): StructType;
  isNullAt(i: number): boolean;
  get(i: number): unknown;
  getAs<T>(i: number): T;
  getBoolean(i: number): boolean;
  getByte(i: number): number;
  getShort(i: number): number;
  getInt(i: number): number;
  getLong(i: number): bigint;
  getFloat(i: number): number;
  getDouble(i: number): number;
  getDecimal(i: number): number;
  getString(i: number): string;
  getBinary(i: number): Buffer;
  getUint8Array(i: number): Uint8Array;
  getDate(i: number): Date;
}

/**
 * Represents a row of data in a DataFrame.
 * 
 * @remarks
 * Row provides type-safe access to columnar data with support for
 * various data types including primitives, dates, and binary data.
 * 
 * @example
 * ```typescript
 * const row = new Row(schema, { name: "Alice", age: 30 });
 * const name = row.getString(0);
 * const age = row.getInt(1);
 * ```
 */
export class Row implements IRow {
  [index: number]: unknown;

  constructor(private schema_: StructType, data: NamedRow = {}) {
    schema_.fields.forEach((field, i) => {
      this[i] = data[field.name];
    });
  }

  size(): number {
    return Object.keys(this).length - 1;
  }

  get length(): number {
    return this.size();
  }

  schema(): StructType {
    return this.schema_;
  }

  isNullAt(i: number): boolean {
    return this[i] === null || this[i] === undefined;
  }

  /**
   * Gets the value at the specified column index.
   * 
   * @param i - The column index (0-based)
   * @returns The value at the specified index as a plain JavaScript value
   * @throws Error if the field does not exist
   */
  get(i: number): unknown {
    const dt = this.schema().fields[i]?.dataType;
    if (dt === undefined) {
      // TODO: Use spark error code
      throw new Error(`Field ${i} does not exist`);
    } else  {
      return getAsPlainJS(this.schema().fields[i].dataType, this[i]);
    }
  }

  getAs<T>(i: number): T {
    return this.get(i) as T; // TODO: This is not working like in Scala
  }

  getBoolean(i: number): boolean {
    const value = this[i];
    if (typeof value === 'boolean') {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a boolean`);
    }
  }

  getByte(i: number): number {
    const value = this[i];
    if (typeof value === 'number' && value <= 127 && value >= -128) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a byte`);
    }
  }

  getShort(i: number): number {
    const value = this[i];
    if (typeof value === 'number' && value <= 32767 && value >= -32768) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a short`);
    }
  }

  getInt(i: number): number {
    const value = this[i];
    if (typeof value === 'number' && value <= 2147483647 && value >= -2147483648) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${i}' is not an integer`);
    }
  }

  getLong(i: number): bigint {
    const value = this[i];
    if (typeof value === 'bigint') {
      return value;
    } else if (typeof value === 'number') {
      return BigInt(value);
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a bigint`);
    }
  }

  getFloat(i: number): number {
    return this.getAs<number>(i);
  }

  getDouble(i: number): number {
    return this.getAs<number>(i);
  }

  getDecimal(i: number): number {
    const value = this[i];
    if (typeof value === 'number') {
      return value;
    } else {
      const dt = this.schema_.fields[i].dataType;
      if (dt instanceof DecimalType) {
        return bigNumToNumber(value, dt.scale);
      } else {
        throw new Error(`Value '${value}' for '${i}' is not a Decimal`);
      }
    }
  }

  getString(i: number): string {
    const value = this[i];
    if (typeof value === 'string') {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a string`);
    }
  }

  getBinary(i: number): Buffer {
    return Buffer.from(this.getUint8Array(i));
  }

  getUint8Array(i: number): Uint8Array {
    const value = this[i];
    if (value instanceof Uint8Array) {
      return value;
    } else if (typeof value === 'string' || value instanceof String) {
      return new Uint8Array(Buffer.from(String(value)));
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a Uint8Array`);
    }
  }

  getDate(i: number): Date {
    const value = this[i];
    if (value instanceof Date) {
      return value;
    } else if (typeof value === 'number') {
      return new Date(value);
    } else {
      throw new Error(`Value '${value}' for '${i}' is not a Date`);
    }
  }

  getTimestamp(i: number): Date {
    const dt = this.schema().fields[i].dataType;
    if (dt instanceof TimestampType) {
      return this.getDate(i);
    } else {
      const value = this[i];
      throw new Error(`Value '${value}' for '${i}' is not a Timestamp`);
    }
  }

  toJSON(): NamedRow {
    const obj: NamedRow = {};
    this.schema().fields.forEach((field, i) => {
      obj[field.name] = this.get(i);
    });
    return obj;
  }
}