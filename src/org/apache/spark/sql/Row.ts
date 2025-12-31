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

/* eslint-disable @typescript-eslint/no-unsafe-assignment */
// Row.get() returns 'any' by design - data types are determined at runtime

import { bigNumToNumber, getAsPlainJS } from "./arrow/ArrowUtils";
import { DecimalType } from "./types/DecimalType";
import { StructType } from "./types/StructType";
import { TimestampType } from "./types/TimestampType";

/**
 * Represents a row as a key-value mapping where keys are column names.
 */
export type NamedRow = { [name: string]: any }

export interface IRow {
  size(): number;
  schema(): StructType;
  isNullAt(i: number): boolean;
  get(i: number): any;
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
  [index: number]: any;

  constructor(private schema_: StructType, data: NamedRow = {}) {
    schema_.fields.forEach((field, i) => {
      this[i] = data[field.name];
    });
  }

  /**
   * Returns the number of elements in the Row.
   * 
   * @returns The number of columns in this row
   */
  size(): number {
    return Object.keys(this).length - 1;
  }

  /**
   * Returns the number of elements in the Row (alias for size()).
   * 
   * @returns The number of columns in this row
   */
  get length(): number {
    return this.size();
  }

  /**
   * Returns the schema of this Row.
   * 
   * @returns The StructType schema defining the fields in this row
   */
  schema(): StructType {
    return this.schema_;
  }

  /**
   * Checks whether the value at position i is null.
   * 
   * @param i - The column index (0-based)
   * @returns True if the value is null or undefined, false otherwise
   */
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
  get(i: number): any {
    const dt = this.schema().fields[i]?.dataType;
    // Runtime bounds check for field access
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (dt === undefined) {
      // TODO: Use spark error code
      throw new Error(`Field ${i} does not exist`);
    } else  {
      return getAsPlainJS(this.schema().fields[i].dataType, this[i]);
    }
  }

  /**
   * Returns the value at position i, cast to type T.
   * 
   * @typeParam T - The type to cast the value to
   * @param i - The column index (0-based)
   * @returns The value at the specified index, cast to type T
   */
  getAs<T>(i: number): T {
    return this.get(i) as T; // TODO: This is not working like in Scala
  }

  /**
   * Returns the value at position i as a boolean.
   * 
   * @param i - The column index (0-based)
   * @returns The boolean value at the specified index
   * @throws Error if the value is not a boolean
   */
  getBoolean(i: number): boolean {
    if (typeof this[i] === 'boolean') {
      return this[i];
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a boolean`);
    }
  }

  /**
   * Returns the value at position i as a byte (8-bit signed integer).
   * 
   * @param i - The column index (0-based)
   * @returns The byte value at the specified index (range: -128 to 127)
   * @throws Error if the value is not a valid byte
   */
  getByte(i: number): number {
    if (typeof this[i] === 'number' && this[i] <= 127 && this[i] >= -128) {
      return this[i];
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a byte`);
    }
  }

  /**
   * Returns the value at position i as a short (16-bit signed integer).
   * 
   * @param i - The column index (0-based)
   * @returns The short value at the specified index (range: -32768 to 32767)
   * @throws Error if the value is not a valid short
   */
  getShort(i: number): number {
    if (typeof this[i] === 'number' && this[i] <= 32767 && this[i] >= -32768) {
      return this[i];
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a short`);
    }
  }

  /**
   * Returns the value at position i as an integer (32-bit signed integer).
   * 
   * @param i - The column index (0-based)
   * @returns The integer value at the specified index
   * @throws Error if the value is not a valid integer
   */
  getInt(i: number): number {
    if (typeof this[i] === 'number' && this[i] <= 2147483647 && this[i] >= -2147483648) {
      return this[i];
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not an integer`);
    }
  }

  /**
   * Returns the value at position i as a long (64-bit signed integer).
   * 
   * @param i - The column index (0-based)
   * @returns The bigint value at the specified index
   * @throws Error if the value cannot be converted to bigint
   */
  getLong(i: number): bigint {
    if (typeof this[i] === 'bigint') {
      return this[i];
    } else if (typeof this[i] === 'number') {
      return BigInt(this[i]);
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a bigint`);
    }
  }

  /**
   * Returns the value at position i as a float (32-bit floating point number).
   * 
   * @param i - The column index (0-based)
   * @returns The float value at the specified index
   */
  getFloat(i: number): number {
    return this.getAs<number>(i);
  }

  /**
   * Returns the value at position i as a double (64-bit floating point number).
   * 
   * @param i - The column index (0-based)
   * @returns The double value at the specified index
   */
  getDouble(i: number): number {
    return this.getAs<number>(i);
  }

  /**
   * Returns the value at position i as a decimal number.
   * 
   * @param i - The column index (0-based)
   * @returns The decimal value at the specified index
   * @throws Error if the value is not a valid decimal
   */
  getDecimal(i: number): number {
    if (typeof this[i] === 'number') {
      return this[i];
    } else {
      const dt = this.schema_.fields[i].dataType;
      if (dt instanceof DecimalType) {
        return bigNumToNumber(this[i], dt.scale);
      } else {
        throw new Error(`Value '${this[i]}' for '${i}' is not a Decimal`);
      }
    }
  }

  /**
   * Returns the value at position i as a string.
   * 
   * @param i - The column index (0-based)
   * @returns The string value at the specified index
   * @throws Error if the value is not a string
   */
  getString(i: number): string {
    if (typeof this[i] === 'string') {
      return this[i];
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a string`);
    }
  }

  /**
   * Returns the value at position i as a Buffer (binary data).
   * 
   * @param i - The column index (0-based)
   * @returns The binary value as a Node.js Buffer
   * @throws Error if the value cannot be converted to binary
   */
  getBinary(i: number): Buffer {
    return Buffer.from(this.getUint8Array(i));
  }

  /**
   * Returns the value at position i as a Uint8Array.
   * 
   * @param i - The column index (0-based)
   * @returns The binary value as a Uint8Array
   * @throws Error if the value is not a valid byte array
   */
  getUint8Array(i: number): Uint8Array {
    if (this[i] instanceof Uint8Array) {
      return this[i];
    } else if (this[i] instanceof String) {
      return new Uint8Array(Buffer.from(this[i]));
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a Uint8Array`);
    }
  }

  /**
   * Returns the value at position i as a Date.
   * 
   * @param i - The column index (0-based)
   * @returns The date value at the specified index
   * @throws Error if the value cannot be converted to a Date
   */
  getDate(i: number): Date {
    if (this[i] instanceof Date) {
      return this[i];
    } else if (typeof this[i] === 'number') {
      return new Date(this[i]);
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a Date`);
    }
  }

  /**
   * Returns the value at position i as a timestamp (Date).
   * 
   * @param i - The column index (0-based)
   * @returns The timestamp value as a Date object
   * @throws Error if the field is not a TimestampType or cannot be converted
   */
  getTimestamp(i: number): Date {
    const dt = this.schema().fields[i].dataType;
    if (dt instanceof TimestampType) {
      return this.getDate(i);
    } else {
      throw new Error(`Value '${this[i]}' for '${i}' is not a Timestamp`);
    }
  }

  /**
   * Converts the Row to a plain JavaScript object with column names as keys.
   * 
   * @returns An object mapping column names to their values
   */
  toJSON(): NamedRow {
    const obj: NamedRow = {};
    this.schema().fields.forEach((field, i) => {
      obj[field.name] = this.get(i);
    });
    return obj;
  }
}