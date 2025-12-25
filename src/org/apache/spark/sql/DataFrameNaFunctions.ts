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

import { DataFrame } from './DataFrame';
import { toLiteralBuilder } from './proto/expression/utils';

/**
 * Functionality for working with missing data in DataFrames.
 * 
 * @group untypedrel
 */
export class DataFrameNaFunctions {
  constructor(private readonly df: DataFrame) {}

  /**
   * Returns a new DataFrame that replaces null or NaN values.
   *
   * The fill operation can operate on all columns or specific columns.
   *
   * @param value Value to replace null values with. Must be a number, string, or boolean.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with null values replaced.
   *
   * @example
   * ```typescript
   * // Replace nulls with 0 in all numeric columns
   * df.na.fill(0)
   *
   * // Replace nulls with "unknown" in specific columns
   * df.na.fill("unknown", ["name", "address"])
   * ```
   */
  fill(value: number | string | boolean, cols?: string[]): DataFrame {
    return this.fillna(value, cols);
  }

  /**
   * Returns a new DataFrame that replaces null or NaN values.
   *
   * The fill operation can operate on all columns or specific columns.
   *
   * @param value Value to replace null values with. Must be a number, string, or boolean.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with null values replaced.
   *
   * @example
   * ```typescript
   * // Replace nulls with 0 in all numeric columns
   * df.na.fillna(0)
   *
   * // Replace nulls with "unknown" in specific columns
   * df.na.fillna("unknown", ["name", "address"])
   * ```
   */
  fillna(value: number | string | boolean, cols?: string[]): DataFrame {
    const literalValue = toLiteralBuilder(value).builder.build();
    const values = [literalValue];
    const columns = cols || [];
    return this.df.spark.relationBuilderToDF(b =>
      b.withNAFill(columns, values, this.df.plan.relation)
    );
  }

  /**
   * Returns a new DataFrame that drops rows containing null or NaN values.
   *
   * @param how Specifies the deletion criteria:
   *   - 'any': Drop rows containing any null or NaN values.
   *   - 'all': Drop rows only if all values are null or NaN.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with rows containing null values dropped.
   *
   * @example
   * ```typescript
   * // Drop rows with any null values
   * df.na.drop()
   *
   * // Drop rows only if all values are null
   * df.na.drop('all')
   *
   * // Drop rows with any null values in specific columns
   * df.na.drop('any', ['name', 'age'])
   * ```
   */
  drop(how?: 'any' | 'all', cols?: string[]): DataFrame;
  /**
   * Returns a new DataFrame that drops rows containing null or NaN values.
   *
   * @param minNonNulls Minimum number of non-null and non-NaN values required to keep a row.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with rows containing null values dropped.
   *
   * @example
   * ```typescript
   * // Drop rows with fewer than 2 non-null values
   * df.na.drop(2)
   *
   * // Drop rows with fewer than 3 non-null values in specific columns
   * df.na.drop(3, ['col1', 'col2', 'col3'])
   * ```
   */
  drop(minNonNulls?: number, cols?: string[]): DataFrame;
  drop(arg?: 'any' | 'all' | number, cols?: string[]): DataFrame {
    if (typeof arg === 'string') {
      return this.dropna(arg as 'any' | 'all', cols);
    } else {
      return this.dropna(arg as number | undefined, cols);
    }
  }

  /**
   * Returns a new DataFrame that drops rows containing null or NaN values.
   *
   * @param how Specifies the deletion criteria:
   *   - 'any': Drop rows containing any null or NaN values.
   *   - 'all': Drop rows only if all values are null or NaN.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with rows containing null values dropped.
   *
   * @example
   * ```typescript
   * // Drop rows with any null values
   * df.na.dropna()
   *
   * // Drop rows only if all values are null
   * df.na.dropna('all')
   *
   * // Drop rows with any null values in specific columns
   * df.na.dropna('any', ['name', 'age'])
   * ```
   */
  dropna(how?: 'any' | 'all', cols?: string[]): DataFrame;
  /**
   * Returns a new DataFrame that drops rows containing null or NaN values.
   *
   * @param minNonNulls Minimum number of non-null and non-NaN values required to keep a row.
   * @param cols Optional list of column names to consider. If not specified, all columns are considered.
   * @returns DataFrame with rows containing null values dropped.
   *
   * @example
   * ```typescript
   * // Drop rows with fewer than 2 non-null values
   * df.na.dropna(2)
   *
   * // Drop rows with fewer than 3 non-null values in specific columns
   * df.na.dropna(3, ['col1', 'col2', 'col3'])
   * ```
   */
  dropna(minNonNulls?: number, cols?: string[]): DataFrame;
  dropna(arg?: 'any' | 'all' | number, cols?: string[]): DataFrame {
    const columns = cols || [];
    let minNonNulls: number | undefined;

    if (arg === 'all') {
      minNonNulls = 1;
    } else if (arg === 'any' || arg === undefined) {
      // When undefined, the server will drop rows with any null values (all columns must be non-null)
      minNonNulls = undefined;
    } else if (typeof arg === 'number') {
      minNonNulls = arg;
    } else {
      throw new Error(`Invalid argument for dropna: ${arg}`);
    }

    return this.df.spark.relationBuilderToDF(b =>
      b.withNADrop(columns, minNonNulls, this.df.plan.relation)
    );
  }

  /**
   * Returns a new DataFrame that replaces values matching keys in the replacement map.
   *
   * @param replacement Map of old values to new values. Keys and values must be of the same type.
   * @param cols Optional list of column names to consider. If not specified, all type-compatible columns are considered.
   * @returns DataFrame with specified values replaced.
   *
   * @example
   * ```typescript
   * // Replace values in all compatible columns
   * df.na.replace({ 'UNKNOWN': null, 'N/A': null })
   *
   * // Replace values in specific columns
   * df.na.replace({ 1: 100, 2: 200 }, ['col1', 'col2'])
   * ```
   */
  replace(replacement: { [key: string]: any } | { [key: number]: any }, cols?: string[]): DataFrame {
    const columns = cols || [];
    const replacements: Array<{ oldValue: any; newValue: any }> = [];

    for (const [oldVal, newVal] of Object.entries(replacement)) {
      // Object.entries always returns string keys, even if the original object had numeric keys.
      // To preserve intended numeric types, we attempt to parse string keys that look numeric.
      // This handles common cases where users pass { 1: 100, 2: 200 } which becomes { '1': 100, '2': 200 }.
      // Only simple integer/decimal formats are converted; other strings remain as-is.
      let oldValue: any = oldVal;
      if (typeof oldVal === 'string' && /^-?\d+(\.\d+)?$/.test(oldVal)) {
        const parsed = Number(oldVal);
        if (!isNaN(parsed)) {
          oldValue = parsed;
        }
      }
      replacements.push({ oldValue, newValue: newVal });
    }

    return this.df.spark.relationBuilderToDF(b =>
      b.withNAReplace(columns, replacements, this.df.plan.relation)
    );
  }
}
