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

/**
 * Functionality for working with missing data in [[DataFrame]]s.
 *
 * @since 1.0.0
 */
export class DataFrameNaFunctions {
  constructor(public readonly df: DataFrame) {}

  /**
   * Returns a new [[DataFrame]] that drops rows containing any null or NaN values.
   *
   * @since 1.0.0
   */
  drop(): DataFrame;
  /**
   * Returns a new [[DataFrame]] that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.0.0
   */
  drop(cols: string[]): DataFrame;
  /**
   * Returns a new [[DataFrame]] that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values.
   * If `how` is "all", then drop rows only if every column is null or NaN for that row.
   *
   * @since 1.0.0
   */
  drop(how: 'any' | 'all'): DataFrame;
  /**
   * Returns a new [[DataFrame]] that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.0.0
   */
  drop(how: 'any' | 'all', cols: string[]): DataFrame;
  /**
   * Returns a new [[DataFrame]] that drops rows containing less than `minNonNulls` non-null
   * and non-NaN values.
   *
   * @since 1.0.0
   */
  drop(minNonNulls: number): DataFrame;
  /**
   * Returns a new [[DataFrame]] that drops rows containing less than `minNonNulls` non-null
   * and non-NaN values in the specified columns.
   *
   * @since 1.0.0
   */
  drop(minNonNulls: number, cols: string[]): DataFrame;
  drop(howOrMinNonNullsOrCols?: 'any' | 'all' | number | string[], cols?: string[]): DataFrame {
    let minNonNulls: number | undefined = undefined;
    let columnNames: string[] = [];

    if (howOrMinNonNullsOrCols === undefined) {
      // drop(): drop rows with any null
      minNonNulls = undefined;
    } else if (Array.isArray(howOrMinNonNullsOrCols)) {
      // drop(cols: string[]): drop rows with any null in specified columns
      columnNames = howOrMinNonNullsOrCols;
      minNonNulls = undefined;
    } else if (typeof howOrMinNonNullsOrCols === 'string') {
      // drop(how: 'any' | 'all'): drop based on how parameter
      if (howOrMinNonNullsOrCols === 'all') {
        minNonNulls = 1;
      } else {
        minNonNulls = undefined;
      }
      columnNames = cols || [];
    } else if (typeof howOrMinNonNullsOrCols === 'number') {
      // drop(minNonNulls: number) or drop(minNonNulls: number, cols: string[])
      minNonNulls = howOrMinNonNullsOrCols;
      columnNames = cols || [];
    }

    return this.df.spark.relationBuilderToDF(b =>
      b.withNADrop(this.df.plan.relation, columnNames, minNonNulls)
    );
  }

  /**
   * Returns a new [[DataFrame]] that replaces null or NaN values.
   *
   * @since 1.0.0
   */
  fill(value: number | string | boolean): DataFrame;
  /**
   * Returns a new [[DataFrame]] that replaces null or NaN values in specified columns.
   * If a specified column has a type that is not compatible with the value type, that
   * column is ignored.
   *
   * @since 1.0.0
   */
  fill(value: number | string | boolean, cols: string[]): DataFrame;
  /**
   * Returns a new [[DataFrame]] that replaces null values using the specified value map.
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `number`, `string`, or `boolean`.
   *
   * @since 1.0.0
   */
  fill(valueMap: { [key: string]: number | string | boolean }): DataFrame;
  fill(
    valueOrValueMap: number | string | boolean | { [key: string]: number | string | boolean },
    cols?: string[]
  ): DataFrame {
    if (typeof valueOrValueMap === 'object' && !Array.isArray(valueOrValueMap) && valueOrValueMap !== null) {
      // fill(valueMap: { [key: string]: number | string | boolean })
      const valueMap = valueOrValueMap as { [key: string]: number | string | boolean };
      const columnNames = Object.keys(valueMap);
      const values = Object.values(valueMap);
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAFill(this.df.plan.relation, columnNames, values)
      );
    } else {
      // fill(value: number | string | boolean) or fill(value, cols: string[])
      const value = valueOrValueMap as number | string | boolean;
      const columnNames = cols || [];
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAFill(this.df.plan.relation, columnNames, [value])
      );
    }
  }

  /**
   * Returns a new [[DataFrame]] that replaces values matching the keys in `replacement` map
   * with the corresponding values.
   *
   * @since 1.0.0
   */
  replace(
    cols: string | string[],
    replacement: { [key: string]: string | number | boolean } | Map<string | number | boolean, string | number | boolean>
  ): DataFrame {
    const columnNames = Array.isArray(cols) ? cols : [cols];
    let replacementMap: Map<string | number | boolean, string | number | boolean>;
    
    if (replacement instanceof Map) {
      replacementMap = replacement;
    } else {
      // Convert plain object to Map, preserving numeric keys
      replacementMap = new Map();
      for (const [key, value] of Object.entries(replacement)) {
        // Try to parse key as number if it looks like one
        const numericKey = Number(key);
        const actualKey = !isNaN(numericKey) && key === numericKey.toString() ? numericKey : key;
        replacementMap.set(actualKey, value);
      }
    }
    
    return this.df.spark.relationBuilderToDF(b =>
      b.withNAReplace(this.df.plan.relation, columnNames, replacementMap)
    );
  }
}
