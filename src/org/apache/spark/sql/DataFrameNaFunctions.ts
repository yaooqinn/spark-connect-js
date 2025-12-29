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
 * Functionality for working with missing data in DataFrames.
 */
export class DataFrameNaFunctions {
  constructor(private df: DataFrame) {}

  /**
   * Returns a new DataFrame that drops rows containing null or NaN values.
   *
   * @param how
   *   'any': drop a row if it contains any null or NaN values.
   *   'all': drop a row only if all its values are null or NaN.
   * @param cols
   *   Optional list of column names to consider. If not specified, all columns are considered.
   * @return DataFrame with null/NaN rows dropped
   */
  drop(how?: 'any' | 'all', cols?: string[]): DataFrame;
  /**
   * Returns a new DataFrame that drops rows containing null or NaN values
   * in the specified columns.
   *
   * @param cols
   *   List of column names to consider.
   * @return DataFrame with null/NaN rows dropped
   */
  drop(cols: string[]): DataFrame;
  /**
   * Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null and non-NaN values.
   *
   * @param minNonNulls
   *   Minimum number of non-null and non-NaN values required to keep a row.
   * @param cols
   *   Optional list of column names to consider.
   * @return DataFrame with rows dropped based on threshold
   */
  drop(minNonNulls: number, cols?: string[]): DataFrame;
  drop(howOrColsOrMinNonNulls?: 'any' | 'all' | string[] | number, cols?: string[]): DataFrame {
    let minNonNulls: number | undefined = undefined;
    let columnNames: string[] | undefined = undefined;

    if (typeof howOrColsOrMinNonNulls === 'string') {
      // drop(how, cols?)
      const how = howOrColsOrMinNonNulls as 'any' | 'all';
      if (how === 'all') {
        minNonNulls = 1;
      }
      // 'any' means minNonNulls is undefined (all columns must be non-null)
      columnNames = cols;
    } else if (typeof howOrColsOrMinNonNulls === 'number') {
      // drop(minNonNulls, cols?)
      minNonNulls = howOrColsOrMinNonNulls;
      columnNames = cols;
    } else if (Array.isArray(howOrColsOrMinNonNulls)) {
      // drop(cols)
      columnNames = howOrColsOrMinNonNulls;
    }

    return this.df.spark.relationBuilderToDF(b =>
      b.withNADrop(columnNames, minNonNulls, this.df.plan.relation)
    );
  }

  /**
   * Returns a new DataFrame that replaces null or NaN values.
   *
   * @param value
   *   Value to replace null/NaN values with. Must be a number, string, or boolean.
   * @param cols
   *   Optional list of column names to consider. If not specified, all compatible columns are considered.
   * @return DataFrame with null values replaced
   */
  fill(value: number | string | boolean, cols?: string[]): DataFrame;
  /**
   * Returns a new DataFrame that replaces null or NaN values using a map of column names to values.
   *
   * @param valueMap
   *   Map of column names to replacement values.
   * @return DataFrame with null values replaced
   */
  fill(valueMap: { [key: string]: number | string | boolean }): DataFrame;
  fill(
    valueOrMap: number | string | boolean | { [key: string]: number | string | boolean },
    cols?: string[]
  ): DataFrame {
    if (typeof valueOrMap === 'object' && !Array.isArray(valueOrMap)) {
      // fill(valueMap)
      const valueMap = valueOrMap as { [key: string]: number | string | boolean };
      const columns = Object.keys(valueMap);
      const values = columns.map(col => valueMap[col]);
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAFill(columns, values, this.df.plan.relation)
      );
    } else {
      // fill(value, cols?)
      const value = valueOrMap as number | string | boolean;
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAFill(cols, [value], this.df.plan.relation)
      );
    }
  }

  /**
   * Returns a new DataFrame replacing a value with another value.
   *
   * @param from
   *   Value to be replaced. Must be a number, string, or boolean.
   * @param to
   *   Value to replace with. Must be a number, string, or boolean.
   * @param cols
   *   Optional list of column names to consider. If not specified, all compatible columns are considered.
   * @return DataFrame with values replaced
   */
  replace(from: number | string | boolean, to: number | string | boolean, cols?: string[]): DataFrame;
  /**
   * Returns a new DataFrame replacing values using a map.
   *
   * @param replacementMap
   *   Map of old values to new replacement values.
   * @param cols
   *   Optional list of column names to consider. If not specified, all compatible columns are considered.
   * @return DataFrame with values replaced
   */
  replace(replacementMap: { [key: string]: number | string | boolean | null }, cols?: string[]): DataFrame;
  replace(
    fromOrMap: number | string | boolean | { [key: string]: number | string | boolean | null },
    toOrCols?: number | string | boolean | string[],
    cols?: string[]
  ): DataFrame {
    if (typeof fromOrMap === 'object' && !Array.isArray(fromOrMap)) {
      // replace(replacementMap, cols?)
      const replacementMap = fromOrMap as { [key: string]: number | string | boolean | null };
      const columnNames = Array.isArray(toOrCols) ? toOrCols : undefined;
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAReplace(columnNames, replacementMap, this.df.plan.relation)
      );
    } else {
      // replace(from, to, cols?)
      const fromValue = fromOrMap as number | string | boolean;
      const toValue = toOrCols as number | string | boolean;
      const columnNames = cols;
      const replacementMap: { [key: string]: number | string | boolean | null } = {};
      
      // Create a single-entry map for the replacement
      const key = String(fromValue);
      replacementMap[key] = toValue;
      
      return this.df.spark.relationBuilderToDF(b =>
        b.withNAReplace(columnNames, replacementMap, this.df.plan.relation)
      );
    }
  }
}
