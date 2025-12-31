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

import { Column } from "./Column";
import { DataFrame } from "./DataFrame";
import * as f from "./functions";
import { GroupType } from "./proto/aggregate/GroupType";
import { toPivotPB, toGroupingSetsPB } from "./proto/expression/utils";
import { toGroupTypePB } from "./proto/ProtoUtils";
import { Aggregate_Pivot } from "../../../../gen/spark/connect/relations_pb";
import { StructType } from "./types/StructType";

const supportedAggFunctions: Record<string, (c: Column) => Column> = {
  avg: f.avg,
  mean: f.mean,
  sum: f.sum,
  min: f.min,
  max: f.max,
  first: f.first,
  last: f.last,
  stddev: f.stddev,
  stddev_pop: f.stddev_pop,
  stddev_samp: f.stddev_samp,
  variance: f.variance,
  var_pop: f.var_pop,
  var_samp: f.var_samp,
  collect_list: f.collect_list,
  collect_set: f.collect_set,
  count: f.count,
};

/**
 * A set of methods for aggregations on a `DataFrame`, created by [[Dataset#groupBy groupBy]],
 * [[Dataset#cube cube]] or [[Dataset#rollup rollup]] (and also `pivot`).
 */
export class RelationalGroupedDataset {
  constructor(
    public readonly df: DataFrame,
    public readonly groupingExprs: string[] | Column[],
    public readonly groupType: GroupType,
    public readonly pivotValue: Aggregate_Pivot | undefined = undefined,
    public readonly groupingSets: Column[][] = []
  ) {}

  toDF(...aggExprs: Column[]): DataFrame {
    return this.df.spark.relationBuilderToDF((rb) => {
      return rb.withAggregateBuilder((ab) => {
        if (typeof this.groupingExprs[0] === "string") {
          ab.withGroupingExpressions(this.groupingExprs.map((c) => this.df.col(c as string).expr))
        } else {
          ab.withGroupingExpressions(this.groupingExprs.map((c) => (c as Column).expr))
        }
        if (this.groupingSets.length > 0) {
          ab.withGroupingSets(this.groupingSets.map((gs) => toGroupingSetsPB(gs)))
        }
        return ab.withInput(this.df.plan.relation)
          .withAggregateExpressions(aggExprs.map((c) => c.expr))
          .withPivot(this.pivotValue)
          .withGroupType(toGroupTypePB(this.groupType))
      });
    });
  }

  count(): DataFrame {
    return this.toDF(f.count(f.lit(1)).as("count"))
  }

  sum(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.sum(this.df.col(c))))
  }


  avg(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.avg(this.df.col(c))))
  }

  mean(...cols: string[]): DataFrame {
    return this.avg(...cols)
  }

  min(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.min(this.df.col(c))))
  }

  max(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.max(this.df.col(c))))
  }

  first(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.first(this.df.col(c))))
  }

  last(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.last(this.df.col(c))))
  }

  stddev(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.stddev(this.df.col(c))))
  }

  stddevPop(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.stddev_pop(this.df.col(c))))
  }

  stddevSamp(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.stddev_samp(this.df.col(c))))
  }

  variance(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.variance(this.df.col(c))))
  }

  varPop(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.var_pop(this.df.col(c))))
  }

  varSamp(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.var_samp(this.df.col(c))))
  }

  collect_list(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.collect_list(this.df.col(c))))
  }

  collect_set(...cols: string[]): DataFrame {
    return this.toDF(...cols.map((c) => f.collect_set(this.df.col(c))))
  }

  agg(exprs: Record<string, string>): DataFrame;
  agg(...exprs: Column[]): DataFrame;
  agg(exprsOrMap: Record<string, string> | Column, ...exprs: Column[]): DataFrame {
    if (typeof exprsOrMap === "object" && !(exprsOrMap instanceof Column)) {
      const aggExprs = Object.entries(exprsOrMap).map(([col, func]) => {
        const fn = supportedAggFunctions[func];
        if (typeof fn !== "function") {
          const available = Object.keys(supportedAggFunctions).join(", ");
          throw new Error(`Unsupported aggregate function: ${func}. Supported functions: ${available}.`);
        }
        return fn(this.df.col(col));
      });
      return this.toDF(...aggExprs);
    } else {
      const allExprs = [exprsOrMap, ...exprs];
      return this.toDF(...allExprs);
    }
  }

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   *
   * This method is only supported after a `groupBy` operation. There are two versions of pivot:
   * one with explicit pivot values and one without.
   *
   * @param pivotColumn - Column name or Column to pivot on
   * @param values - Optional list of values that will be translated to columns in the output DataFrame
   * @returns A new RelationalGroupedDataset with pivot configuration
   *
   * @example
   * ```typescript
   * // Pivot without values (Spark will compute distinct values)
   * df.groupBy("year").pivot("course").sum("earnings")
   *
   * // Pivot with explicit values (more efficient)
   * df.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")
   * ```
   */
  pivot(pivotColumn: string | Column, values?: any[]): RelationalGroupedDataset {
    if (this.groupType !== GroupType.GROUP_TYPE_GROUPBY) {
      if (this.groupType === GroupType.GROUP_TYPE_PIVOT) {
        throw new Error("pivot cannot be applied on a pivot");
      }
      throw new Error("pivot can only be applied after groupBy");
    }

    const pivotCol = typeof pivotColumn === "string" ? this.df.col(pivotColumn) : pivotColumn;
    const pivotProto = toPivotPB(pivotCol.expr, values);

    return new RelationalGroupedDataset(
      this.df,
      this.groupingExprs,
      GroupType.GROUP_TYPE_PIVOT,
      pivotProto,
      this.groupingSets
    );
  }

  /**
   * Apply a function to each group of the DataFrame.
   * 
   * This method applies a user-defined function to each group. The function receives
   * the group key and an iterator of rows for that group, and should return an iterator
   * of rows.
   * 
   * @param pythonCode Python code as a string defining the group processing function
   * @param outputSchema The output schema for the transformed DataFrame
   * @param pythonVersion Python version (default: '3.11')
   * @returns A new DataFrame with the function applied to each group
   * @group typedrel
   * 
   * @example
   * ```typescript
   * const pythonCode = `
   * def group_func(key, rows):
   *     total = sum(row.value for row in rows)
   *     yield (key.category, total)
   * `;
   * const schema = DataTypes.createStructType([
   *   DataTypes.createStructField('category', DataTypes.StringType, false),
   *   DataTypes.createStructField('total', DataTypes.IntegerType, false),
   * ]);
   * const result = df.groupBy('category').groupMap(pythonCode, schema);
   * ```
   */
  groupMap(
    pythonCode: string,
    outputSchema: StructType,
    pythonVersion: string = '3.11'
  ): DataFrame {
    const groupingExprs = typeof this.groupingExprs[0] === "string"
      ? this.groupingExprs.map((c) => this.df.col(c as string).expr)
      : this.groupingExprs.map((c) => (c as Column).expr);

    return this.df.spark.relationBuilderToDF((rb) => {
      return rb.withGroupMap(groupingExprs, pythonCode, outputSchema, this.df.plan.relation, pythonVersion);
    });
  }
}
