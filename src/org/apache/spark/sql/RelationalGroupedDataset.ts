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

import { Aggregate_Pivot } from "../../../../gen/spark/connect/relations_pb";
import { Column } from "./Column";
import { DataFrame } from "./DataFrame";
import * as f from "./functions";
import { GroupType } from "./proto/aggregate/GroupType";
import { toPivotPB, toGroupingSetsPB } from "./proto/expression/utils";
import { toGroupTypePB } from "./proto/ProtoUtils";

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
}
