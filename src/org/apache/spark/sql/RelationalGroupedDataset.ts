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

import { Aggregate_GroupingSets, Aggregate_Pivot } from "../../../../gen/spark/connect/relations_pb";
import { Column } from "./Column";
import { DataFrame } from "./DataFrame";
import * as f  from "./functions";
import { GroupType } from "./proto/aggregate/GroupType";
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
    public readonly pivot: Aggregate_Pivot | undefined = undefined,
    public readonly groupingSets: Aggregate_GroupingSets[] = []
  ) {}

  toDF(...aggExprs: Column[]): DataFrame {
    return this.df.spark.relationBuilderToDF((rb) => {
      return rb.withAggregateBuilder((ab) => {
        if (typeof this.groupingExprs[0] === "string") {
          ab.withGroupingExpressions(this.groupingExprs.map((c) => this.df.col(c as string).expr))
        } else {
          ab.withGroupingExpressions(this.groupingExprs.map((c) => (c as Column).expr))
        }
        return ab.withInput(this.df.plan.relation)
          .withAggregateExpressions(aggExprs.map((c) => c.expr))
          .withPivot(this.pivot)
          .withGroupingSets(this.groupingSets)
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
}
