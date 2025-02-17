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
import { Aggregate, Aggregate_GroupingSets, Aggregate_GroupType, Aggregate_Pivot, AggregateSchema, Relation } from "../../../../../../gen/spark/connect/relations_pb";
import { Expression } from "../../../../../../gen/spark/connect/expressions_pb";

export class AggregateBuilder {
  private aggregate: Aggregate = create(AggregateSchema, {})
  constructor() { }

  withInput(input?: Relation) {
    this.aggregate.input = input;
    return this;
  }
  withGroupType(groupType: Aggregate_GroupType) {
    this.aggregate.groupType = groupType;
    return this;
  }
  withGroupingExpressions(groupingExprs: Expression[]) {
    this.aggregate.groupingExpressions = groupingExprs;
    return this;
  }
  withAggregateExpressions(aggregateExprs: Expression[]) {
    this.aggregate.aggregateExpressions = aggregateExprs;
    return this;
  }
  withPivot(pivot?: Aggregate_Pivot) {
    this.aggregate.pivot = pivot;
    return this;
  }
  withGroupingSets(groupingSets: Aggregate_GroupingSets[]) {
    this.aggregate.groupingSets = groupingSets;
    return this;
  }
  build(): Aggregate {
    return this.aggregate;
  }
}