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

import { create } from '@bufbuild/protobuf';
import * as b from '../../../../gen/spark/connect/base_pb';
import * as r from '../../../../gen/spark/connect/relations_pb';
import { logger } from '../logger';
import { DataFrameWriter } from './DataFrameWriter';
import { Row } from './Row';
import { SparkResult } from './SparkResult';
import { SparkSession } from './SparkSession';
import { DataTypes } from './types/DataTypes';
import { StructType } from './types/StructType';
import { PlanBuilder } from './proto/ProtoUtils';

export class DataFrame {
  private cachedSchema_: StructType | undefined = undefined;

  constructor(public spark: SparkSession, public plan: b.Plan) {}

  toDF(...cols: string[]): DataFrame {
    if (cols.length === 0) {
      return this;
    } else {
      const newPlan = new PlanBuilder().withRelationBuilder(builder => {
        builder
          .setRelationCommon(this.spark.newRelationCommon())
          .setToDf(cols, this.getPlanRelation());
      }).build();
      return new DataFrame(this.spark, newPlan);
    }
  }

  to(schema: StructType): DataFrame {
    const newPlan = new PlanBuilder().withRelationBuilder(builder => {
      builder
        .setRelationCommon(this.spark.newRelationCommon())
        .setToSchema(schema, this.getPlanRelation());
    }).build();
    return new DataFrame(this.spark, newPlan);
  }

  /**
   * Returns the schema of this Dataset.
   */
  async schema(): Promise<StructType> {
    if (this.cachedSchema_) {
      return this.cachedSchema_;
    }

    const planReqSchema = create(b.AnalyzePlanRequest_SchemaSchema,
      { plan: this.plan});

    return this.spark.client.analyze(req =>
      req.analyze = { value: planReqSchema, case: "schema"}).then(resp => {
        if (resp.result.case === "schema" && resp.result.value.schema) {
          logger.debug("Schema in protobuf:", JSON.stringify(resp.result.value.schema));
          this.cachedSchema_ = DataTypes.fromProtoType(resp.result.value.schema) as StructType;
          logger.debug("Schema in typecript:", this.cachedSchema_);
          return this.cachedSchema_;
        } else {
          throw new Error("Failed to get schema");
        }
      });
  }

  printSchema(level: number = 0): void {
    // TODO: Implement printSchema and treeString for [[StructType]]
    throw new Error("Method not implemented.");
  }

  async explain(): Promise<void>;
  async explain(mode: string): Promise<void>;
  async explain(mode: boolean): Promise<void>;
  async explain(mode?: any): Promise<void> {
    let modeStr;
    if (mode === undefined) {
      modeStr = "simple";
    } else if (typeof mode === "boolean") {
      modeStr = mode ? "extended" : "simple";
    } else {
      modeStr = mode;
    }
    const explain = create(b.AnalyzePlanRequest_ExplainSchema, {
      plan: this.plan,
    });
    switch (modeStr.trim().toLowerCase()) {
      case "simple":
        explain.explainMode = b.AnalyzePlanRequest_Explain_ExplainMode.SIMPLE;
        break;
      case "extended":
        explain.explainMode = b.AnalyzePlanRequest_Explain_ExplainMode.EXTENDED;
        break;
      case "codegen":
        explain.explainMode = b.AnalyzePlanRequest_Explain_ExplainMode.CODEGEN;
        break;
      case "cost":
        explain.explainMode = b.AnalyzePlanRequest_Explain_ExplainMode.COST;
        break;
      case "formatted":
        explain.explainMode = b.AnalyzePlanRequest_Explain_ExplainMode.FORMATTED;
        break;
      default:
        throw new Error("Unsupported explain mode: " + mode);
  }

  return this.spark.client.analyze(req => { req.analyze = { value: explain, case: "explain"} })
    .then(resp => {
      if (resp.result.case === "explain") {
        console.log(resp.result.value.explainString);
      }
    });
  }

  // def dtypes: Array[(String, String)] = schema.fields.map { field =>
  //   (field.name, field.dataType.toString)
  // }
  async dtypes(): Promise<Array<[string, string]>> {
    return this.schema().then(s => s.fields.map(field => [field.name, field.dataType.toString()]));
  }

  async columns(): Promise<string[]> {
    return this.schema().then(s => s.fieldNames());
  }

  async isEmpty(): Promise<boolean> {
    return this.head(1).then(rows => rows.length === 0);
  }

  isLocal(): boolean {
    return this.plan.opType.case === 'root' && this.plan.opType.value.relType.case === "localRelation";
  }

  write(): DataFrameWriter {
    return new DataFrameWriter(this);
  }

  async collect(): Promise<Row[]> {
    return this.withResult(res => {
      return res.toArray();
    });
  };

  limit(n: number): DataFrame {
    const limitPlan = new PlanBuilder().withRelationBuilder(builder => {
      builder
        .setRelationCommon(this.spark.newRelationCommon())
        .setLimit(n, this.getPlanRelation());
    }).build();
    return new DataFrame(this.spark, limitPlan);
  }

  async head(): Promise<Row>;
  async head(n: number): Promise<Row[]>;
  async head(n?: number): Promise<Row[] | Row> {
    if (n) {
      return this.limit(n).collect();
    } else {
      return this.limit(1).collect().then(rows => rows[0]);
    }
  };
  async first(): Promise<Row> {
    return this.head();
  };
  async take(n: number): Promise<Row[]> {
    return this.head(n);
  };

  offset(n: number): DataFrame {
    const offsetPlan = new PlanBuilder().withRelationBuilder(builder => {
      builder
        .setRelationCommon(this.spark.newRelationCommon())
        .setOffset(n, this.getPlanRelation());
    }).build();
    return new DataFrame(this.spark, offsetPlan);
  }

  tail(n: number): Promise<Row[]> {
    const tailPlan = new PlanBuilder().withRelationBuilder(builder => {
      builder
        .setRelationCommon(this.spark.newRelationCommon())
        .setTail(n, this.getPlanRelation());
    }).build();
    return new DataFrame(this.spark, tailPlan).collect();
  }

  // async head(n: number = 1): Promise<Row[]> {
  //   return this.withResult(res => {
  //     return res.toArray(n);
  //   });
  // }

  select(...cols: string[]): DataFrame {
    create(r.ProjectSchema, {});
    return this;
  }

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   *
   * If `vertical` enabled, this command prints output rows vertically (one line per column
   * value)?
   *
   * {{{
   * -RECORD 0-------------------
   *  year            | 1980
   *  month           | 12
   *  AVG('Adj Close) | 0.503218
   *  AVG('Adj Close) | 0.595103
   * -RECORD 1-------------------
   *  year            | 1981
   *  month           | 01
   *  AVG('Adj Close) | 0.523289
   *  AVG('Adj Close) | 0.570307
   * -RECORD 2-------------------
   *  year            | 1982
   *  month           | 02
   *  AVG('Adj Close) | 0.436504
   *  AVG('Adj Close) | 0.475256
   * -RECORD 3-------------------
   *  year            | 1983
   *  month           | 03
   *  AVG('Adj Close) | 0.410516
   *  AVG('Adj Close) | 0.442194
   * -RECORD 4-------------------
   *  year            | 1984
   *  month           | 04
   *  AVG('Adj Close) | 0.450090
   *  AVG('Adj Close) | 0.483521
   * }}}
   *
   * @param numRows Number of rows to show
   * @param truncate If set to `true`, truncate the displayed columns to 20 characters, default is `true`
   * @param vertical If set to `true`, print output rows vertically (one line per column value)
   */
  async show(numRows: number = 20, truncate: boolean | number = true, vertical = false): Promise<void> {
    const truncateValue: number = typeof truncate === "number" ? truncate : (truncate ? 20 : 0);
    const showString = create(r.ShowStringSchema, {
      input: this.plan.opType.value as r.Relation,
      numRows: numRows, truncate: truncateValue,
      vertical: vertical });
    const plan = this.spark.newPlanWithRelation(r => r.relType = { case: "showString", value: showString });

    return this.withResult(res => {
      console.log(res.toArray()[0].getString(0));
    }, plan);
  };

  private async collectResult(plan: b.Plan = this.plan): Promise<SparkResult> {
    return this.spark.client.execute(plan).then(resps => {
      return new SparkResult(resps[Symbol.iterator]());
    });
  }

  private async withResult<E>(func: (result: SparkResult) => E, plan: b.Plan = this.plan): Promise<E> {
    return this.collectResult(plan).then(func);
  }

  private getPlanRelation(): r.Relation {
    if (this.plan.opType.case === "root") {
      return this.plan.opType.value as r.Relation;
    } else {
      throw new Error("Plan does not contain a relation");
    }
  }
}