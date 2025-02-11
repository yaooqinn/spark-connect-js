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

import { DataFrameWriter } from './DataFrameWriter';
import { Row } from './Row';
import { SparkResult } from './SparkResult';
import { SparkSession } from './SparkSession';
import { DataTypes } from './types/DataTypes';
import { StructType } from './types/StructType';
import { AnalyzePlanRequestBuilder } from './proto/AnalyzePlanRequestBuilder';
import { AnalyzePlanResponseHandler } from './proto/AnalyzePlanResponeHandler';
import { RelationBuilder } from './proto/RelationBuilder';
import { StorageLevel } from '../storage/StorageLevel';
import { LogicalPlan } from './proto/LogicalPlan';
import { Column } from './Column';
import { expr } from './functions';

export class DataFrame {
  private cachedSchema_: StructType | undefined = undefined;

  constructor(public readonly spark: SparkSession, public readonly plan: LogicalPlan) {}

  toDF(...cols: string[]): DataFrame {
    if (cols.length === 0) {
      return this;
    } else {
      return this.toNewDataFrame(b => b.withToDf(cols, this.plan.relation));
    }
  }

  to(schema: StructType): DataFrame {
    return this.toNewDataFrame(b => b.withToSchema(schema, this.plan.relation));
  }

  /**
   * Returns the schema of this DataFrame.
   */
  async schema(): Promise<StructType> {
    if (this.cachedSchema_) {
      return this.cachedSchema_;
    }
    return this.analyze(b => b.setSchema(this.plan.plan)).then(async resp => {
      this.cachedSchema_ = DataTypes.fromProtoType(resp.schema) as StructType;
      return this.cachedSchema_;
    });
  }

  async printSchema(level: number = 0): Promise<void> {
    return this.printSchema0(b => b.withTreeString(this.plan.plan, level)).then(console.log);
  }
  /** @ignore */
  async printSchema0(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<string> {
    return this.analyze(f).then(resp => resp.treeString);
  }

  async explain(): Promise<void>;
  async explain(mode: string): Promise<void>;
  async explain(mode: boolean): Promise<void>;
  async explain(mode?: any): Promise<void> {
    return this.explain0(b => b.withExplain(this.plan.plan, mode)).then(console.log);
  }
  /** @ignore */
  async explain0(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<string> {
    return this.analyze(f).then(r=> r.explain);
  }

  async dtypes(): Promise<Array<[string, string]>> {
    return this.schema().then(s => s.fields.map(field => [field.name, field.dataType.toString()]));
  }

  async columns(): Promise<string[]> {
    return this.schema().then(s => s.fieldNames());
  }

  async isEmpty(): Promise<boolean> {
    return this.head(1).then(rows => rows.length === 0);
  }

  async isLocal(): Promise<boolean> {
    return this.analyze(b => b.withIsLocal(this.plan.plan)).then(r => r.isLocal);
  }

  async isStreaming(): Promise<boolean> {
    return this.analyze(b => b.withIsStreaming(this.plan.plan)).then(r => r.isStreaming);
  }

  async checkpoint(): Promise<DataFrame>;
  async checkpoint(eager: boolean): Promise<DataFrame>;
  async checkpoint(eager?: boolean, storageLevel?: StorageLevel): Promise<DataFrame> {
    throw new Error("Not implemented"); // TODO
  }
  async localCheckpoint(): Promise<DataFrame>;
  async localCheckpoint(eager: boolean): Promise<DataFrame>;
  async localCheckpoint(eager?: boolean, storageLevel?: StorageLevel): Promise<DataFrame> {
    throw new Error("Not implemented"); // TODO
  }

  async withWatermark(eventTime: string, delayThreshold: string): Promise<DataFrame> {
    throw new Error("Not implemented"); // TODO
  }

  async inputFiles(): Promise<string[]> {
    return this.analyze(b => b.withInputFiles(this.plan.plan)).then(r => r.inputFiles);
  }

  async sameSemantics(other: DataFrame): Promise<boolean> {
    return this.analyze(b => b.withSameSemantics(this.plan.plan, other.plan.plan)).then(r => r.sameSemantics);
  }

  async semanticHash(): Promise<number> {
    return this.analyze(b => b.withSemanticHash(this.plan.plan)).then(r => r.semanticHash);
  }

  /**
   * Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`).
   */
  async persist(): Promise<DataFrame>;
  /**
   * Persist this DataFrame with the given storage level.
   *
   * @param newLevel a storage level. @see [[StorageLevel]]
   */
  async persist(newLevel: StorageLevel): Promise<DataFrame>;
  async persist(newLevel?: StorageLevel): Promise<DataFrame> {
    return this.analyze(b => b.withPersist(this.plan.relation, newLevel)).then(() => this);
  }
  /**
   * Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`).
   */
  async cache(): Promise<DataFrame> {
    return this.persist();
  }

  /**
   * Mark the DataFrame as non-persistent, and remove all blocks for it from memory and disk. This
   * will not un-persist any cached data that is built upon this Dataset.
   *
   * @param blocking
   *   Whether to block until all blocks are deleted.
   */
  async unpersist(blocking: boolean = false): Promise<DataFrame> {
    return this.analyze(b => b.withUnpersist(this.plan.relation, blocking)).then(() => this);
  }

  /**
   * Get the DataFrame's current storage level, or StorageLevel.NONE if not persisted.
   */
  async storageLevel(): Promise<StorageLevel> {
    return this.analyze(b => b.withGetStorageLevel(this.plan.relation)).then(r => r.getStorageLevel);
  }

  get write(): DataFrameWriter {
    return new DataFrameWriter(this);
  }

  async collect(): Promise<Row[]> {
    return this.withResult(res => {
      return res.toArray();
    });
  };

  limit(n: number): DataFrame {
    return this.toNewDataFrame(b => b.withLimit(n, this.plan.relation));
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
    return this.toNewDataFrame(b => b.withOffset(n, this.plan.relation));
  }

  tail(n: number): Promise<Row[]> {
    return this.toNewDataFrame(b => b.withTail(n, this.plan.relation)).collect();
  }

  /**
   * Displays the Dataset in a tabular form. For example:
   * @param numRows Number of rows to show
   * @param truncate If set to `true`, truncate the displayed columns to 20 characters, default is `true`
   * @param vertical If set to `true`, print output rows vertically (one line per column value)
   */
  async show(): Promise<void>;
  async show(numRows: number): Promise<void>;
  async show(numRows: number, truncate: boolean | number): Promise<void>;
  async show(numRows: number, truncate: boolean | number, vertical: boolean): Promise<void>;
  async show(numRows: number = 20, truncate: boolean | number = true, vertical = false): Promise<void> {
    const truncateValue: number = typeof truncate === "number" ? truncate : (truncate ? 20 : 0);
    const plan = this.spark.planFromRelationBuilder(builder => {
      builder.withShowString(numRows, truncateValue, vertical, this.plan.relation);
    });
    return this.withResult(res => {
      console.log(res.toArray()[0].getString(0));
    }, plan);
  };

  select(...cols: string[]): DataFrame;
  select(...cols: Column[]): DataFrame;
  select(...cols: string[] | Column[]): DataFrame {
    const exprs = cols.map(col => typeof col === "string" ? new Column(col) : col ).map(col => col.expr);
    return this.toNewDataFrame(b => b.withProject(exprs, this.plan.relation));
  }

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   *   df.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   *   // TODO: support expr(..) function
   * }}}
   *
   */
  selectExpr(...cols: string[]): DataFrame {
    return this.select(...cols);
  }

  /**
   * Selects column based on the column name and returns it as a [[org.apache.spark.sql.Column]].
   *
   * @note
   *   The column name can also reference to a nested column like `a.b`.
   * @param colName string column name
   * @return {Column} Column
   */
  col(colName: string): Column {
    return new Column(colName, this.plan.planId);
  }

  /**
   * Selects column based on the column name specified as a regex and returns it as
   * [[org.apache.spark.sql.Column]].
   * @param {string} colName string column name specified as a regex
   * @return {Column} Column
   */
  colRegex(colName: string): Column {
    return new Column(b => b.withUnresolvedRegex(colName, this.plan.planId));
  }

  /**
   * Selects a metadata column based on its logical column name, and returns it as a
   * [[org.apache.spark.sql.Column]].
   *
   * A metadata column can be accessed this way even if the underlying data source defines a data
   * column with a conflicting name.
   *
   * @param colName string column name
   * @return {Column} Column
   */
  metadataColumn(colName: string): Column {
    return new Column(b => b.withUnresolvedAttribute(colName, this.plan.planId, true));
  }

  filter(condition: Column): DataFrame;
  filter(conditionExpr: string): DataFrame;
  filter(condition: Column | string): DataFrame {
    const cond = typeof condition === "string" ? expr(condition) : condition;
    return this.toNewDataFrame(b => b.withFilter(cond.expr, this.plan.relation));
  }

  where(condition: Column): DataFrame;
  where(conditionExpr: string): DataFrame;
  where(condition: Column | string): DataFrame {
    if (typeof condition === "string") {
      return this.filter(condition);
    } else {
      return this.filter(condition);
    }
  }
  private async collectResult(plan: LogicalPlan = this.plan): Promise<SparkResult> {
    return this.spark.client.execute(plan.plan).then(resps => {
      return new SparkResult(resps[Symbol.iterator]());
    });
  }

  private async withResult<E>(func: (result: SparkResult) => E, plan: LogicalPlan = this.plan): Promise<E> {
    return this.collectResult(plan).then(func);
  }

  private toNewDataFrame(f: (builder: RelationBuilder) => void): DataFrame {
    return this.spark.relationBuilderToDF(f);
  }

  private async analyze(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<AnalyzePlanResponseHandler> {
    return this.spark.analyze(f);
  }
}