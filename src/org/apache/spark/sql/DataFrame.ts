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

import * as b from '../../../../gen/spark/connect/base_pb';
import * as r from '../../../../gen/spark/connect/relations_pb';
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

export class DataFrame {
  private cachedSchema_: StructType | undefined = undefined;

  constructor(public spark: SparkSession, public plan: b.Plan) {}

  toDF(...cols: string[]): DataFrame {
    if (cols.length === 0) {
      return this;
    } else {
      return this.toNewDataFrame(b => b.withToDf(cols, this.getPlanRelation()));
    }
  }

  to(schema: StructType): DataFrame {
    return this.toNewDataFrame(b => b.withToSchema(schema, this.getPlanRelation()));
  }

  /**
   * Returns the schema of this DataFrame.
   */
  async schema(): Promise<StructType> {
    if (this.cachedSchema_) {
      return this.cachedSchema_;
    }
    return this.analyze(b => b.setSchema(this.plan)).then(async resp => {
      this.cachedSchema_ = DataTypes.fromProtoType(resp.schema) as StructType;
      return this.cachedSchema_;
    });
  }

  async printSchema(level: number = 0): Promise<void> {
    return this.printSchema0(b => b.withTreeString(this.plan, level)).then(console.log);
  }
  /** @ignore */
  async printSchema0(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<string> {
    return this.analyze(f).then(resp => resp.treeString);
  }

  async explain(): Promise<void>;
  async explain(mode: string): Promise<void>;
  async explain(mode: boolean): Promise<void>;
  async explain(mode?: any): Promise<void> {
    return this.explain0(b => b.withExplain(this.plan, mode)).then(console.log);
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
    return this.analyze(b => b.withIsLocal(this.plan)).then(r => r.isLocal);
  }

  async isStreaming(): Promise<boolean> {
    return this.analyze(b => b.withIsStreaming(this.plan)).then(r => r.isStreaming);
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
    return this.analyze(b => b.withInputFiles(this.plan)).then(r => r.inputFiles);
  }

  async sameSemantics(other: DataFrame): Promise<boolean> {
    return this.analyze(b => b.withSameSemantics(this.plan, other.plan)).then(r => r.sameSemantics);
  }

  async semanticHash(): Promise<number> {
    return this.analyze(b => b.withSemanticHash(this.plan)).then(r => r.semanticHash);
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
    return this.analyze(b => b.withPersist(this.getPlanRelation(), newLevel)).then(() => this);
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
    return this.analyze(b => b.withUnpersist(this.getPlanRelation(), blocking)).then(() => this);
  }

  /**
   * Get the DataFrame's current storage level, or StorageLevel.NONE if not persisted.
   */
  async storageLevel(): Promise<StorageLevel> {
    return this.analyze(b => b.withGetStorageLevel(this.getPlanRelation())).then(r => r.getStorageLevel);
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
    return this.toNewDataFrame(b => b.withLimit(n, this.getPlanRelation()));
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
    return this.toNewDataFrame(b => b.withOffset(n, this.getPlanRelation()));
  }

  tail(n: number): Promise<Row[]> {
    return this.toNewDataFrame(b => b.withTail(n, this.getPlanRelation())).collect();
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
      builder.withShowString(numRows, truncateValue, vertical, this.getPlanRelation());
    });

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

  private toNewDataFrame(f: (builder: RelationBuilder) => void): DataFrame {
    return this.spark.dataFrameFromRelationBuilder(f);
  }

  private async analyze(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<AnalyzePlanResponseHandler> {
    return this.spark.analyze(f);
  }
}