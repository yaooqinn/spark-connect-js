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
import { CommandSchema, WriteOperationV2Schema, WriteOperationV2_Mode } from "../../../../gen/spark/connect/commands_pb";
import { DataFrame } from "./DataFrame";
import { Column } from "./Column";
import { expr } from "./functions";
import { AnalysisException } from "./errors";
import { ExecutePlanResponseHandler } from "./proto/ExecutePlanResponseHandler";

/**
 * Interface used to write a DataFrame to external storage using V2 data sources.
 * Provides advanced write operations (create, replace, append, overwrite) with better
 * semantics than V1.
 * 
 * Use `DataFrame.writeTo(tableName)` to access this.
 * 
 * @stable
 * @since 1.0.0
 * @see https://spark.apache.org/docs/latest/sql-data-sources-v2.html
 */
export class DataFrameWriterV2 {
  private tableName_: string;
  private df_: DataFrame;
  private provider_?: string;
  private options_: Map<string, string> = new Map();
  private tableProperties_: Map<string, string> = new Map();
  private partitionColumns_: Column[] = [];
  private clusteringColumns_: string[] = [];

  constructor(tableName: string, df: DataFrame) {
    this.tableName_ = tableName;
    this.df_ = df;
  }

  /**
   * Specify data source provider (e.g., "parquet", "orc", "iceberg", "delta")
   */
  using(provider: string): DataFrameWriterV2 {
    this.provider_ = provider;
    return this;
  }

  /**
   * Add write option
   */
  option(key: string, value: string): DataFrameWriterV2 {
    this.options_.set(key, value);
    return this;
  }

  /**
   * Add multiple options
   */
  options(opts: Record<string, string>): DataFrameWriterV2 {
    Object.entries(opts).forEach(([k, v]) => this.options_.set(k, v));
    return this;
  }

  /**
   * Add table property
   */
  tableProperty(key: string, value: string): DataFrameWriterV2 {
    this.tableProperties_.set(key, value);
    return this;
  }

  /**
   * Partition by columns.
   * Note: Each call to partitionBy replaces previously set partition columns.
   */
  partitionBy(...cols: (string | Column)[]): DataFrameWriterV2 {
    this.partitionColumns_ = cols.map(c => 
      typeof c === 'string' ? new Column(c) : c
    );
    return this;
  }

  /**
   * Cluster by columns (for data sources that support clustering).
   * Note: Each call to clusterBy replaces previously set clustering columns.
   */
  clusterBy(...cols: string[]): DataFrameWriterV2 {
    this.clusteringColumns_ = cols;
    return this;
  }

  /**
   * Create new table
   */
  create(): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('create');
  }

  /**
   * Replace existing table
   */
  replace(): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('replace');
  }

  /**
   * Create or replace table
   */
  createOrReplace(): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('createOrReplace');
  }

  /**
   * Append to existing table
   */
  append(): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('append');
  }

  /**
   * Overwrite matching rows
   */
  overwrite(condition: Column | string): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('overwrite', condition);
  }

  /**
   * Overwrite partitions
   */
  overwritePartitions(): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWrite('overwritePartitions');
  }

  private async executeWrite(
    mode: string,
    condition?: Column | string
  ): Promise<ExecutePlanResponseHandler[]> {
    const writeOp = create(WriteOperationV2Schema, {
      input: this.df_.plan.relation,
      tableName: this.tableName_,
      provider: this.provider_,
      partitioningColumns: this.partitionColumns_.map(c => c.expr),
      options: Object.fromEntries(this.options_),
      tableProperties: Object.fromEntries(this.tableProperties_),
      mode: this.getModeProto(mode)
    });

    if (this.clusteringColumns_.length > 0) {
      writeOp.clusteringColumns = this.clusteringColumns_;
    }

    if (condition) {
      writeOp.overwriteCondition =
        typeof condition === 'string'
          ? expr(condition).expr
          : condition.expr;
    }

    const writeCmd = create(CommandSchema, { 
      commandType: { case: "writeOperationV2", value: writeOp }
    });
    
    return this.df_.spark.execute(writeCmd);
  }

  private getModeProto(mode: string): WriteOperationV2_Mode {
    const modeMap: Record<string, WriteOperationV2_Mode> = {
      'create': WriteOperationV2_Mode.CREATE,
      'replace': WriteOperationV2_Mode.REPLACE,
      'createOrReplace': WriteOperationV2_Mode.CREATE_OR_REPLACE,
      'append': WriteOperationV2_Mode.APPEND,
      'overwrite': WriteOperationV2_Mode.OVERWRITE,
      'overwritePartitions': WriteOperationV2_Mode.OVERWRITE_PARTITIONS
    };
    const protoMode = modeMap[mode];
    if (protoMode === undefined) {
      const validModes = Object.keys(modeMap).map(m => `"${m}"`).join(', ');
      throw new AnalysisException(
        "INVALID_WRITE_MODE_V2",
        `The specified write mode "${mode}" is invalid. Valid modes include ${validModes}.`,
        "42000"
      );
    }
    return protoMode;
  }
}
