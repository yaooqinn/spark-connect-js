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

import { DataFrame } from "./DataFrame";
import { Column } from "./Column";
import { expr } from "./functions";
import { ExecutePlanResponseHandler } from "./proto/ExecutePlanResponseHandler";
import { WriteOperationV2Builder } from "./proto/WriteOperationV2Builder";
import { CommandBuilder } from "./proto/CommandBuilder";

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
    const builder = new WriteOperationV2Builder()
      .withInput(this.df_.plan.relation)
      .withTableName(this.tableName_)
      .withOptions(Object.fromEntries(this.options_))
      .withTableProperties(Object.fromEntries(this.tableProperties_))
      .withMode(mode);

    if (this.provider_) {
      builder.withProvider(this.provider_);
    }

    if (this.partitionColumns_.length > 0) {
      builder.withPartitioningColumns(this.partitionColumns_.map(c => c.expr));
    }

    if (this.clusteringColumns_.length > 0) {
      builder.withClusteringColumns(this.clusteringColumns_);
    }

    if (condition) {
      const conditionExpr = typeof condition === 'string'
        ? expr(condition).expr
        : condition.expr;
      builder.withOverwriteCondition(conditionExpr);
    }

    const writeOp = builder.build();
    const command = new CommandBuilder()
      .withWriteOperationV2(writeOp)
      .build();
    
    return this.df_.spark.execute(command);
  }
}
