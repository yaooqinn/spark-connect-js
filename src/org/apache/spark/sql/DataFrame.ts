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
import { DataFrameWriterV2 } from './DataFrameWriterV2';
import { DataFrameStatFunctions } from './DataFrameStatFunctions';
import { DataFrameNaFunctions } from './DataFrameNaFunctions';
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
import { RelationalGroupedDataset } from './RelationalGroupedDataset';
import { GroupType } from './proto/aggregate/GroupType';

/**
 * A distributed collection of data organized into named columns.
 * 
 * @remarks
 * DataFrame is the primary abstraction in Spark SQL for working with structured data.
 * It provides a domain-specific language for distributed data manipulation and supports
 * a wide variety of operations including selecting, filtering, joining, and aggregating.
 * 
 * DataFrames are lazy - operations are not executed until an action (like collect, count, or show)
 * is called. This allows Spark to optimize the execution plan.
 * 
 * @example
 * ```typescript
 * // Create a DataFrame from a table
 * const df = spark.table("users");
 * 
 * // Select and filter
 * const result = df.select("name", "age")
 *                  .filter(col("age").gt(21));
 * 
 * // Show results
 * await result.show();
 * 
 * // Perform aggregations
 * const avgAge = await df.groupBy("department")
 *                        .avg("age")
 *                        .collect();
 * ```
 * 
 * @group core
 * @since 1.0.0
 */
export class DataFrame {
  private cachedSchema_: StructType | undefined = undefined;

  constructor(public readonly spark: SparkSession, public readonly plan: LogicalPlan) {}

  /**
   * Returns a new DataFrame with columns renamed.
   * 
   * @param cols - New column names. If empty, returns this DataFrame unchanged.
   * @returns A new DataFrame with the specified column names
   * 
   * @example
   * ```typescript
   * // Rename columns
   * const df2 = df.toDF("name", "age", "city");
   * ```
   * 
   * @group dfops
   */
  toDF(...cols: string[]): DataFrame {
    if (cols.length === 0) {
      return this;
    } else {
      return this.toNewDataFrame(b => b.withToDf(cols, this.plan.relation));
    }
  }

  /**
   * Returns a new DataFrame with the specified schema applied.
   * 
   * @param schema - The schema to apply to this DataFrame
   * @returns A new DataFrame with the specified schema
   * 
   * @example
   * ```typescript
   * const newSchema = new StructType([
   *   new StructField("name", DataTypes.StringType),
   *   new StructField("age", DataTypes.IntegerType)
   * ]);
   * const df2 = df.to(newSchema);
   * ```
   * 
   * @group dfops
   */
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
    // eslint-disable-next-line no-console
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
    // eslint-disable-next-line no-console
    return this.explain0(b => b.withExplain(this.plan.plan, mode)).then(console.log);
  }
  /** @ignore */
  async explain0(f: (builder: AnalyzePlanRequestBuilder) => void): Promise<string> {
    return this.analyze(f).then(r=> r.explain);
  }

  /**
   * Returns all column names and their data types as an array of tuples.
   * 
   * @returns A promise that resolves to an array of [columnName, dataType] tuples
   * 
   * @example
   * ```typescript
   * const types = await df.dtypes();
   * // Returns: [["name", "string"], ["age", "integer"], ...]
   * ```
   * 
   * @group basic
   */
  async dtypes(): Promise<Array<[string, string]>> {
    return this.schema().then(s => s.fields.map(field => [field.name, field.dataType.toString()]));
  }

  /**
   * Returns all column names as an array.
   * 
   * @returns A promise that resolves to an array of column names
   * 
   * @example
   * ```typescript
   * const cols = await df.columns();
   * // Returns: ["name", "age", "city"]
   * ```
   * 
   * @group basic
   */
  async columns(): Promise<string[]> {
    return this.schema().then(s => s.fieldNames());
  }

  /**
   * Returns true if this DataFrame contains zero rows.
   * 
   * @returns A promise that resolves to true if the DataFrame is empty, false otherwise
   * 
   * @example
   * ```typescript
   * const empty = await df.isEmpty();
   * if (empty) {
   *   console.log("No data found");
   * }
   * ```
   * 
   * @group basic
   */
  async isEmpty(): Promise<boolean> {
    return this.head(1).then(rows => rows.length === 0);
  }

  /**
   * Returns true if the collect and take methods can be run locally without any Spark executors.
   * 
   * @returns A promise that resolves to true if operations can run locally
   * 
   * @example
   * ```typescript
   * const local = await df.isLocal();
   * console.log(`Can run locally: ${local}`);
   * ```
   * 
   * @group basic
   */
  async isLocal(): Promise<boolean> {
    return this.analyze(b => b.withIsLocal(this.plan.plan)).then(r => r.isLocal);
  }

  /**
   * Returns true if this DataFrame contains one or more sources that continuously return data as it arrives.
   * 
   * @returns A promise that resolves to true if this is a streaming DataFrame
   * 
   * @example
   * ```typescript
   * const streaming = await df.isStreaming();
   * if (streaming) {
   *   console.log("This is a streaming DataFrame");
   * }
   * ```
   * 
   * @group streaming
   */
  async isStreaming(): Promise<boolean> {
    return this.analyze(b => b.withIsStreaming(this.plan.plan)).then(r => r.isStreaming);
  }

  /**
   * Returns a checkpointed version of this DataFrame. Checkpointing can be used to truncate the
   * logical plan of this DataFrame, which is especially useful in iterative algorithms where the
   * plan may grow exponentially. It will be saved to files inside the checkpoint
   * directory set with `spark.sql.checkpoint.location`.
   *
   * @param eager
   *   Whether to checkpoint this DataFrame immediately (default is true).
   *   If false, the checkpoint will be performed when the DataFrame is first materialized.
   * @group basic
   */
  async checkpoint(eager: boolean = true): Promise<DataFrame> {
    if (!this.plan.relation) {
      throw new Error('DataFrame plan must have a relation for checkpoint operation');
    }
    const plan = this.spark.planFromCommandBuilder(b =>
      b.withCheckpointCommand(this.plan.relation, false, eager)
    );
    await this.spark.client.execute(plan.plan);
    return this;
  }

  /**
   * Returns a locally checkpointed version of this DataFrame. Checkpointing can be used to truncate
   * the logical plan of this DataFrame, which is especially useful in iterative algorithms where the
   * plan may grow exponentially. It will be saved to a local temporary directory.
   *
   * This is a local checkpoint and is less reliable than a regular checkpoint because it is stored
   * in executor storage and may be lost if executors fail.
   *
   * @param eager
   *   Whether to checkpoint this DataFrame immediately (default is true).
   *   If false, the checkpoint will be performed when the DataFrame is first materialized.
   * @param storageLevel
   *   The storage level to use for the local checkpoint. If not specified, the default storage level is used.
   * @group basic
   */
  async localCheckpoint(eager: boolean = true, storageLevel?: StorageLevel): Promise<DataFrame> {
    if (!this.plan.relation) {
      throw new Error('DataFrame plan must have a relation for localCheckpoint operation');
    }
    const plan = this.spark.planFromCommandBuilder(b =>
      b.withCheckpointCommand(this.plan.relation, true, eager, storageLevel)
    );
    await this.spark.client.execute(plan.plan);
    return this;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
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

  /**
   * Create a write builder for writing to a table using V2 API
   */
  writeTo(tableName: string): DataFrameWriterV2 {
    return new DataFrameWriterV2(tableName, this);
  }

  get stat(): DataFrameStatFunctions {
    return new DataFrameStatFunctions(this);
  }

  get na(): DataFrameNaFunctions {
    return new DataFrameNaFunctions(this);
  }

  /**
   * Returns all rows in this DataFrame as an array of Row objects.
   * 
   * @returns A promise that resolves to an array of Row objects
   * 
   * @remarks
   * This is an action that triggers execution of the DataFrame's computation.
   * Use with caution on large datasets as it collects all data to the driver.
   * 
   * @example
   * ```typescript
   * const rows = await df.collect();
   * rows.forEach(row => {
   *   console.log(row.getString(0), row.getInt(1));
   * });
   * ```
   * 
   * @group action
   */
  async collect(): Promise<Row[]> {
    return this.withResult(res => {
      return res.toArray();
    });
  }

  /**
   * Returns a new DataFrame by taking the first n rows.
   * 
   * @param n - The number of rows to take
   * @returns A new DataFrame with at most n rows
   * 
   * @example
   * ```typescript
   * const top10 = df.limit(10);
   * await top10.show();
   * ```
   * 
   * @group dfops
   */
  limit(n: number): DataFrame {
    return this.toNewDataFrame(b => b.withLimit(n, this.plan.relation));
  }

  /**
   * Returns the first row.
   * 
   * @returns A promise that resolves to the first Row
   * 
   * @group action
   */
  async head(): Promise<Row>;
  /**
   * Returns the first n rows.
   * 
   * @param n - The number of rows to return
   * @returns A promise that resolves to an array of Row objects
   * 
   * @example
   * ```typescript
   * const firstRow = await df.head();
   * const first5 = await df.head(5);
   * ```
   * 
   * @group action
   */
  async head(n: number): Promise<Row[]>;
  async head(n?: number): Promise<Row[] | Row> {
    if (n) {
      return this.limit(n).collect();
    } else {
      return this.limit(1).collect().then(rows => rows[0]);
    }
  }
  /**
   * Returns the first row. Alias for head().
   * 
   * @returns A promise that resolves to the first Row
   * 
   * @group action
   */
  async first(): Promise<Row> {
    return this.head();
  }
  /**
   * Returns the first n rows. Alias for head(n).
   * 
   * @param n - The number of rows to return
   * @returns A promise that resolves to an array of Row objects
   * 
   * @group action
   */
  async take(n: number): Promise<Row[]> {
    return this.head(n);
  }

  /**
   * Returns a new DataFrame by skipping the first n rows.
   * 
   * @param n - The number of rows to skip
   * @returns A new DataFrame with the first n rows removed
   * 
   * @example
   * ```typescript
   * const skipped = df.offset(100);
   * await skipped.show();
   * ```
   * 
   * @group dfops
   */
  offset(n: number): DataFrame {
    return this.toNewDataFrame(b => b.withOffset(n, this.plan.relation));
  }

  /**
   * Returns the last n rows in the DataFrame.
   * 
   * @param n - The number of rows to return from the end
   * @returns A promise that resolves to an array of Row objects
   * 
   * @example
   * ```typescript
   * const lastRows = await df.tail(10);
   * ```
   * 
   * @group action
   */
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
      // eslint-disable-next-line no-console
      console.log(res.toArray()[0].getString(0));
    }, plan);
  }

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
    return this.select(...cols.map(col => expr(col)));
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

  /**
   * Filters rows using the given Column condition.
   * 
   * @param condition - A Column representing the filter condition
   * @returns A new DataFrame with rows matching the condition
   * 
   * @group dfops
   */
  filter(condition: Column): DataFrame;
  /**
   * Filters rows using the given SQL expression string.
   * 
   * @param conditionExpr - A SQL expression string representing the filter condition
   * @returns A new DataFrame with rows matching the condition
   * 
   * @example
   * ```typescript
   * // Using Column
   * const adults = df.filter(col("age").gt(18));
   * 
   * // Using SQL expression
   * const adults2 = df.filter("age > 18");
   * ```
   * 
   * @group dfops
   */
  filter(conditionExpr: string): DataFrame;
  filter(condition: Column | string): DataFrame {
    const cond = typeof condition === "string" ? expr(condition) : condition;
    return this.toNewDataFrame(b => b.withFilter(cond.expr, this.plan.relation));
  }

  /**
   * Filters rows using the given Column condition. Alias for filter().
   * 
   * @param condition - A Column representing the filter condition
   * @returns A new DataFrame with rows matching the condition
   * 
   * @group dfops
   */
  where(condition: Column): DataFrame;
  /**
   * Filters rows using the given SQL expression string. Alias for filter().
   * 
   * @param conditionExpr - A SQL expression string representing the filter condition
   * @returns A new DataFrame with rows matching the condition
   * 
   * @example
   * ```typescript
   * const result = df.where(col("status").equalTo("active"));
   * const result2 = df.where("status = 'active'");
   * ```
   * 
   * @group dfops
   */
  where(conditionExpr: string): DataFrame;
  where(condition: Column | string): DataFrame {
    if (typeof condition === "string") {
      return this.filter(condition);
    } else {
      return this.filter(condition);
    }
  }

  /**
   * Specifies some hint on the current DataFrame. As an example, the following code specifies that
   * one of the plan can be broadcasted:
   *
   * {{{
   *   df1.join(df2.hint("broadcast"))
   * }}}
   *
   * the following code specifies that this dataset could be rebalanced with given number of
   * partitions:
   *
   * {{{
   *    df1.hint("rebalance", 10)
   * }}}
   *
   * @param name
   *   the name of the hint
   * @param parameters
   *   the parameters of the hint, all the parameters should be a `Column` or `Expression` or
   *   could be converted into a `Literal`
   * @group basic
   */
  hint(name: string, ...parameters: any[]): DataFrame {
    return this.toNewDataFrame(b => b.withHint(name, parameters, this.plan.relation));
  }

  /**
   * Groups the Dataset using the specified columns, so we can run aggregation on them. See
   * [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   ds.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   ds.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   */
  groupBy(...cols: string[]): RelationalGroupedDataset;
  groupBy(...cols: Column[]): RelationalGroupedDataset;
  groupBy(...cols: string[] | Column[]): RelationalGroupedDataset {
    return new RelationalGroupedDataset(this, cols, GroupType.GROUP_TYPE_GROUPBY);
  }

  /**
   * Returns the number of rows in the Dataset.
   *
   * @group action
   */
  async count(): Promise<bigint> {
    return this.groupBy().count().head().then(row => row.getLong(0));
  }

  // TODO: reduce()
  // TODO: groupByKey()

  /**
   * Create a multi-dimensional rollup for the current Dataset using the specified columns, so we
   * can run aggregation on them. See [[RelationalGroupedDataset]] for all the available aggregate
   * functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolled up by department and group.
   *   ds.rollup(col("department"), col("group")).avg()
   * }}}
   *
   * @group untypedrel
   */
  rollup(...cols: string[]): RelationalGroupedDataset;
  rollup(...cols: Column[]): RelationalGroupedDataset;
  rollup(...cols: string[] | Column[]): RelationalGroupedDataset {
    return new RelationalGroupedDataset(this, cols, GroupType.GROUP_TYPE_ROLLUP);
  }

  /**
   * Create a multi-dimensional cube for the current Dataset using the specified columns, so we
   * can run aggregation on them. See [[RelationalGroupedDataset]] for all the available aggregate
   * functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   ds.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   ds.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   */
  cube(...cols: string[]): RelationalGroupedDataset;
  cube(...cols: Column[]): RelationalGroupedDataset;
  cube(...cols: string[] | Column[]): RelationalGroupedDataset {
    return new RelationalGroupedDataset(this, cols, GroupType.GROUP_TYPE_CUBE);
  }

  groupingSets(groupingSets: Column[][], ...cols: Column[]): RelationalGroupedDataset {
    return new RelationalGroupedDataset(this, cols, GroupType.GROUPING_SETS, undefined, groupingSets);
  }

  /**
   * Join with another DataFrame.
   *
   * Behaves as an INNER JOIN and resolves columns by name (not by position).
   *
   * @param right Right side of the join operation.
   * @param usingColumn Name of the column to join on. This column must exist on both sides.
   *
   * @group untypedrel
   */
  join(right: DataFrame, usingColumn: string): DataFrame;
  /**
   * Inner join with another DataFrame, using the given join expression.
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   *
   * @group untypedrel
   */
  join(right: DataFrame, joinExprs: Column): DataFrame;
  /**
   * Join with another DataFrame, using the given join expression. The following performs a full
   * outer join between `df1` and `df2`.
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType Type of join to perform. Default `inner`. Must be one of:
   *   `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`, `leftouter`,
   *   `left_outer`, `right`, `rightouter`, `right_outer`, `semi`, `leftsemi`, `left_semi`,
   *   `anti`, `leftanti`, `left_anti`.
   *
   * @group untypedrel
   */
  join(right: DataFrame, joinExprs: Column, joinType: string): DataFrame;
  /**
   * Inner join with another DataFrame using the list of columns to join on.
   *
   * @param right Right side of the join.
   * @param usingColumns Names of columns to join on. These columns must exist on both sides.
   *
   * @group untypedrel
   */
  join(right: DataFrame, usingColumns: string[]): DataFrame;
  /**
   * Join with another DataFrame using the list of columns to join on.
   *
   * @param right Right side of the join.
   * @param usingColumns Names of columns to join on. These columns must exist on both sides.
   * @param joinType Type of join to perform. Default `inner`. Must be one of:
   *   `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`, `leftouter`,
   *   `left_outer`, `right`, `rightouter`, `right_outer`, `semi`, `leftsemi`, `left_semi`,
   *   `anti`, `leftanti`, `left_anti`.
   *
   * @group untypedrel
   */
  join(right: DataFrame, usingColumns: string[], joinType: string): DataFrame;
  join(right: DataFrame, on: string | string[] | Column, joinType?: string): DataFrame {
    if (typeof on === "string") {
      // Single column name
      return this.toNewDataFrame(b => 
        b.withJoin(this.plan.relation, right.plan.relation, undefined, joinType, [on])
      );
    } else if (Array.isArray(on)) {
      // Array of column names
      return this.toNewDataFrame(b => 
        b.withJoin(this.plan.relation, right.plan.relation, undefined, joinType, on)
      );
    } else {
      // Column expression
      return this.toNewDataFrame(b => 
        b.withJoin(this.plan.relation, right.plan.relation, on.expr, joinType, undefined)
      );
    }
  }

  /**
   * Explicit cartesian join with another DataFrame.
   *
   * @param right Right side of the join operation.
   *
   * @note Cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @group untypedrel
   */
  crossJoin(right: DataFrame): DataFrame {
    return this.toNewDataFrame(b => 
      b.withJoin(this.plan.relation, right.plan.relation, undefined, "cross", undefined)
    );
  }

  /**
   * Perform an as-of join between this DataFrame and another DataFrame.
   *
   * This is similar to a left-join except that we match on nearest key rather than equal keys.
   * For each row in the left DataFrame, we find the closest match in the right DataFrame
   * based on the as-of column(s) and join condition.
   *
   * @param right Right side of the join.
   * @param leftAsOf Column to join on from the left DataFrame.
   * @param rightAsOf Column to join on from the right DataFrame.
   * @param joinExprs Optional additional join expression.
   * @param joinType Type of join to perform. Default `inner`.
   * @param tolerance Optional tolerance for inexact matches.
   * @param allowExactMatches Whether to allow exact matches. Default true.
   * @param direction Direction of search. One of: `backward`, `forward`, `nearest`. Default `backward`.
   *
   * @group untypedrel
   */
  asOfJoin(
    right: DataFrame, 
    leftAsOf: Column, 
    rightAsOf: Column, 
    joinExprs?: Column,
    joinType?: string,
    tolerance?: Column,
    allowExactMatches?: boolean,
    direction?: string
  ): DataFrame;
  /**
   * Perform an as-of join between this DataFrame and another DataFrame using column names.
   *
   * @param right Right side of the join.
   * @param leftAsOf Column name to join on from the left DataFrame.
   * @param rightAsOf Column name to join on from the right DataFrame.
   * @param usingColumns Names of columns to join on. These columns must exist on both sides.
   * @param joinType Type of join to perform. Default `inner`.
   * @param tolerance Optional tolerance for inexact matches.
   * @param allowExactMatches Whether to allow exact matches. Default true.
   * @param direction Direction of search. One of: `backward`, `forward`, `nearest`. Default `backward`.
   *
   * @group untypedrel
   */
  asOfJoin(
    right: DataFrame, 
    leftAsOf: Column, 
    rightAsOf: Column, 
    usingColumns?: string[],
    joinType?: string,
    tolerance?: Column,
    allowExactMatches?: boolean,
    direction?: string
  ): DataFrame;
  asOfJoin(
    right: DataFrame, 
    leftAsOf: Column, 
    rightAsOf: Column, 
    joinExprsOrUsing?: Column | string[],
    joinType?: string,
    tolerance?: Column,
    allowExactMatches?: boolean,
    direction?: string
  ): DataFrame {
    if (Array.isArray(joinExprsOrUsing)) {
      return this.toNewDataFrame(b => 
        b.withAsOfJoin(
          this.plan.relation, 
          right.plan.relation, 
          leftAsOf.expr, 
          rightAsOf.expr,
          undefined,
          joinExprsOrUsing,
          joinType,
          tolerance?.expr,
          allowExactMatches,
          direction
        )
      );
    } else {
      return this.toNewDataFrame(b => 
        b.withAsOfJoin(
          this.plan.relation, 
          right.plan.relation, 
          leftAsOf.expr, 
          rightAsOf.expr,
          joinExprsOrUsing?.expr,
          undefined,
          joinType,
          tolerance?.expr,
          allowExactMatches,
          direction
        )
      );
    }
  }

  /**
   * Perform a lateral join between this DataFrame and another DataFrame.
   *
   * Lateral joins allow the right side to reference columns from the left side.
   * This is useful for operations like exploding arrays or applying table-valued functions.
   *
   * @param right Right side of the join (typically a table-valued function or explode).
   * @param joinType Type of join to perform. Must be one of: `inner`, `left`, `cross`. Default `inner`.
   * @param condition Optional join condition.
   *
   * @group untypedrel
   */
  lateralJoin(right: DataFrame, joinType?: string, condition?: Column): DataFrame {
    return this.toNewDataFrame(b => 
      b.withLateralJoin(this.plan.relation, right.plan.relation, joinType, condition?.expr)
    );
  }

  /**
   * Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
   * set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
   * which cannot be reversed.
   *
   * This function is useful to massage a DataFrame into a format where some columns are
   * identifier columns ("ids"), while all other columns ("values") are "unpivoted" to the rows,
   * leaving just two non-id columns, named as given by `variableColumnName` and
   * `valueColumnName`.
   *
   *
   * When no "id" columns are given, the unpivoted DataFrame consists of only the "variable" and
   * "value" columns.
   *
   * All "value" columns must share a least common data type. Unless they are the same data type,
   * all "value" columns are cast to the nearest common data type. For instance, types
   * `IntegerType` and `LongType` are cast to `LongType`, while `IntegerType` and `StringType` do
   * not have a common data type and `unpivot` fails with an `AnalysisException`.
   *
   * @param ids
   *   Id columns
   * @param values
   *   Value columns to unpivot
   * @param variableColumnName
   *   Name of the variable column
   * @param valueColumnName
   *   Name of the value column
   * @group untypedrel
   */
  unpivot(ids: Column[], variableColumnName: string, valueColumnName: string): DataFrame;
  unpivot(ids: Column[], values: Column[], variableColumnName: string, valueColumnName: string): DataFrame;
  unpivot(ids: Column[], ...args: any[]): DataFrame {
    let values: Column[] | undefined = undefined;
    let variableColumnName: string;
    let valueColumnName: string;
    if (args.length === 2) {
      variableColumnName = args[0];
      valueColumnName = args[1];
    } else {
      values = args[0] as Column[];
      variableColumnName = args[1];
      valueColumnName = args[2];
    }
    return this.toNewDataFrame(b => b.withUnpivot(ids, variableColumnName, valueColumnName, values, this.plan.relation));
  }

  /**
   * Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
   * set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
   * which cannot be reversed. This is an alias for `unpivot`.
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot(Array, Array, String, String)`
   *
   * This is equivalent to calling `Dataset#unpivot(Array, Array, String, String)` where `values`
   * is set to all non-id columns that exist in the DataFrame.
   * @param ids
   *   Id columns
   * @param variableColumnName
   *   Name of the variable column
   * @param valueColumnName
   *   Name of the value column
   * @group untypedrel
   */
  melt(ids: Column[], variableColumnName: string, valueColumnName: string): DataFrame;
  melt(ids: Column[], values: Column[], variableColumnName: string, valueColumnName: string): DataFrame;
  melt(ids: Column[], ...args: any[]): DataFrame {
    let values: Column[] | undefined = undefined;
    let variableColumnName: string;
    let valueColumnName: string;
    if (args.length === 2) {
      variableColumnName = args[0];
      valueColumnName = args[1];
    } else {
      values = args[0] as Column[];
      variableColumnName = args[1];
      valueColumnName = args[2];
    }
    return this.toNewDataFrame(b => b.withUnpivot(ids, variableColumnName, valueColumnName, values, this.plan.relation));
  }

  /**
   * Transposes a DataFrame such that the values in the specified index column become the new
   * columns of the DataFrame.
   *
   * Please note:
   *   - All columns except the index column must share a least common data type. Unless they are
   *     the same data type, all columns are cast to the nearest common data type.
   *   - The name of the column into which the original column names are transposed defaults to
   *     "key".
   *   - null values in the index column are excluded from the column names for the transposed
   *     table, which are ordered in ascending order.
   *
   * {{{
   *   val df = Seq(("A", 1, 2), ("B", 3, 4)).toDF("id", "val1", "val2")
   *   df.show()
   *   // output:
   *   // +---+----+----+
   *   // | id|val1|val2|
   *   // +---+----+----+
   *   // |  A|   1|   2|
   *   // |  B|   3|   4|
   *   // +---+----+----+
   *
   *   df.transpose($"id").show()
   *   // output:
   *   // +----+---+---+
   *   // | key|  A|  B|
   *   // +----+---+---+
   *   // |val1|  1|  3|
   *   // |val2|  2|  4|
   *   // +----+---+---+
   *   // schema:
   *   // root
   *   //  |-- key: string (nullable = false)
   *   //  |-- A: integer (nullable = true)
   *   //  |-- B: integer (nullable = true)
   *
   *   df.transpose().show()
   *   // output:
   *   // +----+---+---+
   *   // | key|  A|  B|
   *   // +----+---+---+
   *   // |val1|  1|  3|
   *   // |val2|  2|  4|
   *   // +----+---+---+
   *   // schema:
   *   // root
   *   //  |-- key: string (nullable = false)
   *   //  |-- A: integer (nullable = true)
   *   //  |-- B: integer (nullable = true)
   * }}}
   *
   * @param indexColumn
   *   The single column that will be treated as the index for the transpose operation. This
   *   column will be used to pivot the data, transforming the DataFrame such that the values of
   *   the indexColumn become the new columns in the transposed DataFrame.
   *
   * @group untypedrel
   */
  transpose(): DataFrame;
  transpose(indexColumn: Column): DataFrame;
  transpose(indexColumn?: Column): DataFrame {
    return this.toNewDataFrame(b => b.withTranspose(indexColumn, this.plan.relation));
  }

  /**
   * Returns a Column representing this DataFrame as a scalar subquery.
   * 
   * This method is used to create scalar subqueries from a DataFrame that is expected to return 
   * a single row and a single column. The resulting Column can be used in DataFrame operations
   * like select(), filter(), and where() clauses, similar to SQL scalar subqueries.
   * 
   * @returns A Column representing this DataFrame as a scalar subquery
   * 
   * @example
   * ```typescript
   * // Filter employees with salary greater than the average salary
   * const avgSalary = employees.select(avg("salary")).scalar();
   * const result = employees.where(col("salary").gt(avgSalary));
   * await result.show();
   * ```
   * 
   * @example
   * ```typescript
   * // Use in select clause
   * const maxSalary = employees.select(max("salary")).scalar();
   * const df = employees.select(col("name"), col("salary"), maxSalary.as("max_salary"));
   * await df.show();
   * ```
   * 
   * @group untypedrel
   * @since 4.0.0
   */
  scalar(): Column {
    return new Column(b => b.withSubqueryExpression(this.plan.relation));
  }

  // TODO: exists()

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   * This is equivalent to `UNION DISTINCT` in SQL.
   *
   * To do a SQL-style union that keeps duplicates, use [[unionAll]].
   *
   * Also as standard in SQL, this function resolves columns by position (not by name):
   *
   * {{{
   *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
   *   val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
   *   df1.union(df2).show
   *
   *   // output:
   *   // +----+----+----+
   *   // |col0|col1|col2|
   *   // +----+----+----+
   *   // |   1|   2|   3|
   *   // |   4|   5|   6|
   *   // +----+----+----+
   * }}}
   *
   * Notice that the column positions in the schema aren't necessarily matched with the fields in
   * the strongly typed objects in a Dataset. This function resolves columns by their positions in
   * the schema, not the fields in the strongly typed objects. Use [[unionByName]] to resolve
   * columns by field name in the typed objects.
   *
   * @group typedrel
   * @since 2.0.0
   */
  union(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "union", false));
  }

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   * This is equivalent to `UNION ALL` in SQL.
   *
   * To do a SQL-style set union (that does deduplication of elements), use [[union]].
   *
   * Also as standard in SQL, this function resolves columns by position (not by name).
   *
   * @group typedrel
   */
  unionAll(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "union", true));
  }

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   *
   * Unlike [[union]], this function resolves columns by name (not by position).
   * This is equivalent to `UNION ALL` in SQL with column name matching.
   *
   * When the parameter `allowMissingColumns` is true, the set of column names
   * in this and `other` Dataset can differ; missing columns will be filled with null.
   *
   * @group typedrel
   */
  unionByName(other: DataFrame): DataFrame;
  unionByName(other: DataFrame, allowMissingColumns: boolean): DataFrame;
  unionByName(other: DataFrame, allowMissingColumns?: boolean): DataFrame {
    allowMissingColumns = allowMissingColumns ?? false;
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "union", true, true, allowMissingColumns));
  }
  /**
   * Returns a new Dataset containing rows only in both this Dataset and another Dataset. This is
   * equivalent to `INTERSECT` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded representation of the data and thus
   *   is not affected by a custom `equals` function defined on `T`.
   * @group typedrel
   */
  intersect(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "intersect", false));
  }
  /**
   * Returns a new Dataset containing rows only in both this Dataset and another Dataset while
   * preserving the duplicates. This is equivalent to `INTERSECT ALL` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded representation of the data and thus
   *   is not affected by a custom `equals` function defined on `T`. Also as standard in SQL, this
   *   function resolves columns by position (not by name).
   * @group typedrel
   */
  intersectAll(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "intersect", true));
  }
  /**
   * Returns a new Dataset containing rows in this Dataset but not in another Dataset. This is
   * equivalent to `EXCEPT DISTINCT` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded representation of the data and thus
   *   is not affected by a custom `equals` function defined on `T`.
   * @group typedrel
   */
  except(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "except", false));
  }
  /**
   * Returns a new Dataset containing rows in this Dataset but not in another Dataset while
   * preserving the duplicates. This is equivalent to `EXCEPT ALL` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded representation of the data and thus
   *   is not affected by a custom `equals` function defined on `T`. Also as standard in SQL, this
   *   function resolves columns by position (not by name).
   * @group typedrel
   */
  exceptAll(other: DataFrame): DataFrame {
    return this.toNewDataFrame(b => b.withSetOperation(this.plan.relation, other.plan.relation, "except", true));
  }

  /**
   * Returns a new DataFrame that has exactly `numPartitions` partitions.
   * 
   * This operation requires a shuffle, making it a wide transformation.
   * 
   * @param numPartitions The target number of partitions. Must be positive.
   * @group typedrel
   */
  repartition(numPartitions: number): DataFrame;
  /**
   * Returns a new DataFrame partitioned by the given partitioning expressions.
   * The resulting DataFrame is hash partitioned.
   * 
   * This operation requires a shuffle, making it a wide transformation.
   * 
   * @param partitionExprs Column expressions to partition by
   * @group typedrel
   */
  repartition(...partitionExprs: Column[]): DataFrame;
  /**
   * Returns a new DataFrame partitioned by the given partitioning expressions,
   * using `numPartitions` partitions. The resulting DataFrame is hash partitioned.
   * 
   * This operation requires a shuffle, making it a wide transformation.
   * 
   * @param numPartitions The target number of partitions
   * @param partitionExprs Column expressions to partition by
   * @group typedrel
   */
  repartition(numPartitions: number, ...partitionExprs: Column[]): DataFrame;
  repartition(numPartitionsOrExpr: number | Column, ...partitionExprs: Column[]): DataFrame {
    if (typeof numPartitionsOrExpr === 'number') {
      if (partitionExprs.length === 0) {
        // repartition(numPartitions)
        return this.toNewDataFrame(b => b.withRepartition(numPartitionsOrExpr, true, this.plan.relation));
      } else {
        // repartition(numPartitions, ...partitionExprs)
        const exprs = partitionExprs.map(col => col.expr);
        return this.toNewDataFrame(b => b.withRepartitionByExpression(exprs, numPartitionsOrExpr, this.plan.relation));
      }
    } else {
      // repartition(...partitionExprs)
      const exprs = [numPartitionsOrExpr, ...partitionExprs].map(col => col.expr);
      return this.toNewDataFrame(b => b.withRepartitionByExpression(exprs, undefined, this.plan.relation));
    }
  }

  /**
   * Returns a new DataFrame that has exactly `numPartitions` partitions, when
   * the fewer partitions are requested. If a larger number of partitions is requested,
   * it will stay at the current number of partitions. Similar to coalesce defined on an `RDD`,
   * this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100
   * partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10
   * of the current partitions. If a larger number of partitions is requested, it will stay at the
   * current number of partitions.
   * 
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may result
   * in your computation taking place on fewer nodes than you like (e.g. one node in the case of
   * numPartitions = 1). To avoid this, you can call repartition(). This will add a shuffle step,
   * but means the current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   * 
   * @param numPartitions The target number of partitions. Must be positive.
   * @group typedrel
   */
  coalesce(numPartitions: number): DataFrame {
    return this.toNewDataFrame(b => b.withRepartition(numPartitions, false, this.plan.relation));
  }

  /**
   * Returns a new DataFrame partitioned by the given partitioning expressions. The resulting
   * DataFrame is range partitioned.
   * 
   * At least one partition-by expression must be specified. When no explicit sort order is
   * specified, "ascending nulls first" is assumed. Note, the rows are not sorted in each
   * partition of the resulting DataFrame.
   * 
   * This operation requires a shuffle, making it a wide transformation.
   * 
   * @param partitionExprs Column expressions to partition by
   * @group typedrel
   */
  repartitionByRange(...partitionExprs: Column[]): DataFrame;
  /**
   * Returns a new DataFrame partitioned by the given partitioning expressions into
   * `numPartitions`. The resulting DataFrame is range partitioned.
   * 
   * At least one partition-by expression must be specified. When no explicit sort order is
   * specified, "ascending nulls first" is assumed. Note, the rows are not sorted in each
   * partition of the resulting DataFrame.
   * 
   * This operation requires a shuffle, making it a wide transformation.
   * 
   * @param numPartitions The target number of partitions
   * @param partitionExprs Column expressions to partition by
   * @group typedrel
   */
  repartitionByRange(numPartitions: number, ...partitionExprs: Column[]): DataFrame;
  repartitionByRange(numPartitionsOrExpr: number | Column, ...partitionExprs: Column[]): DataFrame {
    // Helper to convert column to sort order (asc by default if not already sorted)
    const toSortCol = (col: Column): Column => {
      // Check if column already has a SortOrder expression
      const expr = col.expr;
      if (expr.exprType.case === "sortOrder") {
        return col;
      }
      // Default to ascending nulls first for range partitioning
      return col.asc;
    };

    if (typeof numPartitionsOrExpr === 'number') {
      // repartitionByRange(numPartitions, ...partitionExprs)
      const exprs = partitionExprs.map(col => toSortCol(col).expr);
      return this.toNewDataFrame(b => b.withRepartitionByExpression(exprs, numPartitionsOrExpr, this.plan.relation));
    } else {
      // repartitionByRange(...partitionExprs)
      const exprs = [numPartitionsOrExpr, ...partitionExprs].map(col => toSortCol(col).expr);
      return this.toNewDataFrame(b => b.withRepartitionByExpression(exprs, undefined, this.plan.relation));
    }
  }

  /**
   * Apply a function to each partition of the DataFrame.
   * 
   * This method applies a user-defined function to each partition of the DataFrame.
   * The function should take an iterator of rows and return an iterator of rows.
   * 
   * @param pythonCode Python code as a string defining the partition processing function
   * @param outputSchema The output schema for the transformed DataFrame
   * @param pythonVersion Python version (default: '3.11')
   * @returns A new DataFrame with the function applied to each partition
   * @group typedrel
   * 
   * @example
   * ```typescript
   * const pythonCode = `
   * def process_partition(partition):
   *     for row in partition:
   *         yield (row.id * 2, row.value)
   * `;
   * const schema = DataTypes.createStructType([
   *   DataTypes.createStructField('id', DataTypes.IntegerType, false),
   *   DataTypes.createStructField('value', DataTypes.StringType, false),
   * ]);
   * const result = df.mapPartitions(pythonCode, schema);
   * ```
   */
  mapPartitions(
    pythonCode: string,
    outputSchema: StructType,
    pythonVersion: string = '3.11'
  ): DataFrame {
    return this.toNewDataFrame(b => 
      b.withMapPartitions(pythonCode, outputSchema, this.plan.relation, pythonVersion)
    );
  }

  /**
   * Co-group two DataFrames and apply a function to each group.
   * 
   * This method groups two DataFrames by the specified columns and applies a user-defined
   * function to each group pair. The function receives the group key and iterators for
   * rows from both DataFrames.
   * 
   * @param other The other DataFrame to co-group with
   * @param thisGroupingCols Columns to group by for this DataFrame
   * @param otherGroupingCols Columns to group by for the other DataFrame
   * @param pythonCode Python code as a string defining the co-group processing function
   * @param outputSchema The output schema for the transformed DataFrame
   * @param pythonVersion Python version (default: '3.11')
   * @returns A new DataFrame with the function applied to each co-group
   * @group typedrel
   * 
   * @example
   * ```typescript
   * const pythonCode = `
   * def cogroup_func(key, left_rows, right_rows):
   *     for l in left_rows:
   *         for r in right_rows:
   *             yield (key.id, l.value, r.value)
   * `;
   * const schema = DataTypes.createStructType([
   *   DataTypes.createStructField('id', DataTypes.IntegerType, false),
   *   DataTypes.createStructField('left_value', DataTypes.StringType, false),
   *   DataTypes.createStructField('right_value', DataTypes.StringType, false),
   * ]);
   * const result = df1.coGroupMap(df2, [col('id')], [col('id')], pythonCode, schema);
   * ```
   */
  coGroupMap(
    other: DataFrame,
    thisGroupingCols: Column[],
    otherGroupingCols: Column[],
    pythonCode: string,
    outputSchema: StructType,
    pythonVersion: string = '3.11'
  ): DataFrame {
    const inputGroupingExprs = thisGroupingCols.map(col => col.expr);
    const otherGroupingExprs = otherGroupingCols.map(col => col.expr);
    if (!this.plan.relation || !other.plan.relation) {
      throw new Error('DataFrame plans must have relations for coGroupMap operation');
    }
    return this.toNewDataFrame(b =>
      b.withCoGroupMap(
        this.plan.relation,
        inputGroupingExprs,
        other.plan.relation,
        otherGroupingExprs,
        pythonCode,
        outputSchema,
        pythonVersion
      )
    );
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
