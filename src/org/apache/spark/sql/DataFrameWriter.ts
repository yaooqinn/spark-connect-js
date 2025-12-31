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

import * as c from "../../../../gen/spark/connect/commands_pb";
import { DataFrame } from "./DataFrame";
import { SaveMode } from "./SaveMode";
import { AnalysisException } from "./errors";
import { ExecutePlanResponseHandler } from "./proto/ExecutePlanResponseHandler";
import { CaseInsensitiveMap } from "./util/CaseInsensitiveMap";
import { WriteOperationBuilder } from "./proto/WriteOperationBuilder";
import { CommandBuilder } from "./proto/CommandBuilder";

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `DataFrame.write()` to access this.
 * 
 * @stable
 * @since 1.0.0
 * @see https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/DataFrameWriter.html
 * @author Kent Yao <yao@apache.org>
 * 
 * TODO: Some features are not implemented yet:
 */
export class DataFrameWriter {
  private mode_: c.WriteOperation_SaveMode = c.WriteOperation_SaveMode.ERROR_IF_EXISTS;
  private source_?: string;
  private extraOptions_ = new CaseInsensitiveMap<string>();
  private partitioningColumns_: string[] = [];
  private bucketColumnNames_: string[] = [];
  private numBuckets_?: number;
  private sortColumnNames_: string[] = [];
  private clusteringColumns_: string[] = [];

  constructor(public df: DataFrame) {}

  mode(saveMode: string | SaveMode): DataFrameWriter {
    if (typeof saveMode === "string") {
      let normalizedMode = saveMode.toUpperCase();
      if (normalizedMode === "ERROR" || normalizedMode === "ERRORIFEXISTS" || normalizedMode === "DEFAULT") {
        normalizedMode = "ERROR_IF_EXISTS";
      }
      this.mode_ = c.WriteOperation_SaveMode[normalizedMode as keyof typeof c.WriteOperation_SaveMode];
      // Runtime validation for invalid mode strings
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (this.mode_ === undefined) {
        throw new AnalysisException(
          "INVALID_SAVE_MODE",
          `The specified save mode "${saveMode}" is invalid. Valid save modes include "append", "overwrite", "ignore", "error", "errorifexists", and "default"`,
          "42000");
      }
    } else {
      this.mode_ = saveMode as unknown as c.WriteOperation_SaveMode;
    }
    return this;
  }

  format(source: string): DataFrameWriter {
    this.source_ = source;
    return this;
  }

  option(key: string, value: string | number | boolean): DataFrameWriter {
    this.extraOptions_.set(key, value.toString());
    return this;
  }

  options(opts: Record<string, string>): DataFrameWriter {
    Object.entries(opts).forEach(([key, value]) => this.option(key, value));
    return this;
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like: <ul>
   * <li>year=2016/month=01/</li> <li>year=2016/month=02/</li> </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout. It
   * provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number of
   * distinct values in each column should typically be less than tens of thousands.
   *
   */
  partitionBy(...cols: string[]): DataFrameWriter {
    this.partitioningColumns_ = cols;
    this.validatePartitioning();
    return this;
  }

  /**
   * Buckets the output by the given columns. If specified, the output is laid out on the file
   * system similar to Hive's bucketing scheme, but with a different bucket hash function and is
   * not compatible with Hive's bucketing.
   */
  bucketBy(numBuckets: number, colName: string, ...colNames: string[]): DataFrameWriter {
    this.bucketColumnNames_ = [colName, ...colNames];
    this.numBuckets_ = numBuckets;
    this.validatePartitioning();
    return this;
  }

  /**
   * Sorts the output in each bucket by the given columns.
   */
  sortBy(colName: string, ...colNames: string[]): DataFrameWriter {
    this.sortColumnNames_ = [colName, ...colNames];
    return this;
  }

  /**
   * Clusters the output by the given columns on the storage. The rows with matching values in the
   * specified clustering columns will be consolidated within the same group.
   *
   * For instance, if you cluster a dataset by date, the data sharing the same date will be stored
   * together in a file. This arrangement improves query efficiency when you apply selective
   * filters to these clustering columns, thanks to data skipping.
   */
  clusterBy(colName: string, ...colNames: string[]): DataFrameWriter {
    this.clusteringColumns_ = [colName, ...colNames];
    this.validatePartitioning();
    return this;
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   */
  save(path?: string): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWriteOperation(builder => {
      if (path) {
        builder.withPath(path);
      }
    });
  }

  /**
   * Inserts the content of the `DataFrame` to the specified table. It requires that the schema of
   * the `DataFrame` is the same as the schema of the table.
   *
   * @note
   *   Unlike `saveAsTable`, `insertInto` ignores the column names and just uses position-based
   *   resolution. For example:
   * @note
   *   SaveMode.ErrorIfExists and SaveMode.Ignore behave as SaveMode.Append in `insertInto` as
   *   `insertInto` is not a table creating operation.
   */
  insertInto(tableName: string): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWriteOperation(builder => {
      builder.withTable(tableName, c.WriteOperation_SaveTable_TableSaveMethod.INSERT_INTO);
    });
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the save mode,
   * specified by the `mode` function (default to throwing an exception). When `mode` is
   * `Overwrite`, the schema of the `DataFrame` does not need to be the same as that of the
   * existing table.
   *
   * When `mode` is `Append`, if there is an existing table, we will use the format and options of
   * the existing table. The column order in the schema of the `DataFrame` doesn't need to be same
   * as that of the existing table. Unlike `insertInto`, `saveAsTable` will use the column names
   * to find the correct column positions. For example:
   *
   * In this method, save mode is used to determine the behavior if the data source table exists
   * in Spark catalog. We will always overwrite the underlying data of data source (e.g. a table
   * in JDBC data source) if the table doesn't exist in Spark catalog, and will always append to
   * the underlying data of data source if the table already exists.
   *
   * When the DataFrame is created from a non-partitioned `HadoopFsRelation` with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   */
  saveAsTable(tableName: string): Promise<ExecutePlanResponseHandler[]> {
    return this.executeWriteOperation(builder => {
      builder.withTable(tableName, c.WriteOperation_SaveTable_TableSaveMethod.SAVE_AS_TABLE);
    });
  }

  /**
   * Saves the content of the `DataFrame` to an external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the save
   * mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * JDBC-specific option and parameter documentation for storing tables via JDBC in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @param url 
   *  JDBC database url of the form `jdbc:subprotocol:subname`.
   * @param table
   *   Name of the table in the external database.
   * @param connectionProperties
   *   JDBC database connection arguments, a list of arbitrary string tag/value. Normally at least
   *   a "user" and "password" property should be included. "batchsize" can be used to control the
   *   number of rows per insert. "isolationLevel" can be one of "NONE", "READ_COMMITTED",
   *   "READ_UNCOMMITTED", "REPEATABLE_READ", or "SERIALIZABLE", corresponding to standard
   *   transaction isolation levels defined by JDBC's Connection object, with default of
   *   "READ_UNCOMMITTED".
   */
  jdbc(url: string, table: string, connectionProperties: Record<string, string>): Promise<ExecutePlanResponseHandler[]> {
    this.assertNotPartitioned("jdbc");
    this.assertNotBucketed("jdbc");
    this.assertNotClustered("jdbc");
    return this.format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .options(connectionProperties)
      .save();
  }

  /**
   * Saves the content of the `DataFrame` in JSON format (<a href="http://jsonlines.org/"> JSON
   * Lines text format or newline-delimited JSON</a>) at the specified path. This is equivalent
   * to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * You can find the JSON-specific options for writing JSON files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   */
  json(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("json").save(path);
  }

  /**
   * Saves the content of the `DataFrame` in Parquet format at the specified path. This is
   * equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * Parquet-specific option(s) for writing Parquet files can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   *
   * @since 1.0.0
   */
  parquet(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("parquet").save(path);
  }

  /**
   * Saves the content of the `DataFrame` in ORC format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * ORC-specific option(s) for writing ORC files can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   */
  orc(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("orc").save(path);
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path. The DataFrame must
   * have only one column that is of string type. Each row becomes a new line in the output file.
   *
   * You can find the text-specific options for writing text files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   * Data Source Option</a> in the version you use.
   */
  text(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("text").save(path);
  }

  /**
   * Saves the content of the `DataFrame` in CSV format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("csv").save(path)
   * }}}
   *
   * You can find the CSV-specific options for writing CSV files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   * Data Source Option</a> in the version you use.
   */
  csv(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("csv").save(path);
  }

  /**
   * Saves the content of the `DataFrame` in XML format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("xml").save(path)
   * }}}
   *
   * Note that writing a XML file from `DataFrame` having a field `ArrayType` with its element as
   * `ArrayType` would have an additional nested field for the element. For example, the
   * `DataFrame` having a field below,
   *
   * {@code fieldA [[data1], [data2]]}
   *
   * would produce a XML file below. {@code <fieldA> <item>data1</item> </fieldA> <fieldA>
   * <item>data2</item> </fieldA>}
   *
   * Namely, roundtrip in writing and reading can end up in different schema structure.
   *
   * You can find the XML-specific options for writing XML files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
   * Data Source Option</a> in the version you use.
   */
  xml(path: string): Promise<ExecutePlanResponseHandler[]> {
    return this.format("xml").save(path);
  }

  private executeWriteOperation(f: (builder: WriteOperationBuilder) => void): Promise<ExecutePlanResponseHandler[]> {
    const relation = this.df.plan.relation;
    if (!relation) {
      throw new Error('DataFrame plan must have a relation for write operation');
    }
    const builder = new WriteOperationBuilder()
      .withInput(relation)
      .withMode(this.mode_)
      .withPartitioningColumns(this.partitioningColumns_)
      .withClusteringColumns(this.clusteringColumns_)
      .withSortColumnNames(this.sortColumnNames_)
      .withOptions(this.extraOptions_.toIndexSignature());

    if (this.source_) {
      builder.withSource(this.source_);
    }

    if (this.numBuckets_ !== undefined) {
      builder.withBucketBy(this.bucketColumnNames_, this.numBuckets_);
    }

    f(builder);

    const writeOp = builder.build();
    const command = new CommandBuilder()
      .withWriteOperation(writeOp)
      .build();

    return this.df.spark.execute(command);
  }

  private validatePartitioning(): void {
    if (this.clusteringColumns_.length > 0) {
      if (this.partitioningColumns_.length > 0) {
        throw new AnalysisException(
          "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
          "Cannot specify both CLUSTER BY and PARTITIONED BY.",
           "42908");
      }
      
      if (this.isBucketed()) {
        throw new AnalysisException(
          "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
          "Cannot specify both CLUSTER BY and CLUSTERED BY INTO BUCKETS",
           "42908");
      }
    }
  }

  private isBucketed(): boolean {
    if (this.sortColumnNames_.length > 0 && !this.numBuckets_) {
      throw new AnalysisException(
        "SORT_BY_WITHOUT_BUCKETING",
        "sortBy must be used together with bucketBy.",
         "42601");
    }
    return this.numBuckets_ !== undefined;
  }

  private assertNotBucketed(operation: string): void {
    if (this.isBucketed()) {
      if (this.sortColumnNames_.length === 0) {
        throw new AnalysisException(
          "_LEGACY_ERROR_TEMP_1312",
          `'${operation}' does not support bucketBy right now.`);
      } else {
        throw new AnalysisException(
          "_LEGACY_ERROR_TEMP_1313",
          `'${operation}' does not support bucketBy and sortBy right now.`);
      }
    }
  }

  private assertNotPartitioned(operation: string): void {
    if (this.partitioningColumns_.length > 0) {
      throw new AnalysisException(
        "_LEGACY_ERROR_TEMP_1197",
        `'${operation}' does not support partitioning.`);
    }
  }

  private assertNotClustered(operation: string): void {
    if (this.clusteringColumns_.length > 0) {
      throw new AnalysisException(
        "CLUSTERING_NOT_SUPPORTED",
        `'${operation}' does not support clustering.`,
        "42000");
    }
  }
}
