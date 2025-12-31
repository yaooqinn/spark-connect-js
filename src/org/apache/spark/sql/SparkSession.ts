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
import { DataFrame } from './DataFrame';
import * as cmd from '../../../../gen/spark/connect/commands_pb';
import * as r from '../../../../gen/spark/connect/relations_pb';
import { Client } from './grpc/Client';
import { RuntimeConfig } from './RuntimeConfig';
import { ClientBuilder } from './grpc/client_builder';
import { PlanIdGenerator } from '../../../../utils';
import { logger } from '../logger';
import { DataFrameReader } from './DataFrameReader';
import { createLocalRelation, createLocalRelationFromArrowTable } from './proto/ProtoUtils';
import { StructType } from './types/StructType';
import { Table } from 'apache-arrow';
import { Row } from './Row';
import { tableFromRows } from './arrow/ArrowUtils';
import { AnalyzePlanRequestBuilder } from './proto/AnalyzePlanRequestBuilder';
import { AnalyzePlanResponseHandler } from './proto/AnalyzePlanResponeHandler';
import { RelationBuilder } from './proto/RelationBuilder';
import { PlanBuilder } from './proto/PlanBuilder';
import { CommandBuilder } from './proto/CommandBuilder';
import { ExecutePlanResponseHandler } from './proto/ExecutePlanResponseHandler';
import { Catalog } from './catalog/Catalog';
import { LogicalPlan } from './proto/LogicalPlan';
import { UDFRegistration } from './UDFRegistration';

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 * 
 * @remarks
 * A SparkSession can be used to create DataFrames, register DataFrames as tables, execute SQL
 * over tables, cache tables, and read data from various sources. A SparkSession is created using
 * the {@link SparkSession.builder} method.
 * 
 * The SparkSession provides access to:
 * - {@link sql} - Execute SQL queries and return results as DataFrames
 * - {@link read} - Read data from external sources (CSV, JSON, Parquet, tables, etc.)
 * - {@link createDataFrame} - Create DataFrames from in-memory data
 * - {@link table} - Load tables as DataFrames
 * - {@link range} - Generate DataFrames with sequences of numbers
 * - {@link catalog} - Access the Spark catalog for metadata operations
 * - {@link udf} - Register and manage user-defined functions
 * - {@link conf} - Configure Spark runtime settings
 * 
 * @example
 * ```typescript
 * // Create a SparkSession
 * const spark = await SparkSession.builder()
 *   .remote("sc://localhost:15002")
 *   .appName("MyApp")
 *   .getOrCreate();
 * 
 * // Execute SQL
 * const df = await spark.sql("SELECT * FROM users WHERE age > 21");
 * await df.show();
 * 
 * // Read data
 * const csvDf = spark.read.csv("data.csv");
 * 
 * // Create DataFrame from data
 * const data = [new Row(schema, ["Alice", 30]), new Row(schema, ["Bob", 25])];
 * const df2 = spark.createDataFrame(data, schema);
 * ```
 * 
 * @group core
 * @since 1.0.0
 */
export class SparkSession {
  private planIdGenerator: PlanIdGenerator = PlanIdGenerator.getInstance();
  private conf_: RuntimeConfig;
  private version_?: string;
  public readonly catalog: Catalog = new Catalog(this);
  public readonly udf: UDFRegistration = new UDFRegistration(this);
  
  constructor(public client: Client) {
    this.conf_ = new RuntimeConfig(client);
  }

  /**
   * Creates a SparkSessionBuilder for constructing a SparkSession.
   * 
   * @returns A new SparkSessionBuilder instance
   * 
   * @example
   * ```typescript
   * const spark = await SparkSession.builder()
   *   .remote("sc://localhost:15002")
   *   .appName("MyApplication")
   *   .config("spark.sql.shuffle.partitions", "10")
   *   .getOrCreate();
   * ```
   * 
   * @group builder
   */
  public static builder(): SparkSessionBuilder { return new SparkSessionBuilder(); }


  /**
   * Returns the session ID for this SparkSession.
   * 
   * @returns The unique session identifier
   * 
   * @group core
   */
  public session_id(): string { return this.client.session_id_; }

  /**
   * Returns the Spark version this SparkSession is connected to.
   * 
   * @returns A promise that resolves to the Spark version string
   * 
   * @example
   * ```typescript
   * const version = await spark.version();
   * console.log(`Connected to Spark ${version}`);
   * ```
   * 
   * @group core
   */
  public async version(): Promise<string> {
    if (!this.version_) {
      const resp = await this.analyze(b => b.withSparkVersion());
      this.version_ = resp.version;
      return this.version_;
    }
    return this.version_ || "unknown";
  }
  
  /**
   * Runtime configuration interface for this SparkSession.
   * 
   * @returns The RuntimeConfig instance for getting/setting Spark configuration
   * 
   * @example
   * ```typescript
   * await spark.conf.set("spark.sql.shuffle.partitions", "200");
   * const value = await spark.conf.get("spark.sql.shuffle.partitions");
   * ```
   * 
   * @group config
   */
  public get conf(): RuntimeConfig { return this.conf_; }

  /**
   * Returns an empty DataFrame.
   * 
   * @returns A DataFrame with no rows or columns
   * 
   * @example
   * ```typescript
   * const empty = spark.emptyDataFrame;
   * const isEmpty = await empty.isEmpty(); // true
   * ```
   * 
   * @group core
   */
  public get emptyDataFrame(): DataFrame {
    return this.relationBuilderToDF(b => b.withLocalRelation(createLocalRelation()));
  }

  /**
   * Executes a SQL query and returns the result as a DataFrame.
   * 
   * @param sqlStr - The SQL query string to execute
   * @returns A promise that resolves to a DataFrame containing the query results
   * 
   * @example
   * ```typescript
   * const df = await spark.sql("SELECT * FROM users WHERE age > 21");
   * await df.show();
   * 
   * // Use with table joins
   * const joined = await spark.sql(`
   *   SELECT u.name, o.amount 
   *   FROM users u 
   *   JOIN orders o ON u.id = o.user_id
   * `);
   * ```
   * 
   * @group core
   */
  // TODO: support other parameters
  public async sql(sqlStr: string): Promise<DataFrame> {
    const command = new CommandBuilder().withSqlCommand(sqlStr).build();
    const resps = await this.execute(command);
    const resp = resps.filter(r => r.isSqlCommandResult)[0];
    const relation = resp.sqlCommandResult;
    if (relation) {
      relation.common = this.newRelationCommon();
      return this.dataFrameFromRelation(relation);
    } else {
      // TODO: not quite sure what to do here
      return new DataFrame(this, this.planFromCommand(command));
    }
  }

  /**
   * Returns a DataFrameReader for reading data from external sources.
   * 
   * @returns A DataFrameReader instance
   * 
   * @example
   * ```typescript
   * // Read CSV
   * const df = spark.read.csv("data.csv");
   * 
   * // Read JSON
   * const df2 = spark.read.json("data.json");
   * 
   * // Read Parquet with options
   * const df3 = spark.read
   *   .option("mergeSchema", "true")
   *   .parquet("data.parquet");
   * 
   * // Read from table
   * const df4 = spark.read.table("my_table");
   * ```
   * 
   * @group io
   */
  public get read(): DataFrameReader {
    return new DataFrameReader(this);
  }

  /**
   * Creates a DataFrame from an array of Row objects with a specified schema.
   * 
   * @param data - An array of Row objects
   * @param schema - The schema defining the structure of the DataFrame
   * @returns A new DataFrame containing the provided data
   * 
   * @example
   * ```typescript
   * const schema = new StructType([
   *   new StructField("name", DataTypes.StringType),
   *   new StructField("age", DataTypes.IntegerType)
   * ]);
   * 
   * const data = [
   *   new Row(schema, { name: "Alice", age: 30 }),
   *   new Row(schema, { name: "Bob", age: 25 })
   * ];
   * 
   * const df = spark.createDataFrame(data, schema);
   * await df.show();
   * ```
   * 
   * @group core
   */
  public createDataFrame(data: Row[], schema: StructType): DataFrame {
    const table = tableFromRows(data, schema);
    return this.createDataFrameFromArrowTable(table, schema);
  }

  /**
   * Creates a DataFrame from an Apache Arrow Table.
   * 
   * @param table - An Apache Arrow Table object
   * @param schema - The Spark schema defining the structure of the DataFrame
   * @returns A new DataFrame containing the Arrow table data
   * 
   * @example
   * ```typescript
   * import { Table } from 'apache-arrow';
   * 
   * // Assuming you have an Arrow table
   * const arrowTable = ...; // Arrow Table
   * const schema = new StructType([...]);
   * 
   * const df = spark.createDataFrameFromArrowTable(arrowTable, schema);
   * await df.show();
   * ```
   * 
   * @group core
   */
  public createDataFrameFromArrowTable(table: Table, schema: StructType): DataFrame {
    const local = createLocalRelationFromArrowTable(table, schema);
    return this.relationBuilderToDF(b => b.withLocalRelation(local));
  }

  /**
   * Executes a Spark Connect command and returns the response handlers.
   * 
   * @param cmd - The command to execute
   * @returns A promise that resolves to an array of response handlers
   * 
   * @remarks
   * This is a low-level API used internally. Most users should use higher-level
   * APIs like sql(), read, or DataFrame operations instead.
   * 
   * @group internal
   * @ignore
   */
  async execute(cmd: cmd.Command): Promise<ExecutePlanResponseHandler[]> {
    const plan = this.planFromCommand(cmd);
    return this.client.execute(plan.plan).then(resps => resps.map(resp => new ExecutePlanResponseHandler(resp)));
  }

  /**
   * Returns the specified table as a DataFrame.
   * 
   * @param name - The name of the table to load
   * @returns A DataFrame representing the table
   * 
   * @example
   * ```typescript
   * const users = spark.table("users");
   * await users.show();
   * 
   * // With database qualifier
   * const logs = spark.table("production.logs");
   * ```
   * 
   * @group io
   */
  table(name: string): DataFrame {
    return this.read.table(name);
  }

  /**
   * Creates a DataFrame with a single column of Long values starting from 0 to end (exclusive).
   * 
   * @param end - The end value (exclusive)
   * @returns A DataFrame with a single column named "id" containing values from 0 to end-1
   * 
   * @group core
   */
  range(end: bigint | number): DataFrame;
  /**
   * Creates a DataFrame with a single column of Long values.
   * 
   * @param start - The start value (inclusive)
   * @param end - The end value (exclusive)
   * @returns A DataFrame with a single column named "id" containing values from start to end-1
   * 
   * @group core
   */
  range(start: bigint | number, end: bigint | number): DataFrame;
  /**
   * Creates a DataFrame with a single column of Long values with a custom step.
   * 
   * @param start - The start value (inclusive)
   * @param end - The end value (exclusive)
   * @param step - The increment between consecutive values
   * @returns A DataFrame with a single column named "id"
   * 
   * @group core
   */
  range(start: bigint | number, end: bigint | number, step: bigint | number): DataFrame;
  /**
   * Creates a DataFrame with a single column of Long values with custom step and partitions.
   * 
   * @param start - The start value (inclusive)
   * @param end - The end value (exclusive)
   * @param step - The increment between consecutive values
   * @param numPartitions - The number of partitions for the resulting DataFrame
   * @returns A DataFrame with a single column named "id"
   * 
   * @example
   * ```typescript
   * // Generate 0 to 9
   * const df1 = spark.range(10);
   * 
   * // Generate 5 to 14
   * const df2 = spark.range(5, 15);
   * 
   * // Generate 0, 2, 4, 6, 8
   * const df3 = spark.range(0, 10, 2);
   * 
   * // Generate with 4 partitions
   * const df4 = spark.range(0, 100, 1, 4);
   * ```
   * 
   * @group core
   */
  range(start: bigint | number, end: bigint | number, step: bigint | number, numPartitions: number): DataFrame;
  range(start: bigint | number, end?: bigint | number, step?: bigint | number, numPartitions?: number): DataFrame {
    if (typeof start === 'number') start = BigInt(start);
    if (typeof end === 'number') end = BigInt(end);
    if (typeof step === 'number') step = BigInt(step);

    if (end === undefined) {
      end = start;
      start = 0n;
    }
    if (step === undefined) {
      step = 1n;
    }
    return this.relationBuilderToDF(b => b.withRange(start, end, step, numPartitions));
  }

  /**
   * Convenience method to create a spark.connect.RelationCommon
   * @ignore
   * @private
   * @returns a new RelationCommon instance
   */
  private newRelationCommon(): r.RelationCommon {
    return create(r.RelationCommonSchema, {
      planId: this.planIdGenerator.getNextId(),
      sourceInfo: ""
    });
  }

  /** @ignore @private */
  async analyze(f: (b: AnalyzePlanRequestBuilder) => void): Promise<AnalyzePlanResponseHandler> {
    return this.client.analyze(f).then(resp => new AnalyzePlanResponseHandler(resp));
  }

  /** @ignore @private */
  planFromRelationBuilder(f: (builder: RelationBuilder) => void): LogicalPlan {
    const withRelationCommonFunc = (builder: RelationBuilder) => {
      builder.withRelationCommon(this.newRelationCommon());
      f(builder);
    }
    return new PlanBuilder().withRelationBuilder(withRelationCommonFunc).build();
  }

  planFromRelation(relation: r.Relation): LogicalPlan {
    return new PlanBuilder().withRelation(relation).build();
  }

  /** @ignore @private */
  planFromCommandBuilder(f: (builder: CommandBuilder) => void): LogicalPlan {
    return new PlanBuilder().withCommandBuilder(f).build();
  }

  /** @ignore @private */
  planFromCommand(cmd: cmd.Command): LogicalPlan {
    return new PlanBuilder().withCommand(cmd).build();
  }
  
  /** @ignore @private */
  relationBuilderToDF(f: (builder: RelationBuilder) => void): DataFrame {
    return new DataFrame(this, this.planFromRelationBuilder(f));
  }

  dataFrameFromRelation(relation: r.Relation): DataFrame {
    return new DataFrame(this, this.planFromRelation(relation));
  }
}

class SparkSessionBuilder {
  // TODO: Cache the SparkSession and Client with address or session id as key?
  private static _cachedSparkSession: SparkSession | undefined = undefined;
  private static _cachedGrpcClient: Client | undefined = undefined;
  private _builder: ClientBuilder = new ClientBuilder();
  private _options: Map<string, string> = new Map();
  
  constructor() {}

  config(key: string, value: string): SparkSessionBuilder {
    this._options.set(key, value);
    return this;
  }

  remote(connectionString: string = "sc://localhost"): SparkSessionBuilder {
    this._builder.connectionString(connectionString);
    return this;
  }

  appName(name: string = "Spark Connect TypeScript"): SparkSessionBuilder {
    return this.config("spark.app.name", name);
  }

  async create(): Promise<SparkSession> {
    if (!SparkSessionBuilder._cachedGrpcClient) {
      SparkSessionBuilder._cachedGrpcClient = this._builder.build();
    }
    const newSession = new SparkSession(SparkSessionBuilder._cachedGrpcClient);
    return newSession.conf.setAll(this._options).then(() => {
      logger.info("Updated configuration for new SparkSession", this._options);
      SparkSessionBuilder._cachedSparkSession = newSession;
    }).then(() => {
      newSession.version().then(v => {
        logger.info(`Spark Connect Server verison: ${v}`);
      });
      return newSession;
    });
  }

  async getOrCreate(): Promise<SparkSession> {
    const existing = SparkSessionBuilder._cachedSparkSession;
    if (existing) {
      logger.debug("Reusing existing SparkSession", existing);
      await existing.conf.setAll(this._options);
      logger.info("Updated configuration for existing SparkSession", this._options);
      existing.version().then(v => {
        logger.info(`The verion of Spark Connect Server is ${v}`);
      });
      return existing;
    }
    return this.create();
  }
}