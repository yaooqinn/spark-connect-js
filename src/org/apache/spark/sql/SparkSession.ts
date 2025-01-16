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
import * as b from '../../../../gen/spark/connect/base_pb';
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
import { AnalyzePlanResponseWraper } from './proto/AnalyzePlanResponeWrapper';
import { RelationBuilder } from './proto/RelationBuilder';
import { PlanBuilder } from './proto/PlanBuilder';
import { CommandBuilder } from './proto/CommandBuilder';
import { ExecutePlanResponseWrapper } from './proto/ExecutePlanResponseWrapper';

/**
 * @since 1.0.0
 */
export class SparkSession {
  private planIdGenerator: PlanIdGenerator = PlanIdGenerator.getInstance();
  private conf_: RuntimeConfig;
  private version_?: string;
  
  constructor(public client: Client) {
    this.conf_ = new RuntimeConfig(client);
  }

  public static builder(): SparkSessionBuilder { return new SparkSessionBuilder(); }


  session_id(): string { return this.client.session_id_; }

  async version(): Promise<string> {
    if (!this.version_) {
      return this.analyze(b => b.withSparkVersion()).then(resp => {
        this.version_ = resp.version;
        return this.version_;
      });
    }
    return this.version_ || "unknown";
  }
  
  conf(): RuntimeConfig { return this.conf_; };

  emptyDataFrame(): DataFrame {
    return this.dataFrameFromRelationBuilder(b => b.withLocalRelation(createLocalRelation()));
  }

  // TODO: support other parameters
  async sql(sqlStr: string): Promise<DataFrame> {
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
    };
  };

  read(): DataFrameReader {
    return new DataFrameReader(this);
  }

  createDataFrame(data: Row[], schema: StructType): DataFrame {
    const table = tableFromRows(data, schema);
    return this.createDataFrameFromArrowTable(table, schema);
  }

  createDataFrameFromArrowTable(table: Table, schema: StructType): DataFrame {
    const local = createLocalRelationFromArrowTable(table, schema);
    return this.dataFrameFromRelationBuilder(b => b.withLocalRelation(local));
  }

  async execute(cmd: cmd.Command): Promise<ExecutePlanResponseWrapper[]> {
    const plan = this.planFromCommand(cmd);
    return this.client.execute(plan).then(resps => resps.map(resp => new ExecutePlanResponseWrapper(resp)));
  }

  table(name: string): DataFrame {
    return this.read().table(name);
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
  async analyze(f: (b: AnalyzePlanRequestBuilder) => void): Promise<AnalyzePlanResponseWraper> {
    return this.client.analyze(f).then(resp => new AnalyzePlanResponseWraper(resp));
  }

  /** @ignore @private */
  planFromRelationBuilder(f: (builder: RelationBuilder) => void): b.Plan {
    const withRelationCommonFunc = (builder: RelationBuilder) => {
      builder.withRelationCommon(this.newRelationCommon());
      f(builder);
    }
    return new PlanBuilder().withRelationBuilder(withRelationCommonFunc).build();
  }

  planFromRelation(relation: r.Relation): b.Plan {
    return new PlanBuilder().withRelation(relation).build();
  }

  /** @ignore @private */
  planFromCommandBuilder(f: (builder: CommandBuilder) => void): b.Plan {
    return new PlanBuilder().withCommandBuilder(f).build();
  }

  /** @ignore @private */
  planFromCommand(cmd: cmd.Command): b.Plan {
    return new PlanBuilder().withCommand(cmd).build();
  }
  
  /** @ignore @private */
  dataFrameFromRelationBuilder(f: (builder: RelationBuilder) => void): DataFrame {
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
  
  constructor() {};

  config(key: string, value: string): SparkSessionBuilder {
    this._options.set(key, value);
    return this;
  }

  remote(connectionString: string = "sc://localhost"): SparkSessionBuilder {
    this._builder.connectionString(connectionString);
    return this;
  }

  master(master: string = "local[*]"): SparkSessionBuilder {
    return this
  }

  appName(name: string = "Spark Connect TypeScript"): SparkSessionBuilder {
    return this.config("spark.app.name", name);
  }

  async create(): Promise<SparkSession> {
    if (!SparkSessionBuilder._cachedGrpcClient) {
      SparkSessionBuilder._cachedGrpcClient = this._builder.build();
    }
    const newSession = new SparkSession(SparkSessionBuilder._cachedGrpcClient);
    return newSession.conf().setAll(this._options).then(() => {
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
      await existing.conf().setAll(this._options);
      logger.info("Updated configuration for existing SparkSession", this._options);
      existing.version().then(v => {
        logger.info(`The verion of Spark Connect Server is ${v}`);
      });
      return existing;
    }
    return this.create();
  }
}