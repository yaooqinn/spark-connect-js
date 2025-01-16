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
import { createLocalRelation, createLocalRelationFromArrowTable, PlanBuilder, RelationBuilder } from './proto/ProtoUtils';
import { StructType } from './types/StructType';
import { Table } from 'apache-arrow';
import { Row } from './Row';
import { tableFromRows } from './arrow/ArrowUtils';
import { AnalyzePlanRequestBuilder } from './proto/AnalyzePlanRequestBuilder';
import { AnalyzePlanResponseWraper } from './proto/AnalyzePlanResponeWraper';

/**
 * @since 1.0.0
 */
export class SparkSession {
  public planIdGenerator: PlanIdGenerator = PlanIdGenerator.getInstance();
  private conf_: RuntimeConfig;
  private version_?: string;
  
  constructor(public client: Client) {
    this.conf_ = new RuntimeConfig(client);
  }

  public static builder(): SparkSessionBuilder { return new SparkSessionBuilder(); }


  session_id(): string { return this.client.session_id_; }

  async version(): Promise<string> {
    if (!this.version_) {
      const builder = new AnalyzePlanRequestBuilder().setSparkVersion();
      return this.analyze(builder).then(resp => {
        this.version_ = resp.version;
        return this.version_;
      });
    }
    return this.version_ || "unknown";
  }
  
  conf(): RuntimeConfig { return this.conf_; };

  emptyDataFrame(): DataFrame {
    const plan = this.newPlanWithRelation(r => r.relType = { case: "localRelation", value: createLocalRelation() });
    return new DataFrame(this, plan);
  }

  // TODO: support other parameters
  sql(sqlStr: string): Promise<DataFrame> {
    const sqlCmd = create(cmd.SqlCommandSchema, { sql: sqlStr});
    return this.sqlInternal(sqlCmd);
  };

  private async sqlInternal(sqlCmd: cmd.SqlCommand): Promise<DataFrame> {
    const command = create(cmd.CommandSchema, {
      commandType: {
        value: sqlCmd,
        case: "sqlCommand"
      }
    });
    return this.execute(command).then(resps => {
      const resp = resps.filter(resp => resp.responseType.case === "sqlCommandResult")[0];
      const responseType = resp.responseType;
      if (responseType.case === "sqlCommandResult" && responseType.value.relation) {
        const relation = responseType.value.relation;
        relation.common = this.newRelationCommon();
        this.newPlan({ value: relation, case: "root" });
        const final = this.newPlan({ value: relation, case: "root" });
        return new DataFrame(this, final);
      } else {
        // TODO: not quite sure what to do here
        return new DataFrame(this, this.newPlan({ value: command, case: "command" }));
      };
    });
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
    const relation = new RelationBuilder()
      .setRelationCommon(this.newRelationCommon())
      .setLocalRelation(local)
      .build();
    const plan = new PlanBuilder()
      .setRelation(relation)
      .build();
    return new DataFrame(this, plan);
  }

  execute(cmd: cmd.Command): Promise<b.ExecutePlanResponse[]> {
    const plan =  this.newPlan({ value: cmd, case: "command" });
    return this.client.execute(plan);
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
  newRelationCommon(): r.RelationCommon {
    return create(r.RelationCommonSchema, {
      planId: this.planIdGenerator.getNextId(),
      sourceInfo: ""
    });
  }

  /**
   * Convenience method to create a spark.connect.Relation
   * @ignore
   * @private 
   */
  newRelation(f: (r: r.Relation) => void): r.Relation {
    const relation =  create(r.RelationSchema, {
      common: this.newRelationCommon()
    });
    f(relation);
    return relation;
  }

  /**
   * Convenience method to create a spark.connect.Plan
   * @ignore
   * @private 
   */
  newPlan(operationType: { value: r.Relation; case: "root"; } | { value: cmd.Command; case: "command"; }): b.Plan {
    const plan = create(b.PlanSchema, { opType: operationType });
    return plan;
  }

  /**
   * Convenience method to create a spark.connect.Plan with a Relation
   * @ignore
   * @private 
   */
  newPlanWithRelation(f: (r: r.Relation) => void): b.Plan {
    const relation = this.newRelation(f);
    return this.newPlan({ value: relation, case: "root" });
  }

  /** @ignore @private */
  async analyze(builder: AnalyzePlanRequestBuilder): Promise<AnalyzePlanResponseWraper> {
    return this.client.analyze2(builder).then(resp => new AnalyzePlanResponseWraper(resp));
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