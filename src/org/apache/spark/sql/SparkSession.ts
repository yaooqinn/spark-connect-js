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

/**
 * @since 1.0.0
 */
export class SparkSession {
  private client_: Client;
  private planIdGenerator_: PlanIdGenerator = PlanIdGenerator.getInstance();
  private conf_: RuntimeConfig;
  private version_?: string;
  
  constructor(client: Client) {
    this.client_ = client;
    this.conf_ = new RuntimeConfig(client);
  }

  public static builder(): SparkSessionBuilder { return new SparkSessionBuilder(); }

  get client(): Client { return this.client_; }

  session_id(): string { return this.client_.session_id_; }

  async version(): Promise<string> {
    if (!this.version_) {
      return this.client.analyze(req => {
        req.analyze = {
          value: create(b.AnalyzePlanRequest_SparkVersionSchema, {}),
          case: "sparkVersion"
        };
      }).then(resp => {
        if (resp.result.case === "sparkVersion") {
          this.version_ = resp.result.value.version;
          return this.version_;
        } else {
          throw new Error("Failed to get spark version");
        }
      }).catch(e => {
        console.error(`Failed to get spark version, ${e}`);
        throw e;
      });
    }
    return this.version_ || "unknown";
  }
  
  conf(): RuntimeConfig { return this.conf_; }

  // TODO: support other parameters
  sql(sqlStr: string): Promise<DataFrame> {
    const sqlCmd = create(cmd.SqlCommandSchema, { sql: sqlStr});
    return this.sqlInternal(sqlCmd);
  }

  private async sqlInternal(sqlCmd: cmd.SqlCommand): Promise<DataFrame> {
    const command = create(cmd.CommandSchema, {
      commandType: {
        value: sqlCmd,
        case: "sqlCommand"
      }
    });
    const plan = create(b.PlanSchema, { opType: { value: command, case: "command" }});
    return this.client.execute(plan).then(resp => {
      const responseType = resp.responseType;
      const relationCommon = create(r.RelationCommonSchema, {
        planId: this.planIdGenerator_.getNextId(),
        sourceInfo: ""
      });
      if (responseType.case === "sqlCommandResult" && responseType.value.relation) {
        const relation = responseType.value.relation;
        relation.common = relationCommon;
        const final = create(b.PlanSchema, { opType: { case: "root", value: relation } });
        return new DataFrame(this, final);
      } else {
        // TODO: not quite sure what to do here
        return new DataFrame(this, plan);
      };

    });
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