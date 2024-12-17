import { create } from '@bufbuild/protobuf';
import { DataFrame } from './data_frame';
import * as b from '../../../../gen/spark/connect/base_pb';
import * as cmd from '../../../../gen/spark/connect/commands_pb';
import * as r from '../../../../gen/spark/connect/relations_pb';
import { Client } from '../../../../client/Client';
import { RuntimeConfig } from './runtime_config';
import { ClientBuilder } from '../../../../client/client_builder';
import { PlanIdGenerator } from '../../../../utils';

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
        return this.client.anlyze(req => {
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

    private sqlInternal(sqlCmd: cmd.SqlCommand): Promise<DataFrame> {
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
          planId: BigInt(this.planIdGenerator_.getNextId()).valueOf(),
          sourceInfo: ""
        });
        if (responseType.case === "sqlCommandResult" && responseType.value.relation) {
          const relation = responseType.value.relation;
            relation.common = relationCommon
            const final = create(b.PlanSchema, { opType: { case: "root", value: relation } });
          return new DataFrame(this, final);
        } else {
          // TODO: not quite sure what to do here
          return new DataFrame(this, plan);
        }
      });
    }
}

class SparkSessionBuilder {
    private builder: ClientBuilder = new ClientBuilder();
    private _options: Map<string, string> = new Map();
    
    constructor() { };
  
    config(key: string, value: string): SparkSessionBuilder {
      this._options.set(key, value);
      return this;
    }
  
    remote(connectionString: string = "sc://localhost"): SparkSessionBuilder {
      this.builder.connectionString(connectionString);
      return this;
    }
  
    master(master: string = "local[*]"): SparkSessionBuilder {
      // do nothing
      return this
    }
  
    appName(name: string = "Spark Connect JS"): SparkSessionBuilder {
      return this.config("spark.app.name", name);
    }
  
    async create(): Promise<SparkSession> {
      const client = this.builder.build();
      const spark = new SparkSession(client);
      await spark.conf().setAll(this._options);
      return spark;
    }

    // TODO: should use existing session if it exists
    async getOrCreate(): Promise<SparkSession> {
      return this.create();
    }
  }