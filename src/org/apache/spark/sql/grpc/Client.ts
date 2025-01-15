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

import { ClientBuilder, Configuration } from "./client_builder";
import * as grpc from '@grpc/grpc-js';
import * as b from '../../../../../gen/spark/connect/base_pb'; // proto import: "spark/connect/base.proto"
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { logger } from '../../logger';
import { type MessageShape, type DescMessage } from '@bufbuild/protobuf';
import { fromStatus } from "../errors";
  

export class Client {
  conf_: Configuration;
  client_: grpc.Client;
  session_id_: string;
  user_context_: b.UserContext;
  user_agent_: string = "spark-connect-js"; // TODO: What shall I set here?
  metadata_: grpc.Metadata;

  constructor(conf: Configuration) {
    this.conf_ = conf;
    this.client_ = this.conf_.new_client();
    this.session_id_ = this.conf_.get_session_id();
    this.user_context_ = this.conf_.get_user_context();
    this.metadata_ = this.conf_.get_metadata();
  }

  static builder(): ClientBuilder {
    return new ClientBuilder();
  }

  close(): void {
    try {
        this.client_.close();
    } catch (e) {
        console.error("Failed to close the underlying grpc client", e);
    }
  }

  private api(method: string): string {
    return `/spark.connect.SparkConnectService/${method}`;
  }

  private runServiceCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Buffer,
      deser: (resp: Buffer) => RespType): Promise<RespType> {
    logger.debug("Sending unnary request by", method, req);
    return new Promise((resolve, reject) => {
      this.client_.makeUnaryRequest<ReqType, RespType>(
        this.api(method),
        ser,
        deser,
        req,
        this.metadata_,
        {}, // call options
        (err: grpc.ServiceError | null, resp?: RespType) => {
          if (err) {
            const handledErrr = fromStatus(err);
            logger.error("Errored calling", method + ": ", handledErrr);
            reject(handledErrr);
          } else if (resp) {
            logger.debug("Received response by", method + ": ", JSON.stringify(resp));
            resolve(resp);
          } else {
            const msg = "No response or error received by " + method;
            logger.error(msg);
            reject(new Error(msg));
          }
        }
      )
    });
  };

  private runServerStreamCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Buffer,
      deser: (resp: Buffer) => RespType): Promise<RespType> {
    logger.debug("Sending stream request by", method, req);
    return new Promise((resolve, reject) => {
      this.client_.makeServerStreamRequest<ReqType, RespType>(
        this.api(method),
        ser,
        deser,
        req,
        this.metadata_,
        {}
      ).on("data", (data: RespType) => {
          logger.debug("Received data from", method, data);
          resolve(data);
        }
      ).on("error", (err: any) => {
          const handledErrr = fromStatus(err);
          logger.error("Errored calling", method, handledErrr);
          reject(handledErrr);
        }
      ).on("end", () => {
          logger.debug("End of", method);
        }
      );
    });
  };

  private serializer<Desc extends DescMessage>(desc: Desc) {
    return (msg: MessageShape<Desc>) => {
       return Buffer.from(toBinary(desc, msg));
    }
  }

  private deserializer<Desc extends DescMessage>(desc: Desc) {
    return (buf: Buffer) => {
      return fromBinary(desc, buf);
    }
  }

  async analyze(analysis: (req: b.AnalyzePlanRequest) => void): Promise<b.AnalyzePlanResponse> {
    const req = create(b.AnalyzePlanRequestSchema, {
      sessionId: this.session_id_,
      userContext: this.user_context_,
      clientType: this.user_agent_
    });
    analysis(req);
    return this.runServiceCall<b.AnalyzePlanRequest, b.AnalyzePlanResponse>(
      "AnalyzePlan",
      req,
      this.serializer(b.AnalyzePlanRequestSchema),
      this.deserializer(b.AnalyzePlanResponseSchema),
    );
  };

  async config(operation: b.ConfigRequest_Operation): Promise<b.ConfigResponse> {
    const req = create(b.ConfigRequestSchema, {
        sessionId: this.session_id_,
        userContext: this.user_context_,
        clientType: this.user_agent_,
        operation: operation
    });
    return this.runServiceCall<b.ConfigRequest, b.ConfigResponse>(
      "Config",
      req,
      this.serializer(b.ConfigRequestSchema),
      this.deserializer(b.ConfigResponseSchema),
    );
  };

  async execute(plan: b.Plan): Promise<b.ExecutePlanResponse[]> {
    const req = create(b.ExecutePlanRequestSchema, {
      sessionId: this.session_id_,
      userContext: this.user_context_,
      clientType: this.user_agent_,
      plan: plan,
      tags: [] // TODO: FIXME
    });

    const method = "ExecutePlan";
    logger.debug("Sending stream request by", method, req.plan?.opType.value);
    // TODO: Etract this into a helper function
    return new Promise((resolve, reject) => {
      const call = this.client_.makeServerStreamRequest<b.ExecutePlanRequest, b.ExecutePlanResponse>(
        this.api(method),
        this.serializer(b.ExecutePlanRequestSchema),
        this.deserializer(b.ExecutePlanResponseSchema),
        req,
        this.metadata_,
        {}
      )
      const results: b.ExecutePlanResponse[] = [];
      call.on("data", (data: b.ExecutePlanResponse) => {
        const responseType = data.responseType
        switch (responseType.case) {
          case "executionProgress":
            const stageInfo = this.stringifyStageInfo(data.operationId, responseType.value);
            if (stageInfo.length > 0) {
              logger.debug(stageInfo);
            }
            break;
          default:
            if (data.metrics) {
              logger.debug(`Metrics: ${this.stringifyMetrics(data.metrics)}`);
            }
            // TODO: do we need 
            results.push(data);
        }
      });
      
      call.on("error", (err: any) => {
        let handledErr = fromStatus(err);
        logger.error("Errored calling", method + ":", handledErr);
        reject(handledErr);
      });

      call.on("status", (status: grpc.StatusObject) => {
        if (status.code !== grpc.status.OK) {
          reject(status);
        }
      });

      call.on("end", () => {
        logger.debug(`End of`, method, req.plan?.opType.value, `Forwarding ${results.length} responses`);
        resolve(results)
      });
    });
  };

  private stringifyStageInfo(id: string, progress: b.ExecutePlanResponse_ExecutionProgress): string {
    let ret = "";
    if (progress.stages.length > 0) {
      ret += "Execution Progress: id " + id;
      ret += ("\n\tStages:\n" + progress.stages.map(info => {
          const state = info.done ? "[DONE]" : "[RUNNING]";
          return `\t\tStage ${info.stageId}${state}: ${info.numCompletedTasks}/${info.numTasks} tasks completed, reading ${info.inputBytesRead} bytes`
        }).join("\n"))
      if (progress.numInflightTasks > 0) {
        ret += `\n\tRunning tasks: ${progress.numInflightTasks}`;
      }
    }
    return ret;
  }

  private stringifyMetrics(metrics: b.ExecutePlanResponse_Metrics): string {
    return metrics.metrics.map(metric => {
      return (`${metric.name}[${metric.planId}|${metric.parent}]: ` + 
        Object.entries(metric.executionMetrics).map(([key, value]) => {
          return `${value.name}(${key}): ${value.value}`;
      }).join(", "));
    }).join("\n");
  }
}