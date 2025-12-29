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
import { grpc } from '@grpc/grpc-web';
import * as b from '../../../../../gen/spark/connect/base_pb'; // proto import: "spark/connect/base.proto"
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { logger } from '../../logger';
import { type MessageShape, type DescMessage } from '@bufbuild/protobuf';
import { fromStatus } from "../errors";
import { AnalyzePlanRequestBuilder } from "../proto/AnalyzePlanRequestBuilder";
  

export class Client {
  conf_: Configuration;
  client_: grpc.AbstractClientBase;
  session_id_: string;
  user_context_: b.UserContext;
  user_agent_: string = "spark-connect-js"; // TODO: What shall I set here?
  metadata_: Record<string, string>;

  constructor(conf: Configuration) {
    this.conf_ = conf;
    const protocol = conf.get_is_ssl_enabled() ? 'https' : 'http';
    const host = `${protocol}://${conf.get_host()}:${conf.get_port()}`;
    
    this.client_ = new grpc.GrpcWebClientBase({
      format: 'binary'
    });
    
    this.session_id_ = this.conf_.get_session_id();
    this.user_context_ = this.conf_.get_user_context();
    this.metadata_ = this.conf_.get_metadata();
  }

  static builder(): ClientBuilder {
    return new ClientBuilder();
  }

  close(): void {
    try {
        // grpc-web clients don't need explicit closing
        logger.debug("Client closed");
    } catch (e) {
        console.error("Failed to close the underlying grpc client", e);
    }
  }

  private api(method: string): string {
    const protocol = this.conf_.get_is_ssl_enabled() ? 'https' : 'http';
    const host = `${protocol}://${this.conf_.get_host()}:${this.conf_.get_port()}`;
    return `${host}/spark.connect.SparkConnectService/${method}`;
  }

  private createMetadata(): {[key: string]: string} {
    const metadata: {[key: string]: string} = {};
    for (const [key, value] of Object.entries(this.metadata_)) {
      metadata[key] = value;
    }
    return metadata;
  }

  private runServiceCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Uint8Array,
      deser: (resp: Uint8Array) => RespType): Promise<RespType> {
    logger.debug("Sending unary request by", method, req);
    return new Promise((resolve, reject) => {
      this.client_.rpcCall(
        this.api(method),
        req,
        this.createMetadata(),
        {
          methodDescriptor: {
            name: method,
            requestType: {} as any,
            responseType: {} as any,
            requestSerializeFn: ser,
            responseDeserializeFn: deser,
          }
        } as any,
        (err: grpc.Error | null, resp?: RespType) => {
          if (err) {
            const handledErr = fromStatus(err);
            logger.debug("Errored calling", method + ": ", handledErr);
            reject(handledErr);
          } else if (resp) {
            logger.debug("Received response by", method + ": ", resp);
            resolve(resp);
          } else {
            const msg = "No response or error received by " + method;
            logger.error(msg);
            reject(new Error(msg));
          }
        }
      );
    });
  }

  private runServerStreamCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Uint8Array,
      deser: (resp: Uint8Array) => RespType): Promise<RespType> {
    logger.debug("Sending stream request by", method, req);
    return new Promise((resolve, reject) => {
      const stream = this.client_.serverStreaming(
        this.api(method),
        req,
        this.createMetadata(),
        {
          methodDescriptor: {
            name: method,
            requestType: {} as any,
            responseType: {} as any,
            requestSerializeFn: ser,
            responseDeserializeFn: deser,
          }
        } as any
      );

      stream.on('data', (data: RespType) => {
        logger.debug("Received data from", method, data);
        resolve(data);
      });

      stream.on('error', (err: grpc.Error) => {
        const handledErr = fromStatus(err);
        logger.error("Errored calling", method, handledErr);
        reject(handledErr);
      });

      stream.on('end', () => {
        logger.debug("End of", method);
      });
    });
  }

  private serializer<Desc extends DescMessage>(desc: Desc) {
    return (msg: MessageShape<Desc>) => {
       return toBinary(desc, msg);
    }
  }

  private deserializer<Desc extends DescMessage>(desc: Desc) {
    return (buf: Uint8Array) => {
      return fromBinary(desc, buf);
    }
  }

  async analyze(f: (b: AnalyzePlanRequestBuilder) => void): Promise<b.AnalyzePlanResponse> {
    const ab = new AnalyzePlanRequestBuilder();
    f(ab);
    const req = ab.sessionId(this.session_id_)
      .userContext(this.user_context_)
      .clientType(this.user_agent_)
      .build();
    return this.runServiceCall<b.AnalyzePlanRequest, b.AnalyzePlanResponse>(
      "AnalyzePlan",
      req,
      this.serializer(b.AnalyzePlanRequestSchema),
      this.deserializer(b.AnalyzePlanResponseSchema),
    );
  }

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
  }

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
    
    return new Promise((resolve, reject) => {
      const stream = this.client_.serverStreaming(
        this.api(method),
        req,
        this.createMetadata(),
        {
          methodDescriptor: {
            name: method,
            requestType: {} as any,
            responseType: {} as any,
            requestSerializeFn: this.serializer(b.ExecutePlanRequestSchema),
            responseDeserializeFn: this.deserializer(b.ExecutePlanResponseSchema),
          }
        } as any
      );

      const results: b.ExecutePlanResponse[] = [];
      
      stream.on('data', (data: b.ExecutePlanResponse) => {
        results.push(data);
      });
      
      stream.on('error', (err: grpc.Error) => {
        const handledErr = fromStatus(err);
        logger.error("Errored calling", method + ":", handledErr);
        reject(handledErr);
      });

      stream.on('status', (status: grpc.Status) => {
        if (status.code !== grpc.StatusCode.OK) {
          const handledErr = fromStatus(status as any);
          logger.error("Status received by", method, handledErr);
          reject(handledErr);
        } else {
          logger.debug("Status received by", method, status);
        }
      });

      stream.on('end', () => {
        logger.debug(`End of`, method, req.plan?.opType.value, `Forwarding ${results.length} responses`);
        resolve(results)
      });
    });
  }
}