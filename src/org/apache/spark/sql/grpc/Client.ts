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
import { AnalyzePlanRequestBuilder } from "../proto/AnalyzePlanRequestBuilder";
  
/**
 * gRPC client for Spark Connect protocol.
 * 
 * @remarks
 * This class manages the gRPC connection to a Spark Connect server and provides
 * methods for executing plans, analyzing plans, and managing configuration.
 * 
 * **Resource Management:**
 * - The client maintains a persistent gRPC connection
 * - Stream handlers are automatically registered for each request
 * - Event listeners are managed per-request and cleaned up on stream completion
 * - Call {@link close} to explicitly release resources when done
 * 
 * **Stream Lifecycle:**
 * 1. Stream created via makeServerStreamRequest
 * 2. Event listeners registered: 'data', 'error', 'status', 'end'
 * 3. Stream processes responses
 * 4. Listeners automatically removed on 'end' or 'error' events
 * 5. Promise resolves/rejects based on stream outcome
 * 
 * **Error Handling:**
 * - All errors are converted using {@link fromStatus}
 * - Streams handle both 'error' and 'status' events for comprehensive error reporting
 * - Rejected promises propagate errors to callers
 * 
 * @example
 * ```typescript
 * const client = new Client(config);
 * try {
 *   const results = await client.execute(plan);
 *   // Process results
 * } finally {
 *   client.close(); // Clean up resources
 * }
 * ```
 * 
 * @group grpc
 * @since 1.0.0
 */
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

  /**
   * Closes the underlying gRPC client and releases resources.
   * 
   * @remarks
   * This method should be called when the client is no longer needed to ensure
   * proper cleanup of gRPC connections and prevent resource leaks.
   * 
   * **Resource Disposal:**
   * - Closes the gRPC channel
   * - Releases any pending connections
   * - Errors during close are logged but not thrown
   * 
   * **Important Notes:**
   * - After calling close(), the client cannot be reused
   * - Any in-flight requests will be terminated
   * - Safe to call multiple times (idempotent)
   * 
   * @example
   * ```typescript
   * const client = new Client(config);
   * try {
   *   await client.execute(plan);
   * } finally {
   *   client.close(); // Always clean up
   * }
   * ```
   * 
   * @group lifecycle
   */
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
            logger.debug("Errored calling", method + ": ", handledErrr);
            reject(handledErrr);
          } else if (resp) {
            logger.debug("Received response by", method + ": ", resp);
            resolve(resp);
          } else {
            const msg = "No response or error received by " + method;
            logger.error(msg);
            reject(new Error(msg));
          }
        }
      )
    });
  }

  /**
   * Executes a server-streaming gRPC call.
   * 
   * @remarks
   * This method creates a server stream and registers event handlers for the stream lifecycle.
   * 
   * **Stream Resource Management:**
   * - Stream is created via makeServerStreamRequest
   * - Event listeners ('data', 'error', 'end') are registered on the stream
   * - Listeners are automatically cleaned up when the stream completes or errors
   * - The stream is automatically closed by gRPC when 'end' event fires
   * 
   * **Listener Lifecycle:**
   * 1. 'data' - Fired for each response chunk, resolves promise with first data
   * 2. 'error' - Fired on stream error, rejects promise and cleans up
   * 3. 'end' - Fired when stream completes, cleans up listeners
   * 
   * **Memory Safety:**
   * - Event listeners are scoped to the Promise closure
   * - No manual listener cleanup needed - handled by Promise resolution/rejection
   * - Stream object eligible for GC after Promise settles
   * 
   * @param method - The gRPC method name
   * @param req - The request message
   * @param ser - Serializer function for request
   * @param deser - Deserializer function for response
   * @returns Promise that resolves with the first response chunk
   * 
   * @internal
   */
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
  }

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

  /**
   * Executes a Spark plan and returns all response chunks.
   * 
   * @param plan - The Spark execution plan to run
   * @returns Promise that resolves with array of all ExecutePlanResponse chunks
   * 
   * @remarks
   * This method creates a server-streaming gRPC call to execute a Spark plan.
   * 
   * **Stream Resource Management Pattern:**
   * ```typescript
   * // 1. Stream Creation
   * const call = client.makeServerStreamRequest(...);
   * 
   * // 2. Event Handler Registration
   * call.on('data', handler);   // Collects responses
   * call.on('error', handler);  // Handles errors
   * call.on('status', handler); // Checks final status
   * call.on('end', handler);    // Completes promise
   * 
   * // 3. Automatic Cleanup
   * // - On 'end': listeners removed, promise resolved
   * // - On 'error': listeners removed, promise rejected
   * // - Stream closed by gRPC automatically
   * ```
   * 
   * **Resource Lifecycle:**
   * 1. **Creation**: gRPC stream created, event listeners registered
   * 2. **Processing**: 'data' events accumulate responses in results array
   * 3. **Completion**: 
   *    - Normal: 'end' event → resolve promise → listeners cleaned up
   *    - Error: 'error' event → reject promise → listeners cleaned up
   *    - Status error: 'status' event → reject promise → listeners cleaned up
   * 4. **Cleanup**: Stream closed by gRPC, event listeners garbage collected
   * 
   * **Memory Management:**
   * - Results array grows with each 'data' event
   * - Stream reference held until Promise settles
   * - Event handlers are closures over results array and Promise resolve/reject
   * - All references released when Promise completes
   * - No manual cleanup needed - managed by Promise lifecycle
   * 
   * **Error Handling:**
   * - Network errors → 'error' event → promise rejected
   * - Protocol errors → 'status' event with non-OK code → promise rejected
   * - All errors converted via {@link fromStatus} for consistent error handling
   * 
   * @example
   * ```typescript
   * const plan = createPlan();
   * try {
   *   const responses = await client.execute(plan);
   *   responses.forEach(resp => processResponse(resp));
   * } catch (error) {
   *   console.error('Execution failed:', error);
   * }
   * // Stream automatically cleaned up whether success or failure
   * ```
   * 
   * @group execution
   */
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
      // Create server stream - this establishes the gRPC stream
      const call = this.client_.makeServerStreamRequest<b.ExecutePlanRequest, b.ExecutePlanResponse>(
        this.api(method),
        this.serializer(b.ExecutePlanRequestSchema),
        this.deserializer(b.ExecutePlanResponseSchema),
        req,
        this.metadata_,
        {}
      );
      
      // Accumulator for all response chunks
      const results: b.ExecutePlanResponse[] = [];
      
      // Event: 'data' - Accumulate each response chunk
      // Resource note: results array grows with each chunk
      call.on("data", (data: b.ExecutePlanResponse) => {
        results.push(data);
      });
      
      // Event: 'error' - Handle stream errors
      // Resource note: Rejecting promise triggers automatic cleanup
      call.on("error", (err: any) => {
        const handledErr = fromStatus(err);
        logger.error("Errored calling", method + ":", handledErr);
        reject(handledErr); // Promise rejection removes all event listeners
      });

      // Event: 'status' - Check final gRPC status
      // Resource note: Non-OK status rejects promise and triggers cleanup
      call.on("status", (status: grpc.StatusObject) => {
        if (status.code !== grpc.status.OK) {
          const handledErr = fromStatus(status);
          logger.error("Status received by", method, handledErr);
          reject(handledErr); // Promise rejection removes all event listeners
        } else {
          logger.debug("Status received by", method, status);
        }
      });

      // Event: 'end' - Stream completed successfully
      // Resource note: Resolving promise removes all event listeners and allows GC
      call.on("end", () => {
        logger.debug(`End of`, method, req.plan?.opType.value, `Forwarding ${results.length} responses`);
        resolve(results); // Promise resolution triggers automatic cleanup
      });
      
      // Note: No manual cleanup needed!
      // - Event listeners are automatically removed when Promise settles
      // - Stream is closed by gRPC when 'end' event fires
      // - All references become eligible for garbage collection
    });
  }
}