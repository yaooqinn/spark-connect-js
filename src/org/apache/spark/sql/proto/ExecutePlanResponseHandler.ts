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

import { ExecutePlanResponse, ExecutePlanResponse_SqlCommandResult } from "../../../../../gen/spark/connect/base_pb";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";

/**
 * Discriminated union type for ExecutePlanResponse result types.
 * 
 * @remarks
 * This type ensures type safety when handling different response types from Spark Connect.
 * Each variant represents a specific response type case.
 */
type ExecuteResultType = 
  | { case: "sqlCommandResult"; value: ExecutePlanResponse_SqlCommandResult }
  | { case: "arrowBatch"; value: unknown }
  | { case: "resultComplete"; value: unknown }
  | { case: "metrics"; value: unknown }
  | { case: "observedMetrics"; value: unknown }
  | { case: "streamingQueryProgress"; value: unknown }
  | { case: "streamingQueryCommand"; value: unknown }
  | { case: "writeStreamOperationStart"; value: unknown }
  | { case: "extension"; value: unknown }
  | { case: undefined; value: undefined };

/**
 * Handles responses from ExecutePlanRequest operations.
 * 
 * @remarks
 * This class wraps ExecutePlanResponse and provides type-safe access to different
 * response types using discriminated unions. It validates the response type before
 * accessing specific result values.
 * 
 * @example
 * ```typescript
 * const handler = new ExecutePlanResponseHandler(response);
 * if (handler.isSqlCommandResult) {
 *   const relation = handler.sqlCommandResult;
 *   // Process SQL command result
 * }
 * ```
 * 
 * @group internal
 */
export class ExecutePlanResponseHandler {
  private readonly resultType: string | undefined;
  private readonly resultValue: ExecuteResultType;
  
  constructor(public readonly response: ExecutePlanResponse) {
    this.resultType = response.responseType.case;
    this.resultValue = response.responseType as ExecuteResultType;
  }

  /**
   * Returns the session ID for this response.
   * 
   * @returns The session identifier
   * @group core
   */
  get sessionId(): string {
    return this.response.sessionId;
  }

  /**
   * Returns the server-side session ID.
   * 
   * @returns The server-side session identifier
   * @group core
   */
  get serverSideSessionId(): string {
    return this.response.serverSideSessionId;
  }

  /**
   * Returns the operation ID.
   * 
   * @returns The operation identifier
   * @group core
   */
  get operationId(): string {
    return this.response.operationId;
  }

  /**
   * Returns the response ID.
   * 
   * @returns The response identifier
   * @group core
   */
  get responseId(): string {
    return this.response.responseId;
  }

  /**
   * Returns the raw response type structure.
   * 
   * @returns The response type discriminated union
   * @group core
   */
  get result(): ExecuteResultType {
    return this.resultValue;
  }

  /**
   * Checks if this response contains a SQL command result.
   * 
   * @returns True if the response is a SQL command result
   * @group validation
   */
  get isSqlCommandResult(): boolean {
    return this.resultType === "sqlCommandResult";
  }

  /**
   * Returns the SQL command result relation.
   * 
   * @returns The Relation from the SQL command result
   * @throws Error if the response type is not sqlCommandResult
   * 
   * @example
   * ```typescript
   * if (handler.isSqlCommandResult) {
   *   const relation = handler.sqlCommandResult;
   * }
   * ```
   * 
   * @group result
   */
  get sqlCommandResult(): Relation | undefined {
    if (this.resultValue.case !== "sqlCommandResult") {
      throw this.unexpectedType("sqlCommandResult");
    }
    return this.resultValue.value.relation;
  }

  private unexpectedType(expectedType: string): Error {
    return new Error(`Unexpected response type. Expected: ${expectedType}, Actual: ${this.resultType}`);
  }
}