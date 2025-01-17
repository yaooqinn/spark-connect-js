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

export class ExecutePlanResponseHandler {
  private resultType: any;
  private resultValue: any;
  
  constructor(public readonly response: ExecutePlanResponse) {
    this.resultType = response.responseType.case;
    this.resultValue = response.responseType.value;
  }

  get sessionId(): string {
    return this.response.sessionId;
  }

  get serverSideSessionId(): string {
    return this.response.serverSideSessionId;
  }

  get operationId(): string {
    return this.response.operationId;
  }

  get responseId(): string {
    return this.response.responseId;
  }

  get result(): any {
    return this.response.responseType;
  }

  get isSqlCommandResult(): boolean {
    return this.resultType === "sqlCommandResult";
  }

  get sqlCommandResult(): Relation | undefined {
    if (this.resultType !== "sqlCommandResult") {
      throw this.unexpectedType("sqlCommandResult");
    }
    return (this.resultValue as ExecutePlanResponse_SqlCommandResult).relation;
  }

  private unexpectedType(expectedType: string): Error {
    return new Error(`Unexpected response type. Expected: ${expectedType}, Actual: ${this.resultType}`);
  }
}