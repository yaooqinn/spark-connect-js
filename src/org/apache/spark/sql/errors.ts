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

import * as grpc from '@grpc/grpc-js';

abstract class SparkThrowable extends Error {
  constructor(
      public condition: string,
      public msg: string,
      public sqlState: string | undefined = undefined) {
    let cond = "";
    if (!condition.startsWith("_LEGACY_ERROR_TEMP")) {
      cond = `[${condition}] `;
    }
    const state = sqlState ? ` SQLSTATE: ${sqlState}` : "";
    super(`${cond}${msg}${state}`);
    Object.setPrototypeOf(this, new.target.prototype);
  }

  override toString(): string {
    return this.message;
  }
}

export function fromStatus(err: grpc.StatusObject & Error | grpc.StatusObject): Error {
  const statusDetails = err.metadata.get("grpc-status-details-bin").toString();
  let msg = err.details;
  const conditionMatch = msg.match(/\[([A-Z_\.]+)\]/);
  const condition = conditionMatch ? conditionMatch[1] : "_LEGACY_ERROR_TEMP_00000";
  const sqlStateMatch = msg.match(/SQLSTATE:\s*([0-9A-Z]+)/);
  const sqlState = sqlStateMatch ? sqlStateMatch[1] : "00000";
  if (conditionMatch) {
    msg = msg.replace(conditionMatch[0], "").trim();
  }
  if (sqlStateMatch) {
    msg = msg.replace(sqlStateMatch[0], "").trim();
  }

  let error: Error;
  if (statusDetails.includes("org.apache.spark.sql.AnalysisException")) {
    error = new AnalysisException(condition, msg, sqlState);
  } else if (statusDetails.includes("org.apache.spark.SparkRuntimeException")) {
    error = new SparkRuntimeException(condition, msg, sqlState);
  } else if (statusDetails.includes("org.apache.spark.SparkNoSuchElementException")) {
    error = new SparkRuntimeException(condition, msg, sqlState);
  } else if ( err instanceof Error ) {
    return err;
  } else {
    error = new Error(msg);
  }

  if ('stack' in err) {
    error.stack = err.stack;
  }
  return error;
}

export class AnalysisException extends SparkThrowable {}

export class SparkRuntimeException extends SparkThrowable {}

export class SparkNoSuchElementException extends SparkThrowable {}

export class SparkUnsupportedOperationException extends SparkThrowable {}