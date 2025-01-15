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

import { SparkSession } from "../src/org/apache/spark/sql/SparkSession";
import { dir, setGracefulCleanup } from "tmp";
import { promisify } from "util";
import { rm } from "fs";
import { logger } from "../src/org/apache/spark/logger";
import os from "os";

// Clean up the temporary directory
setGracefulCleanup();

const createTempDir = promisify(dir);

export const sharedSpark = SparkSession.builder()
  .remote(`sc://localhost:15002/;user_id=${currentUser()};user_name=${currentUser()}`)
  .getOrCreate();

export function currentUser(): string {
  return process.env.USER || os.userInfo().username;
}

export function delay(ms: number) {
  return new Promise( resolve => setTimeout(resolve, ms) );
}

export function timeoutOrSatisfied(promise: Promise<any>, timeout: number = 30000): Promise<void> {
  const timeoutPromise = new Promise<void>((_, reject) => {
    setTimeout(() => {
      reject(new Error("Timeout"));
    }, timeout);
  });
  return Promise.race([promise, timeoutPromise]);
}

export async function withTable(
    spark: SparkSession,
    tableName: string,
    fn: (tableName: string) => Promise<any>, timeout: number = 30000): Promise<void> {
  try {
    await spark.sql(`DROP TABLE IF EXISTS ${tableName}`);
    await timeoutOrSatisfied(fn(tableName), timeout);
  } finally {
    await spark.sql(`DROP TABLE IF EXISTS ${tableName}`);
  }
}

export async function withTempDir(fn: (dir: string) => Promise<any>, timeout: number = 30000): Promise<void> {
  return createTempDir().then(async (dir) => {
    try {
      await timeoutOrSatisfied(fn(dir), timeout);
    } finally {
      // Clean up the temporary directory
      rm(dir, { recursive: true }, (err) => {
        if (err) {
          logger.error(`Failed to remove ${dir}: ${err}`);
        }
      });
    }
  });
}
