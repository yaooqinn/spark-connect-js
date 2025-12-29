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
import { logger } from "../src/org/apache/spark/logger";

export const sharedSpark = SparkSession.builder()
  .remote(`sc://localhost:15002/;user_id=${currentUser()};user_name=${currentUser()}`)
  .getOrCreate();

export function currentUser(): string {
  // For browser compatibility, use a default user
  return "testuser";
}

export function delay(ms: number) {
  return new Promise( resolve => setTimeout(resolve, ms) );
}

export async function withDatabase(
    spark: SparkSession,
    dbName: string,
    fn: (dbName: string) => Promise<any>): Promise<void> {
  try {
    await fn(dbName);
  } finally {
    await spark.sql(`DROP DATABASE IF EXISTS ${dbName} CASCADE`);
  }
}

export async function withTable(
    spark: SparkSession,
    tableName: string,
    fn: (tableName: string) => Promise<any>): Promise<void> {
  try {
    await fn(tableName);
  } finally {
    await spark.sql(`DROP TABLE IF EXISTS ${tableName}`);
  }
}

export async function withTempDir(fn: (dir: string) => Promise<any>): Promise<void> {
  // For browser environments, use in-memory paths or skip file-based tests
  const tempDir = `/tmp/spark-test-${Date.now()}`;
  try {
    await fn(tempDir);
  } finally {
    logger.warn(`Temporary directory cleanup not available in browser: ${tempDir}`);
  }
}
