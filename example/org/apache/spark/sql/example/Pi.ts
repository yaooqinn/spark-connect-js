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

import { pow } from "../../../../../../src/org/apache/spark/sql/functions";
import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession"

async function pi() {
  const spark = await SparkSession.builder().appName("Pi").getOrCreate();
  const slice = parseInt(process.argv[2]) || 2;
  const n = Math.min(100000 * slice, Number.MAX_SAFE_INTEGER);
  await spark.range(1, n, 1, slice)
    .selectExpr("rand() * 2 - 1 as x", "rand() * 2 - 1 as y")
    .select(pow("x", 2).add(pow("y", 2)).as("v"))
    .filter("v < 1")
    .count()
    .then(count => console.log("Pi is roughly " + 4.0 * Number(count) / n));
}

pi();
