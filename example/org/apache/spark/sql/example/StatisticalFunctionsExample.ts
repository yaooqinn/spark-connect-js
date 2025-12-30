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

import { col } from "../../../../../../src/org/apache/spark/sql/functions";
import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession"

async function statisticalFunctionsExample() {
  const spark = await SparkSession.builder()
    .appName("Statistical Functions Example")
    .getOrCreate();

  console.log("=== DataFrame Statistical Functions Examples ===\n");

  // Example 1: Correlation and Covariance
  console.log("1. Correlation and Covariance:");
  const numericDf = await spark.sql(
    "SELECT * FROM values (1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0) AS data(a, b)"
  );
  
  const covariance = await numericDf.stat.cov("a", "b");
  console.log(`   Covariance between a and b: ${covariance}`);
  
  const correlation = await numericDf.stat.corr("a", "b");
  console.log(`   Correlation between a and b: ${correlation}\n`);

  // Example 2: Crosstab (Contingency Table)
  console.log("2. Crosstab (Contingency Table):");
  const categoricalDf = await spark.sql(
    "SELECT * FROM values ('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'y'), ('a', 'x') AS data(col1, col2)"
  );
  
  const crosstabDf = categoricalDf.stat.crosstab("col1", "col2");
  console.log("   Crosstab result:");
  await crosstabDf.show();

  // Example 3: Frequent Items
  console.log("3. Frequent Items:");
  const itemsDf = await spark.sql(
    "SELECT * FROM values ('a', 1), ('a', 1), ('a', 2), ('b', 1), ('b', 1) AS data(col1, col2)"
  );
  
  const freqDf = itemsDf.stat.freqItems(["col1", "col2"], 0.4);
  console.log("   Frequent items (support >= 0.4):");
  await freqDf.show();

  // Example 4: Stratified Sampling
  console.log("4. Stratified Sampling:");
  const sampleDf = await spark.sql(
    "SELECT * FROM values ('a', 1), ('a', 2), ('a', 3), ('b', 4), ('b', 5), ('b', 6) AS data(key, value)"
  );
  
  const fractions = new Map();
  fractions.set("a", 0.5);
  fractions.set("b", 0.5);
  
  const sampledDf = sampleDf.stat.sampleBy(col("key"), fractions, 42);
  console.log("   Stratified sample (50% from each stratum):");
  await sampledDf.show();

  // Example 5: Approximate Quantiles
  console.log("5. Approximate Quantiles:");
  const quantileDf = await spark.sql(
    "SELECT * FROM values (1.0), (2.0), (3.0), (4.0), (5.0), (6.0), (7.0), (8.0), (9.0), (10.0) AS data(value)"
  );
  
  const quantiles = quantileDf.stat.approxQuantile(["value"], [0.0, 0.25, 0.5, 0.75, 1.0], 0.01);
  console.log("   Quantiles (min, Q1, median, Q3, max):");
  await quantiles.show();

  console.log("\n=== Examples completed ===");
}

statisticalFunctionsExample();
