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

import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession";
import { Window } from "../../../../../../src/org/apache/spark/sql/Window";
import * as F from "../../../../../../src/org/apache/spark/sql/functions";

/**
 * Example demonstrating window functions usage.
 */
async function runWindowExample() {
  const spark = await SparkSession.builder()
    .appName("WindowExample")
    .getOrCreate();

  // Create a sample DataFrame with employee data
  const df = await spark.sql(`
    SELECT 'Alice' as name, 'Engineering' as department, 90000 as salary UNION ALL
    SELECT 'Bob' as name, 'Engineering' as department, 85000 as salary UNION ALL
    SELECT 'Charlie' as name, 'Engineering' as department, 95000 as salary UNION ALL
    SELECT 'Diana' as name, 'Sales' as department, 70000 as salary UNION ALL
    SELECT 'Eve' as name, 'Sales' as department, 75000 as salary UNION ALL
    SELECT 'Frank' as name, 'Sales' as department, 72000 as salary
  `);

  console.log("Original DataFrame:");
  await df.show();

  // Example 1: Ranking within departments
  const windowSpec1 = Window
    .partitionBy("department")
    .orderBy("salary");

  const rankedDf = await df.select(
    F.col("name"),
    F.col("department"),
    F.col("salary"),
    F.row_number().over(windowSpec1).as("row_num"),
    F.rank().over(windowSpec1).as("rank"),
    F.dense_rank().over(windowSpec1).as("dense_rank")
  );

  console.log("\nRanking within departments:");
  await rankedDf.show();

  // Example 2: Running totals
  const windowSpec2 = Window
    .partitionBy("department")
    .orderBy("salary")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow);

  const runningTotalDf = await df.select(
    F.col("name"),
    F.col("department"),
    F.col("salary"),
    F.sum(F.col("salary")).over(windowSpec2).as("running_total")
  );

  console.log("\nRunning totals by department:");
  await runningTotalDf.show();

  // Example 3: Lag and Lead
  const windowSpec3 = Window
    .partitionBy("department")
    .orderBy("salary");

  const lagLeadDf = await df.select(
    F.col("name"),
    F.col("department"),
    F.col("salary"),
    F.lag("salary", 1).over(windowSpec3).as("prev_salary"),
    F.lead("salary", 1).over(windowSpec3).as("next_salary")
  );

  console.log("\nLag and Lead example:");
  await lagLeadDf.show();

  // Example 4: Ntile for quartiles
  const windowSpec4 = Window
    .partitionBy("department")
    .orderBy("salary");

  const quartilesDf = await df.select(
    F.col("name"),
    F.col("department"),
    F.col("salary"),
    F.ntile(4).over(windowSpec4).as("quartile")
  );

  console.log("\nQuartiles by department:");
  await quartilesDf.show();

  // Example 5: Moving average (3-row window)
  const windowSpec5 = Window
    .partitionBy("department")
    .orderBy("salary")
    .rowsBetween(-1, 1);

  const movingAvgDf = await df.select(
    F.col("name"),
    F.col("department"),
    F.col("salary"),
    F.avg(F.col("salary")).over(windowSpec5).as("moving_avg")
  );

  console.log("\nMoving average (3-row window):");
  await movingAvgDf.show();

  await spark.stop();
}

// Run the example if executed directly
if (require.main === module) {
  runWindowExample().catch(console.error);
}

export { runWindowExample };
