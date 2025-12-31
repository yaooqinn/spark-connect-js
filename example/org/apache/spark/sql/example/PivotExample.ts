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

import { SparkSession } from '../../../../../../src/org/apache/spark/sql/SparkSession';
import { StructType } from '../../../../../../src/org/apache/spark/sql/types/StructType';
import { DataTypes } from '../../../../../../src/org/apache/spark/sql/types';
import { Row } from '../../../../../../src/org/apache/spark/sql/Row';
import { col } from '../../../../../../src/org/apache/spark/sql/functions';

async function runPivotExample() {
  const spark = await SparkSession.builder()
    .remote('sc://localhost:15002')
    .getOrCreate();

  // Create sample data for sales across different products and years
  const schema = new StructType()
    .add("year", DataTypes.IntegerType)
    .add("product", DataTypes.StringType)
    .add("country", DataTypes.StringType)
    .add("amount", DataTypes.IntegerType);

  const data = [
    new Row(schema, { year: 2019, product: "Product A", country: "USA", amount: 1000 }),
    new Row(schema, { year: 2019, product: "Product B", country: "USA", amount: 1500 }),
    new Row(schema, { year: 2019, product: "Product A", country: "Canada", amount: 800 }),
    new Row(schema, { year: 2020, product: "Product A", country: "USA", amount: 1200 }),
    new Row(schema, { year: 2020, product: "Product B", country: "USA", amount: 1800 }),
    new Row(schema, { year: 2020, product: "Product A", country: "Canada", amount: 900 }),
  ];

  const df = spark.createDataFrame(data, schema);

  console.log("Original DataFrame:");
  await df.show();

  // Example 1: Pivot without explicit values
  console.log("\nExample 1: Pivot products by year (without explicit values)");
  const pivoted1 = df.groupBy("year", "country").pivot("product").sum("amount");
  await pivoted1.show();

  // Example 2: Pivot with explicit values (more efficient)
  console.log("\nExample 2: Pivot products by year (with explicit values)");
  const pivoted2 = df.groupBy("year", "country").pivot("product", ["Product A", "Product B"]).sum("amount");
  await pivoted2.show();

  // Example 3: Pivot with multiple aggregations
  console.log("\nExample 3: Multiple aggregations with pivot");
  const pivoted3 = df.groupBy("country").pivot("year", [2019, 2020]).sum("amount");
  await pivoted3.show();

  // await spark.stop();
}

async function runGroupingSetsExample() {
  const spark = await SparkSession.builder()
    .remote('sc://localhost:15002')
    .getOrCreate();

  // Create sample data
  const schema = new StructType()
    .add("department", DataTypes.StringType)
    .add("team", DataTypes.StringType)
    .add("sales", DataTypes.IntegerType);

  const data = [
    new Row(schema, { department: "Engineering", team: "Backend", sales: 1000 }),
    new Row(schema, { department: "Engineering", team: "Frontend", sales: 1200 }),
    new Row(schema, { department: "Sales", team: "Enterprise", sales: 2000 }),
    new Row(schema, { department: "Sales", team: "SMB", sales: 1500 }),
  ];

  const df = spark.createDataFrame(data, schema);

  console.log("Original DataFrame:");
  await df.show();

  // GroupingSets example - compute aggregates for different grouping combinations
  console.log("\nGroupingSets: Aggregating by department and team combinations");
  const grouped = df.groupingSets([
    [col("department")],           // Group by department only
    [col("department"), col("team")] // Group by both department and team
  ]);
  const result = grouped.sum("sales");
  await result.show();

  // await spark.stop();
}

// Run examples sequentially
async function runAllExamples() {
  try {
    await runPivotExample();
    console.log("Pivot example completed");
    await runGroupingSetsExample();
    console.log("GroupingSets example completed");
  } catch (error) {
    console.error(error);
    throw error;
  }
}

runAllExamples();
