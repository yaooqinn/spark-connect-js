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
import { Column } from '../../../../../../src/org/apache/spark/sql/Column';

/**
 * Example demonstrating various join operations in Spark Connect JavaScript client.
 */
async function runJoinExamples() {
  const spark = await SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate();

  console.log("=== DataFrame Join Examples ===\n");

  // Create sample DataFrames
  const employees = await spark.sql(`
    SELECT * FROM VALUES 
      (1, 'Alice', 1),
      (2, 'Bob', 2),
      (3, 'Charlie', 1),
      (4, 'David', 3)
    AS employees(id, name, dept_id)
  `);

  const departments = await spark.sql(`
    SELECT * FROM VALUES 
      (1, 'Engineering'),
      (2, 'Sales'),
      (3, 'Marketing'),
      (4, 'HR')
    AS departments(dept_id, dept_name)
  `);

  // Example 1: Inner join using column name
  console.log("1. Inner join using column name:");
  await employees.join(departments, "dept_id").show();

  // Example 2: Left outer join with join expression
  console.log("\n2. Left outer join:");
  const leftJoinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      employees.col("dept_id").expr,
      departments.col("dept_id").expr
    ])
  );
  await employees.join(departments, leftJoinExpr, "left").show();

  // Example 3: Cross join (Cartesian product)
  console.log("\n3. Cross join:");
  const small1 = await spark.sql("SELECT 'A' as letter");
  const small2 = await spark.sql("SELECT 1 as number UNION ALL SELECT 2 as number");
  await small1.crossJoin(small2).show();

  // Example 4: Left semi join (returns rows from left side that have match on right)
  console.log("\n4. Left semi join:");
  await employees.join(departments, leftJoinExpr, "semi").show();

  // Example 5: Left anti join (returns rows from left side that don't have match on right)
  console.log("\n5. Left anti join:");
  const noMatchEmps = await spark.sql(`
    SELECT * FROM VALUES 
      (5, 'Eve', 99)
    AS employees(id, name, dept_id)
  `);
  await noMatchEmps.join(departments, leftJoinExpr, "anti").show();

  // Example 6: Join with broadcast hint
  console.log("\n6. Join with broadcast hint (for performance):");
  const broadcastDepts = departments.hint("broadcast");
  await employees.join(broadcastDepts, "dept_id").show();

  // Example 7: Join using multiple columns
  console.log("\n7. Join using multiple columns:");
  const sales = await spark.sql(`
    SELECT * FROM VALUES 
      (1, 'Q1', 100),
      (1, 'Q2', 150)
    AS sales(dept_id, quarter, amount)
  `);
  const targets = await spark.sql(`
    SELECT * FROM VALUES 
      (1, 'Q1', 90),
      (1, 'Q2', 140)
    AS targets(dept_id, quarter, target)
  `);
  await sales.join(targets, ["dept_id", "quarter"]).show();

  // Example 8: Self join
  console.log("\n8. Self join:");
  const managers = await spark.sql(`
    SELECT * FROM VALUES 
      (1, 'Alice', null),
      (2, 'Bob', 1),
      (3, 'Charlie', 1),
      (4, 'David', 2)
    AS employees(id, name, manager_id)
  `);
  const managerDetails = managers.select(
    managers.col("id").as("mgr_id"),
    managers.col("name").as("mgr_name")
  );
  const selfJoinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      managers.col("manager_id").expr,
      managerDetails.col("mgr_id").expr
    ])
  );
  await managers.join(managerDetails, selfJoinExpr, "left").show();

  console.log("\n=== Join Examples Complete ===");
}

// Run the examples
runJoinExamples().catch(console.error);
