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

import { SparkSession } from '../../src/org/apache/spark/sql/SparkSession';

/**
 * Example demonstrating DataFrame NA (missing data) functions.
 * This shows how to use fill, drop, and replace operations on DataFrames.
 */
async function main() {
  // Create a Spark session
  const spark = await SparkSession.builder()
    .remote('sc://localhost:15002')
    .getOrCreate();

  console.log('=== DataFrame NA Functions Example ===\n');

  // Create a DataFrame with null values
  const df = await spark.sql(
    "SELECT * FROM VALUES (1, 2, 'a'), (3, NULL, 'b'), (NULL, 4, NULL) AS t(id, value, name)"
  );

  console.log('Original DataFrame:');
  await df.show();

  // Example 1: Drop rows with any null values
  console.log('\n1. Drop rows with any null values:');
  await df.na.drop().show();

  // Example 2: Drop rows where all values are null
  console.log('\n2. Drop rows where all values are null:');
  await df.na.drop('all').show();

  // Example 3: Drop rows with null in specific columns
  console.log('\n3. Drop rows with null in "id" column:');
  await df.na.drop(['id']).show();

  // Example 4: Fill null values with a default value
  console.log('\n4. Fill all null numeric values with 0:');
  await df.na.fill(0).show();

  // Example 5: Fill null values with different values per column
  console.log('\n5. Fill null values with different values per column:');
  await df.na.fill({ id: 0, value: 99, name: 'unknown' }).show();

  // Example 6: Replace specific values
  console.log('\n6. Replace values in "id" column (1 -> 100, 3 -> 300):');
  await df.na.replace('id', { 1: 100, 3: 300 }).show();

  // Example 7: Replace values in multiple columns
  console.log('\n7. Replace values in multiple columns:');
  await df.na.replace(['id', 'value'], { 2: 200, 4: 400 }).show();

  console.log('\n=== Example Complete ===');
}

main().catch(console.error);
