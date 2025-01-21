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

import { AnalysisException } from "../../../../../../src/org/apache/spark/sql/errors";
import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession";
import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types";

async function catalogExample() {
  const dbName = "example_db";
  const tableName = "example_table";
  const tableName2 = "example_table2";
  const tableName3 = "example_table3";
  const tableName4 = "example_table4";
  const spark = await SparkSession.builder().appName().getOrCreate();
  try {
    await spark.sql(`CREATE DATABASE IF NOT EXISTS ${dbName}`);
    // Catalog Database APIs
    let current_db = await spark.catalog.currentDatabase();
    console.log(`============\nThe current database name is '${current_db}'`);
    await spark.catalog.listDatabases().show();
    await spark.catalog.setCurrentDatabase(dbName);
    current_db = await spark.catalog.currentDatabase();
    console.log(`============\nThe current database name is set to '${current_db}'`);
    const database = await spark.catalog.getDatabase(dbName);
    console.log(`============\nGet database: ${database}`);
    // Catalog Table APIs
    const schema = DataTypes.createStructType([
      DataTypes.createStructField("key", DataTypes.IntegerType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false)
    ]);
    await spark.catalog.createTable(dbName + '.' + tableName, 'parquet', schema, new Map<string, string>()).show();
    await spark.sql(`INSERT INTO ${dbName}.${tableName} VALUES (1, 'a'), (2, 'b')`);
    await spark.table(dbName + '.' + tableName).show();
    await spark.catalog.createTable(dbName + '.' + tableName2, 'orc', schema, "A COMMENT", new Map<string, string>()).show();
    await spark.sql(`INSERT INTO ${dbName}.${tableName2} VALUES (3, 'c'), (4, 'd')`);
    await spark.table(dbName + '.' + tableName2).show();
    await spark.catalog.createTable(dbName + '.' + tableName3, __dirname + '/data/users.parquet').show();
    await spark.table(dbName + '.' + tableName3).show();
    await spark.catalog.createTable(dbName + '.' + tableName4, __dirname + '/data/people.json', 'json').head()
    await spark.table(dbName + '.' + tableName4).show();
    await spark.catalog.listTables().show();
    let table = await spark.catalog.getTable(dbName, tableName);
    console.log(`===========\nGet table: ${table}`);
    table = await spark.catalog.getTable(tableName2);
    console.log(`===========\nGet table: ${table}`);
    try {
      await spark.catalog.getTable("default", tableName2);
    } catch (e) {
      console.log(`===========\nERRORED to get table: ${(e as AnalysisException).condition}`);
    }
    table = await spark.catalog.getTable(tableName3);
    console.log(`===========\nGet table: ${table}`);
  } finally {
    await spark.sql(`DROP TABLE IF EXISTS ${dbName}.${tableName}`);
    await spark.sql(`DROP TABLE IF EXISTS ${dbName}.${tableName2}`);
    await spark.sql(`DROP TABLE IF EXISTS ${dbName}.${tableName3}`);
    await spark.sql(`DROP TABLE IF EXISTS ${dbName}.${tableName4}`);
    (await spark.sql(`DROP DATABASE IF EXISTS ${dbName} CASCADE`)).show();
    spark.catalog.listDatabases().show();
  }
};

catalogExample();
