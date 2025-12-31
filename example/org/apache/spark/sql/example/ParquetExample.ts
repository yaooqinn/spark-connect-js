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
import { rm } from "fs";
import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession";

// Get data path - works both locally and in CI with remote Spark server
const dataPath = process.env.SPARK_REMOTE_DATA_PATH || __dirname + "/data";

async function runExample(): Promise<void> {
  const spark = await SparkSession.builder().appName("ParquetExample").getOrCreate();
  const df = spark.read.parquet(dataPath + "/users.parquet")
  // await df.printSchema();
  await df.show();
  const df2 = df.selectExpr("name", "explode(favorite_numbers) as numbers");
  await df2.show();
  try {
    await df2.write.mode("overwrite").parquet(dataPath + "/tmp");
    const df3 = spark.read.parquet(dataPath + "/tmp");
    await df3.collect().then(rows => rows.forEach(r => console.log(r.toJSON())));
  } finally {
    rm(__dirname + "/data/tmp", { recursive: true }, () => {});
  }
  // await spark.stop();
}

runExample();
