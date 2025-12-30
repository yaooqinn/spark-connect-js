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
import { lit } from "../../../../../../src/org/apache/spark/sql/functions";

async function runExample(): Promise<void> {
  const spark = await SparkSession.builder().appName("CSVExample").getOrCreate();
  const df = spark.read
    .option("delimiter", ";")
    .option("header", true)
    .csv(__dirname + "/data/people.csv");
  await df.show()
  const df2 = df.select(df.col("name"), df.col("age").plus(lit(1)).as("age"));
  await df2.show()
  try {
    await df2.write.mode("overwrite").parquet(__dirname + "/data/tmp");
    const df3 = spark.read.parquet(__dirname + "/data/tmp");
    await df3.collect().then(rows => rows.forEach(r => console.log(r.toJSON())));
  } finally {
    // Note: File cleanup should be handled server-side or manually
    console.log("Temporary files created at: " + __dirname + "/data/tmp");
  }
  // await spark.stop();
}

runExample();
