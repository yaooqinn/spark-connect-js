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

import { SparkSession } from "../../../../../src/org/apache/spark/sql/SparkSession";

test("builder", async () => {
  const spark = SparkSession
    .builder()
    .remote('sc://localhost')
    .appName('test')
    // change default value from 200 to 2024
    .config('spark.sql.shuffle.partitions', '1024')
    .config('spark.kent.yao', 'awesome')
    .getOrCreate();

    await spark.then(async s => {
      await s.version().then(version => {
        expect(version).toContain("4.");
      });
      await s.conf.get("spark.sql.shuffle.partitions").then(value => {
        expect(value).toBe("1024");
      });
      await s.conf.getAll().then(configs => {
        expect(configs.get("spark.kent.yao")).toBe("awesome");
      });
    });
});
