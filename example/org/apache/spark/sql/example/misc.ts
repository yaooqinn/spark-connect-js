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

import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession"
import { Float32, tableFromIPC, Type, vectorFromArray } from 'apache-arrow';
import { SparkResult } from "../../../../../../src/org/apache/spark/sql/SparkResult";
import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types";
import { Database } from "../../../../../../src/org/apache/spark/sql/catalog/Database";

async function run() {
  const spark = await SparkSession.builder()
    .appName("example")
    .getOrCreate();

  
  const df = await spark.sql(`SELECT
    1Y AS B,
    2S AS S,
    3 AS I,
    9007199254740991L AS L,
    5.6f AS F,
    7.8d AS D,
    9.0bd AS BD,
    'My way or high way' AS ST,
    X'10011' AS BIN,
    date'2018-11-17' AS DATE,
    timestamp'2021-01-01 01:00:00' tt,
    timestamp'2018-11-17 13:33:33' ttt,
    timestamp'2018-11-17 13:33:33.333' AS TIMESTAMP,
    timestamp_ntz'2018-11-17 13:33:33.333' AS TIMESTAMP_NTZ,
    array(1, 2, 3) AS ARR,
    array('x', 'y', 'z') AS STR_ARR,
    array(1.0f, 2.0f, 3.0f) AS FARR,
    array(1.0bd, 2.0bd, 9999999999999.99bd) AS FARR2,
    array(array(1, 2, 3), array(4, 5, 6)) AS ARR2,
    map(1, 'a', 2, 'b') AS MAP1,
    map(1, 2, 3, 4) AS MAP2,
    map(1, array(1, 2, 3), 2, array(4, 5, 6)) AS MAP3,
    map(1, map(1, 'a', 2, 'b'), 2, map(3, 'c', 4, 'd')) AS MAP4,
    named_struct('a', array('x', 'y', 'z', null), 'b', map(1, 'a', 2, 'b'), 'c', named_struct('a', 1.0)) AS NAMED_STRUCT
    from range(1)`);

    await df.show(20, false, true);
    const res = await df.collect();
    res.forEach(row => {
      console.log('=======\n', row);
    });

    const df2 = await spark.sql("SELECT timestamp'2021-01-01 01:00:00' a, timestamp'2018-11-17 13:33:33' `b.c`")
    const row = await df2.head();
    const a = row.getTimestamp(0);
    const b = row.getLong(1);
    console.log(a, b);

run().catch(console.error);