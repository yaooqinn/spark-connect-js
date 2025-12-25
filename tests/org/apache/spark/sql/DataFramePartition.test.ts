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

import { sharedSpark, timeoutOrSatisfied } from '../../../../helpers';
import { col } from '../../../../../src/org/apache/spark/sql/functions';

test("repartition with numPartitions", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.range(0, 100).then(async df => {
    const repartitioned = df.repartition(10);
    return repartitioned.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});

test("repartition with columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT id % 10 as key, id FROM range(100)").then(async df => {
    const repartitioned = df.repartition(col("key"));
    return repartitioned.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});

test("repartition with numPartitions and columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT id % 10 as key, id FROM range(100)").then(async df => {
    const repartitioned = df.repartition(5, col("key"));
    return repartitioned.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});

test("coalesce", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.range(0, 100).then(async df => {
    const coalesced = df.coalesce(1);
    return coalesced.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});

test("repartitionByRange with columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT id FROM range(100)").then(async df => {
    const repartitioned = df.repartitionByRange(col("id"));
    return repartitioned.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});

test("repartitionByRange with numPartitions and columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT id FROM range(100)").then(async df => {
    const repartitioned = df.repartitionByRange(5, col("id"));
    return repartitioned.collect().then(rows => {
      expect(rows.length).toBe(100);
    });
  }));
});
