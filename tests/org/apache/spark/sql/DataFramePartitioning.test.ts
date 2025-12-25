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
  await timeoutOrSatisfied((async () => {
    const df = spark.range(0n, 100n, 1n, 10);
    const repartitioned = df.repartition(5);
    const plan = repartitioned.plan;
    expect(plan.relation?.relType.case).toBe("repartition");
    if (plan.relation?.relType.case === "repartition") {
      expect(plan.relation.relType.value.numPartitions).toBe(5);
      expect(plan.relation.relType.value.shuffle).toBe(true);
    }
    // Verify it can be collected
    const rows = await repartitioned.collect();
    expect(rows.length).toBe(100);
  })());
});

test("repartition with columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 as a, 2 as b, 3 as c").then(async df => {
    const repartitioned = df.repartition(col("a"), col("b"));
    const plan = repartitioned.plan;
    expect(plan.relation?.relType.case).toBe("repartitionByExpression");
    if (plan.relation?.relType.case === "repartitionByExpression") {
      expect(plan.relation.relType.value.partitionExprs.length).toBe(2);
      expect(plan.relation.relType.value.numPartitions).toBeUndefined();
    }
    // Verify it can be collected
    const rows = await repartitioned.collect();
    expect(rows.length).toBe(1);
  }));
});

test("repartition with numPartitions and columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 as a, 2 as b, 3 as c").then(async df => {
    const repartitioned = df.repartition(10, col("a"), col("b"));
    const plan = repartitioned.plan;
    expect(plan.relation?.relType.case).toBe("repartitionByExpression");
    if (plan.relation?.relType.case === "repartitionByExpression") {
      expect(plan.relation.relType.value.partitionExprs.length).toBe(2);
      expect(plan.relation.relType.value.numPartitions).toBe(10);
    }
    // Verify it can be collected
    const rows = await repartitioned.collect();
    expect(rows.length).toBe(1);
  }));
});

test("coalesce", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied((async () => {
    const df = spark.range(0n, 100n, 1n, 10);
    const coalesced = df.coalesce(2);
    const plan = coalesced.plan;
    expect(plan.relation?.relType.case).toBe("repartition");
    if (plan.relation?.relType.case === "repartition") {
      expect(plan.relation.relType.value.numPartitions).toBe(2);
      expect(plan.relation.relType.value.shuffle).toBe(false);
    }
    // Verify it can be collected
    const rows = await coalesced.collect();
    expect(rows.length).toBe(100);
  })());
});

test("repartitionByRange with columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied((async () => {
    const df = spark.range(0n, 100n, 1n, 10);
    const repartitioned = df.repartitionByRange(col("id"));
    const plan = repartitioned.plan;
    expect(plan.relation?.relType.case).toBe("repartitionByExpression");
    if (plan.relation?.relType.case === "repartitionByExpression") {
      expect(plan.relation.relType.value.partitionExprs.length).toBe(1);
      expect(plan.relation.relType.value.numPartitions).toBeUndefined();
    }
    // Verify it can be collected
    const rows = await repartitioned.collect();
    expect(rows.length).toBe(100);
  })());
});

test("repartitionByRange with numPartitions and columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied((async () => {
    const df = spark.range(0n, 100n, 1n, 10);
    const repartitioned = df.repartitionByRange(5, col("id"));
    const plan = repartitioned.plan;
    expect(plan.relation?.relType.case).toBe("repartitionByExpression");
    if (plan.relation?.relType.case === "repartitionByExpression") {
      expect(plan.relation.relType.value.partitionExprs.length).toBe(1);
      expect(plan.relation.relType.value.numPartitions).toBe(5);
    }
    // Verify it can be collected
    const rows = await repartitioned.collect();
    expect(rows.length).toBe(100);
  })());
});

