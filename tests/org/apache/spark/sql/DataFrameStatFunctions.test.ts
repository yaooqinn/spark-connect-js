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

import { sharedSpark } from '../../../../helpers';
import { col } from '../../../../../src/org/apache/spark/sql/functions';

test("stat.cov api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0 as a, 2.0 as b UNION ALL SELECT 2.0 as a, 3.0 as b UNION ALL SELECT 3.0 as a, 4.0 as b").then(async df => {
      const covariance = await df.stat.cov("a", "b");
      expect(covariance).toBeCloseTo(1.0, 5);
    })
  );
});

test("stat.corr api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0 as a, 2.0 as b UNION ALL SELECT 2.0 as a, 3.0 as b UNION ALL SELECT 3.0 as a, 4.0 as b").then(async df => {
      const correlation = await df.stat.corr("a", "b");
      expect(correlation).toBeCloseTo(1.0, 5);
    })
  );
});

test("stat.corr with method api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0 as a, 2.0 as b UNION ALL SELECT 2.0 as a, 3.0 as b UNION ALL SELECT 3.0 as a, 4.0 as b").then(async df => {
      const correlation = await df.stat.corr("a", "b", "pearson");
      expect(correlation).toBeCloseTo(1.0, 5);
    })
  );
});

test("stat.crosstab api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 'a' as col1, 'x' as col2 UNION ALL SELECT 'a' as col1, 'y' as col2 UNION ALL SELECT 'b' as col1, 'x' as col2").then(async df => {
      const crosstabDf = df.stat.crosstab("col1", "col2");
      const rows = await crosstabDf.collect();
      expect(rows.length).toBeGreaterThan(0);
    })
  );
});

test("stat.freqItems api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 'a' as col1, 1 as col2 UNION ALL SELECT 'a' as col1, 2 as col2 UNION ALL SELECT 'b' as col1, 1 as col2").then(async df => {
      const freqDf = df.stat.freqItems(["col1", "col2"]);
      const rows = await freqDf.collect();
      expect(rows.length).toBe(1);
    })
  );
});

test("stat.freqItems with support api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 'a' as col1, 1 as col2 UNION ALL SELECT 'a' as col1, 2 as col2 UNION ALL SELECT 'b' as col1, 1 as col2").then(async df => {
      const freqDf = df.stat.freqItems(["col1", "col2"], 0.4);
      const rows = await freqDf.collect();
      expect(rows.length).toBe(1);
    })
  );
});

test("stat.sampleBy api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 'a' as key, 1 as value UNION ALL SELECT 'a' as key, 2 as value UNION ALL SELECT 'b' as key, 3 as value UNION ALL SELECT 'b' as key, 4 as value").then(async df => {
      const fractions = new Map();
      fractions.set("a", 0.5);
      fractions.set("b", 0.5);
      const sampledDf = df.stat.sampleBy(col("key"), fractions, 42);
      const rows = await sampledDf.collect();
      expect(rows.length).toBeGreaterThanOrEqual(0);
    })
  );
});

test("stat.approxQuantile api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0 as a, 2.0 as b UNION ALL SELECT 2.0 as a, 3.0 as b UNION ALL SELECT 3.0 as a, 4.0 as b UNION ALL SELECT 4.0 as a, 5.0 as b").then(async df => {
      const quantileDf = df.stat.approxQuantile(["a", "b"], [0.0, 0.5, 1.0], 0.01);
      const rows = await quantileDf.collect();
      expect(rows.length).toBeGreaterThan(0);
    })
  );
});

test("stat.approxQuantile exact api", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0 as a UNION ALL SELECT 2.0 as a UNION ALL SELECT 3.0 as a UNION ALL SELECT 4.0 as a UNION ALL SELECT 5.0 as a").then(async df => {
      const quantileDf = df.stat.approxQuantile(["a"], [0.0, 0.25, 0.5, 0.75, 1.0], 0.0);
      const rows = await quantileDf.collect();
      expect(rows.length).toBeGreaterThan(0);
    })
  );
});
