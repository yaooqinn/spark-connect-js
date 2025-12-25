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

test("na.drop() - drop rows with any null", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, NULL), (NULL, 4) AS t(a, b)").then(async df => {
      const result = await df.na.drop().collect();
      expect(result.length).toBe(1);
      expect(result[0].getInt(0)).toBe(1);
      expect(result[0].getInt(1)).toBe(2);
    })
  );
});

test("na.drop('any') - drop rows with any null", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, NULL), (NULL, 4) AS t(a, b)").then(async df => {
      const result = await df.na.drop('any').collect();
      expect(result.length).toBe(1);
      expect(result[0].getInt(0)).toBe(1);
      expect(result[0].getInt(1)).toBe(2);
    })
  );
});

test("na.drop('all') - drop rows where all values are null", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, NULL), (NULL, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.drop('all').collect();
      expect(result.length).toBe(2);
    })
  );
});

test("na.drop(['a']) - drop rows with null in specified column", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (NULL, 3), (4, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.drop(['a']).collect();
      expect(result.length).toBe(2);
      expect(result[0].getInt(0)).toBe(1);
      expect(result[1].getInt(0)).toBe(4);
    })
  );
});

test("na.drop('any', ['b']) - drop rows with any null in specified columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (NULL, 3), (4, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.drop('any', ['b']).collect();
      expect(result.length).toBe(2);
      expect(result[0].getInt(1)).toBe(2);
      expect(result[1].getInt(1)).toBe(3);
    })
  );
});

test("na.drop(1) - drop rows with less than 1 non-null value", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, NULL), (NULL, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.drop(1).collect();
      expect(result.length).toBe(2);
    })
  );
});

test("na.fill(0) - fill all null numeric values with 0", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, NULL), (NULL, 4) AS t(a, b)").then(async df => {
      const result = await df.na.fill(0).collect();
      expect(result.length).toBe(3);
      expect(result[1].getInt(0)).toBe(3);
      expect(result[1].getInt(1)).toBe(0);
      expect(result[2].getInt(0)).toBe(0);
      expect(result[2].getInt(1)).toBe(4);
    })
  );
});

test("na.fill('unknown') - fill all null string values with 'unknown'", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES ('a', 'b'), ('c', NULL), (NULL, 'd') AS t(x, y)").then(async df => {
      const result = await df.na.fill('unknown').collect();
      expect(result.length).toBe(3);
      expect(result[1].getString(0)).toBe('c');
      expect(result[1].getString(1)).toBe('unknown');
      expect(result[2].getString(0)).toBe('unknown');
      expect(result[2].getString(1)).toBe('d');
    })
  );
});

test("na.fill(0, ['a']) - fill null values in specific column", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (NULL, 3), (4, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.fill(0, ['a']).collect();
      expect(result.length).toBe(3);
      expect(result[1].getInt(0)).toBe(0);
      expect(result[1].get(1)).toBe(3);
    })
  );
});

test("na.fill({a: 0, b: 99}) - fill with different values per column", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (NULL, 3), (4, NULL) AS t(a, b)").then(async df => {
      const result = await df.na.fill({ a: 0, b: 99 }).collect();
      expect(result.length).toBe(3);
      expect(result[1].getInt(0)).toBe(0);
      expect(result[1].getInt(1)).toBe(3);
      expect(result[2].getInt(0)).toBe(4);
      expect(result[2].getInt(1)).toBe(99);
    })
  );
});

test("na.replace('a', {1: 10, 3: 30}) - replace specific values in column", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, 4), (5, 6) AS t(a, b)").then(async df => {
      const result = await df.na.replace('a', { 1: 10, 3: 30 }).collect();
      expect(result.length).toBe(3);
      expect(result[0].getInt(0)).toBe(10);
      expect(result[0].getInt(1)).toBe(2);
      expect(result[1].getInt(0)).toBe(30);
      expect(result[1].getInt(1)).toBe(4);
      expect(result[2].getInt(0)).toBe(5);
      expect(result[2].getInt(1)).toBe(6);
    })
  );
});

test("na.replace(['a', 'b'], {2: 20, 4: 40}) - replace values in multiple columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT * FROM VALUES (1, 2), (3, 4), (5, 6) AS t(a, b)").then(async df => {
      const result = await df.na.replace(['a', 'b'], { 2: 20, 4: 40 }).collect();
      expect(result.length).toBe(3);
      expect(result[0].getInt(0)).toBe(1);
      expect(result[0].getInt(1)).toBe(20);
      expect(result[1].getInt(0)).toBe(3);
      expect(result[1].getInt(1)).toBe(40);
    })
  );
});
