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

test("union - removes duplicates", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = spark.range(1, 6); // [1, 2, 3, 4, 5]
    const df2 = spark.range(3, 8); // [3, 4, 5, 6, 7]
    const result = df1.union(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // union should remove duplicates: [1, 2, 3, 4, 5, 6, 7]
    expect(values).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });
});

test("unionAll - keeps duplicates", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = spark.range(1, 6); // [1, 2, 3, 4, 5]
    const df2 = spark.range(3, 8); // [3, 4, 5, 6, 7]
    const result = df1.unionAll(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // unionAll should keep duplicates: [1, 2, 3, 3, 4, 4, 5, 5, 6, 7]
    expect(values).toEqual([1, 2, 3, 3, 4, 4, 5, 5, 6, 7]);
    expect(values.length).toBe(10);
  });
});

test("unionByName - matches columns by name", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = await spark.sql("SELECT 1 as a, 2 as b");
    const df2 = await spark.sql("SELECT 3 as b, 4 as a"); // different column order
    const result = df1.unionByName(df2);
    
    const rows = await result.collect();
    expect(rows.length).toBe(2);
    
    // First row: a=1, b=2
    expect(Number(rows[0].get(0))).toBe(1);
    expect(Number(rows[0].get(1))).toBe(2);
    
    // Second row: a=4, b=3 (matched by name, not position)
    expect(Number(rows[1].get(0))).toBe(4);
    expect(Number(rows[1].get(1))).toBe(3);
  });
});

test("unionByName - with allowMissingColumns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = await spark.sql("SELECT 1 as a, 2 as b");
    const df2 = await spark.sql("SELECT 3 as a, 4 as c"); // different column 'c' instead of 'b'
    const result = df1.unionByName(df2, true);
    
    const rows = await result.collect();
    expect(rows.length).toBe(2);
    
    const schema = await result.schema();
    const columnNames = schema.fieldNames();
    
    // Should have all columns: a, b, c
    expect(columnNames.sort()).toEqual(['a', 'b', 'c']);
  });
});

test("intersect - returns distinct common rows", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = spark.range(1, 8); // [1, 2, 3, 4, 5, 6, 7]
    const df2 = spark.range(3, 10); // [3, 4, 5, 6, 7, 8, 9]
    const result = df1.intersect(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // intersect should return [3, 4, 5, 6, 7]
    expect(values).toEqual([3, 4, 5, 6, 7]);
  });
});

test("intersectAll - returns all common rows with duplicates", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    // Create DataFrames with duplicates
    const df1 = await spark.sql("SELECT * FROM VALUES (1), (2), (2), (3), (3), (3) AS t(id)");
    const df2 = await spark.sql("SELECT * FROM VALUES (2), (2), (2), (3), (3) AS t(id)");
    const result = df1.intersectAll(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // intersectAll should preserve duplicates based on minimum occurrences
    // df1 has: 1(1x), 2(2x), 3(3x)
    // df2 has: 2(3x), 3(2x)
    // result: 2(2x), 3(2x)
    expect(values).toEqual([2, 2, 3, 3]);
  });
});

test("except - returns rows in df1 but not in df2 (distinct)", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = spark.range(1, 8); // [1, 2, 3, 4, 5, 6, 7]
    const df2 = spark.range(3, 10); // [3, 4, 5, 6, 7, 8, 9]
    const result = df1.except(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // except should return [1, 2]
    expect(values).toEqual([1, 2]);
  });
});

test("exceptAll - returns rows in df1 but not in df2 with duplicates", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    // Create DataFrames with duplicates
    const df1 = await spark.sql("SELECT * FROM VALUES (1), (1), (2), (2), (2), (3) AS t(id)");
    const df2 = await spark.sql("SELECT * FROM VALUES (1), (2), (2) AS t(id)");
    const result = df1.exceptAll(df2);
    
    const rows = await result.collect();
    const values = rows.map(r => Number(r.get(0))).sort((a, b) => a - b);
    
    // exceptAll preserves duplicates
    // df1 has: 1(2x), 2(3x), 3(1x)
    // df2 has: 1(1x), 2(2x)
    // result: 1(1x), 2(1x), 3(1x)
    expect(values).toEqual([1, 2, 3]);
  });
});

test("complex set operations with multiple columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(async () => {
    const df1 = await spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)");
    const df2 = await spark.sql("SELECT * FROM VALUES (2, 'b'), (3, 'c'), (4, 'd') AS t(id, name)");
    
    const unionResult = df1.union(df2);
    const unionRows = await unionResult.collect();
    // union with distinct: (1,'a'), (2,'b'), (3,'c'), (4,'d')
    expect(unionRows.length).toBe(4);
    
    const intersectResult = df1.intersect(df2);
    const intersectRows = await intersectResult.collect();
    // intersect: (2,'b'), (3,'c')
    expect(intersectRows.length).toBe(2);
    
    const exceptResult = df1.except(df2);
    const exceptRows = await exceptResult.collect();
    // except: (1,'a')
    expect(exceptRows.length).toBe(1);
  });
});
