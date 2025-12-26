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

import { Column } from '../../../../../src/org/apache/spark/sql/Column';
import { sharedSpark } from '../../../../helpers';

test("join - inner join with single column name", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const df2 = await spark.sql("SELECT 1 as id, 25 as age");
  const rows = await df1.join(df2, "id").collect();
  expect(rows.length).toBe(1);
  expect(rows[0].getInt(0)).toBe(1);
});

test("join - inner join with column expression", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const df2 = await spark.sql("SELECT 1 as id2, 25 as age");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id2").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr).collect();
  expect(rows.length).toBe(1);
});

test("join - left outer join", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const df2 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr, "left").collect();
  expect(rows.length).toBe(2);
});

test("join - right outer join", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const df2 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr, "right").collect();
  expect(rows.length).toBe(2);
});

test("join - full outer join", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const df2 = await spark.sql("SELECT 2 as id UNION ALL SELECT 3 as id");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr, "full").collect();
  expect(rows.length).toBe(3);
});

test("join - left semi join", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const df2 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr, "semi").collect();
  expect(rows.length).toBe(1);
  // Semi join only returns columns from left side
  expect(rows[0].length()).toBe(1);
});

test("join - left anti join", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const df2 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("==", [
      df1.col("id").expr,
      df2.col("id").expr
    ])
  );
  const rows = await df1.join(df2, joinExpr, "anti").collect();
  expect(rows.length).toBe(1);
  expect(rows[0].getInt(0)).toBe(2);
});

test("join - using multiple columns", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 'A' as type");
  const df2 = await spark.sql("SELECT 1 as id, 'A' as type, 100 as value");
  const rows = await df1.join(df2, ["id", "type"]).collect();
  expect(rows.length).toBe(1);
});

test("join - using columns with join type", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id");
  const df2 = await spark.sql("SELECT 1 as id, 100 as value");
  const rows = await df1.join(df2, ["id"], "left").collect();
  expect(rows.length).toBe(2);
});

test("crossJoin - cartesian product", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as a UNION ALL SELECT 2 as a");
  const df2 = await spark.sql("SELECT 'x' as b UNION ALL SELECT 'y' as b");
  const rows = await df1.crossJoin(df2).collect();
  expect(rows.length).toBe(4);
});

test("join - with broadcast hint", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 'Alice' as name");
  const df2 = await spark.sql("SELECT 1 as id, 25 as age");
  // Apply broadcast hint to the smaller DataFrame
  const broadcastDf2 = df2.hint("broadcast");
  const rows = await df1.join(broadcastDf2, "id").collect();
  expect(rows.length).toBe(1);
});

test("join - complex join condition", async () => {
  const spark = await sharedSpark;
  const df1 = await spark.sql("SELECT 1 as id, 10 as value");
  const df2 = await spark.sql("SELECT 1 as id2, 5 as threshold");
  const joinExpr = new Column((b) => 
    b.withUnresolvedFunction("and", [
      new Column((eb) => 
        eb.withUnresolvedFunction("==", [
          df1.col("id").expr,
          df2.col("id2").expr
        ])
      ).expr,
      new Column((eb) => 
        eb.withUnresolvedFunction(">", [
          df1.col("value").expr,
          df2.col("threshold").expr
        ])
      ).expr
    ])
  );
  const rows = await df1.join(df2, joinExpr).collect();
  expect(rows.length).toBe(1);
});

test("join - alternative join type names", async () => {
  const spark = await sharedSpark;
  
  // Test various aliases for join types
  const testJoinType = async (joinType: string) => {
    const df1 = await spark.sql("SELECT 1 as id");
    const df2 = await spark.sql("SELECT 1 as id");
    const joinExpr = new Column((b) => 
      b.withUnresolvedFunction("==", [
        df1.col("id").expr,
        df2.col("id").expr
      ])
    );
    const rows = await df1.join(df2, joinExpr, joinType).collect();
    expect(rows.length).toBeGreaterThanOrEqual(0);
  };
  
  await testJoinType("inner");
  await testJoinType("left");
  await testJoinType("leftouter");
  await testJoinType("left_outer");
  await testJoinType("right");
  await testJoinType("rightouter");
  await testJoinType("right_outer");
  await testJoinType("outer");
  await testJoinType("full");
  await testJoinType("fullouter");
  await testJoinType("full_outer");
  await testJoinType("semi");
  await testJoinType("leftsemi");
  await testJoinType("left_semi");
  await testJoinType("anti");
  await testJoinType("leftanti");
  await testJoinType("left_anti");
  await testJoinType("cross");
});
