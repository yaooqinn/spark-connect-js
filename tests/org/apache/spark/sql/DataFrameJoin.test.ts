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
import { sharedSpark, timeoutOrSatisfied } from '../../../../helpers';

test("join - inner join with single column name", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 'Alice' as name").then(async df1 => {
      return spark.sql("SELECT 1 as id, 25 as age").then(async df2 => {
        return df1.join(df2, "id").collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getInt(0)).toBe(1);
        });
      });
    })
  );
});

test("join - inner join with column expression", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 'Alice' as name").then(async df1 => {
      return spark.sql("SELECT 1 as id2, 25 as age").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id2").expr
          ])
        );
        return df1.join(df2, joinExpr).collect().then(rows => {
          expect(rows.length).toBe(1);
        });
      });
    })
  );
});

test("join - left outer join", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df1 => {
      return spark.sql("SELECT 1 as id, 'Alice' as name").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, "left").collect().then(rows => {
          expect(rows.length).toBe(2);
        });
      });
    })
  );
});

test("join - right outer join", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 'Alice' as name").then(async df1 => {
      return spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, "right").collect().then(rows => {
          expect(rows.length).toBe(2);
        });
      });
    })
  );
});

test("join - full outer join", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df1 => {
      return spark.sql("SELECT 2 as id UNION ALL SELECT 3 as id").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, "full").collect().then(rows => {
          expect(rows.length).toBe(3);
        });
      });
    })
  );
});

test("join - left semi join", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df1 => {
      return spark.sql("SELECT 1 as id, 'Alice' as name").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, "semi").collect().then(rows => {
          expect(rows.length).toBe(1);
          // Semi join only returns columns from left side
          expect(rows[0].length()).toBe(1);
        });
      });
    })
  );
});

test("join - left anti join", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df1 => {
      return spark.sql("SELECT 1 as id, 'Alice' as name").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, "anti").collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getInt(0)).toBe(2);
        });
      });
    })
  );
});

test("join - using multiple columns", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 'A' as type").then(async df1 => {
      return spark.sql("SELECT 1 as id, 'A' as type, 100 as value").then(async df2 => {
        return df1.join(df2, ["id", "type"]).collect().then(rows => {
          expect(rows.length).toBe(1);
        });
      });
    })
  );
});

test("join - using columns with join type", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id UNION ALL SELECT 2 as id").then(async df1 => {
      return spark.sql("SELECT 1 as id, 100 as value").then(async df2 => {
        return df1.join(df2, ["id"], "left").collect().then(rows => {
          expect(rows.length).toBe(2);
        });
      });
    })
  );
});

test("crossJoin - cartesian product", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as a UNION ALL SELECT 2 as a").then(async df1 => {
      return spark.sql("SELECT 'x' as b UNION ALL SELECT 'y' as b").then(async df2 => {
        return df1.crossJoin(df2).collect().then(rows => {
          expect(rows.length).toBe(4);
        });
      });
    })
  );
});

test("join - with broadcast hint", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 'Alice' as name").then(async df1 => {
      return spark.sql("SELECT 1 as id, 25 as age").then(async df2 => {
        // Apply broadcast hint to the smaller DataFrame
        const broadcastDf2 = df2.hint("broadcast");
        return df1.join(broadcastDf2, "id").collect().then(rows => {
          expect(rows.length).toBe(1);
        });
      });
    })
  );
});

test("join - complex join condition", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(
    spark.sql("SELECT 1 as id, 10 as value").then(async df1 => {
      return spark.sql("SELECT 1 as id2, 5 as threshold").then(async df2 => {
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
        return df1.join(df2, joinExpr).collect().then(rows => {
          expect(rows.length).toBe(1);
        });
      });
    })
  );
});

test("join - alternative join type names", async () => {
  const spark = await sharedSpark;
  
  // Test various aliases for join types
  const testJoinType = async (joinType: string) => {
    return spark.sql("SELECT 1 as id").then(async df1 => {
      return spark.sql("SELECT 1 as id").then(async df2 => {
        const joinExpr = new Column((b) => 
          b.withUnresolvedFunction("==", [
            df1.col("id").expr,
            df2.col("id").expr
          ])
        );
        return df1.join(df2, joinExpr, joinType).collect().then(rows => {
          expect(rows.length).toBeGreaterThanOrEqual(0);
        });
      });
    });
  };
  
  await timeoutOrSatisfied(testJoinType("inner"));
  await timeoutOrSatisfied(testJoinType("left"));
  await timeoutOrSatisfied(testJoinType("leftouter"));
  await timeoutOrSatisfied(testJoinType("left_outer"));
  await timeoutOrSatisfied(testJoinType("right"));
  await timeoutOrSatisfied(testJoinType("rightouter"));
  await timeoutOrSatisfied(testJoinType("right_outer"));
  await timeoutOrSatisfied(testJoinType("outer"));
  await timeoutOrSatisfied(testJoinType("full"));
  await timeoutOrSatisfied(testJoinType("fullouter"));
  await timeoutOrSatisfied(testJoinType("full_outer"));
  await timeoutOrSatisfied(testJoinType("semi"));
  await timeoutOrSatisfied(testJoinType("leftsemi"));
  await timeoutOrSatisfied(testJoinType("left_semi"));
  await timeoutOrSatisfied(testJoinType("anti"));
  await timeoutOrSatisfied(testJoinType("leftanti"));
  await timeoutOrSatisfied(testJoinType("left_anti"));
  await timeoutOrSatisfied(testJoinType("cross"));
});
