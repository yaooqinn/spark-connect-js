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
import { AnalyzePlanRequestBuilder } from '../../../../../src/org/apache/spark/sql/proto/AnalyzePlanRequestBuilder';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';
import { StorageLevel } from '../../../../../src/org/apache/spark/storage/StorageLevel';
import { sharedSpark, timeoutOrSatisfied } from '../../../../helpers';

test("to api", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1L as a").then(async df => {
    return df.schema().then(rSchema => {
      expect(rSchema.fields.length).toBe(1);
      expect(rSchema.fields[0].name).toBe("a");
      expect(rSchema.fields[0].dataType).toBe(DataTypes.LongType);
      const f1 = DataTypes.createStructField("a", DataTypes.IntegerType, false);
      const schema = DataTypes.createStructType([f1]);
      return df.to(schema).schema().then(schema2 => {
        expect(schema2.fields.length).toBe(1);
        expect(schema2.fields[0].name).toBe("a");
        expect(schema2.fields[0].dataType).toBe(DataTypes.IntegerType);
      });
    })
  }));
});

test("toDF api", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1L as a, 2Y as b").then(async df => {
    return df.schema().then(async rSchema => {
      expect(rSchema.fields.length).toBe(2);
      expect(rSchema.fields[0].name).toBe("a");
      expect(rSchema.fields[0].dataType).toBe(DataTypes.LongType);
      expect(rSchema.fields[1].name).toBe("b");
      expect(rSchema.fields[1].dataType).toBe(DataTypes.ByteType);
      await df.toDF('b', 'a').schema().then(async schema2 => {
        expect(schema2.fields.length).toBe(2);
        expect(schema2.fields[0].name).toBe("b");
        expect(schema2.fields[0].dataType).toBe(DataTypes.LongType);
        expect(schema2.fields[1].name).toBe("a");
        expect(schema2.fields[1].dataType).toBe(DataTypes.ByteType);
      });
      await df.toDF().schema().then(async schema3 => {
        expect(schema3.fields.length).toBe(2);
        expect(schema3.fields[0].name).toBe("a");
        expect(schema3.fields[0].dataType).toBe(DataTypes.LongType);
        expect(schema3.fields[1].name).toBe("b");
        expect(schema3.fields[1].dataType).toBe(DataTypes.ByteType);
      });
    })
  }));
});

test("explain api", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan)).then(explain => {
      expect(explain).toContain("== Physical Plan ==");
      expect(explain.includes("== Analyzed Logical Plan ==")).toBe(false);
      return df.explain().then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, 'simple')).then(explain => {
      expect(explain).toContain("== Physical Plan ==");
      expect(explain.includes("== Analyzed Logical Plan ==")).toBe(false);
      return df.explain("simple").then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, "extended")).then(explain => {
      expect(explain).toContain("== Physical Plan ==");
      expect(explain).toContain("== Analyzed Logical Plan ==");
      expect(explain).toContain("== Optimized Logical Plan ==");
      expect(explain).toContain("== Parsed Logical Plan ==");
      return df.explain("extended").then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, "codegen")).then(explain => {
      expect(explain).toContain("WholeStageCodegen");
      return df.explain("codegen").then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, "cost")).then(explain => {
      expect(explain).toContain("== Optimized Logical Plan ==");
      expect(explain).toContain("Statistics(sizeInBytes=");
      return df.explain("cost").then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, "formatted")).then(explain => {
      expect(explain).toContain("Arguments");
      expect(explain).toContain("== Physical Plan ==");
      return df.explain("formatted").then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, false)).then(explain => {
      expect(explain).toContain("== Physical Plan ==");
      expect(explain.includes("== Analyzed Logical Plan ==")).toBe(false);
      return df.explain(false).then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    return df.explain0(b => b.withExplain(df.plan.plan, true)).then(explain => {
      expect(explain).toContain("== Physical Plan ==");
      expect(explain).toContain("== Analyzed Logical Plan ==");
      return df.explain(true).then(() => {});
    });
  }));

  await timeoutOrSatisfied(spark.sql("SHOW TABLES").then(df => {
    expect(() => new AnalyzePlanRequestBuilder().withExplain(df.plan.plan, 'invalid')).toThrow('invalid');
  }));
});

test("printSchema api", async () => {
  const spark = await sharedSpark;
  [-1, 0, 1].forEach(async level => {
    await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
      return df.printSchema0(b => b.withTreeString(df.plan.plan, level)).then(schema => {
        expect(schema).toContain("a: int");
      });
    }));

    await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
      return df.printSchema(level).then(() => {});
    }));
  });
});

test("dtypes api", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.dtypes().then(dtypes => {
      expect(dtypes.length).toBe(1);
      expect(dtypes[0][0]).toBe("a");
      expect(dtypes[0][1]).toBe("IntegerType");
    });
  }));
});

test("columns api", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.columns().then(columns => {
      expect(columns.length).toBe(1);
      expect(columns[0]).toBe("a");
    });
  }));
});

test("isLocal", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.isLocal().then(isLocal => {
      expect(isLocal).toBe(false);
    });
  }));

  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    const schema = await df.schema();
    const rows = await df.collect();
    spark.createDataFrame(rows, schema).isLocal().then(isLocal => {
      expect(isLocal).toBe(true);
    })
  }));
});

test("isStreaming", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    return df.isStreaming().then(isStreaming => {
      expect(isStreaming).toBe(false);
    });
  }));
});

test("inputFiles", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    return df.inputFiles().then(files => {
      expect(files.length).toBe(0);
    });
  }));
});

test("same semantics", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    return df.sameSemantics(df).then(sameSemantics => {
      expect(sameSemantics).toBe(true);
    });
  }));

  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(df => {
    return df.sameSemantics(spark.emptyDataFrame).then(sameSemantics => {
      expect(sameSemantics).toBe(false);
    });
  }));
});

test("semanticHash", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    return df.semanticHash().then(hash => {
      expect(hash).toBeGreaterThan(0);
    });
  }));
});

test("persist", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    await df.persist().then(async persisted => {
      return persisted.storageLevel().then(level => {
        expect(level.equals(StorageLevel.MEMORY_AND_DISK)).toBe(true);
      })
    });
    [
      StorageLevel.DISK_ONLY,
      StorageLevel.DISK_ONLY_2,
      StorageLevel.MEMORY_ONLY,
      StorageLevel.MEMORY_ONLY_2,
      StorageLevel.MEMORY_ONLY_SER,
      StorageLevel.MEMORY_ONLY_SER_2,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK_2,
      StorageLevel.MEMORY_AND_DISK_SER,
      StorageLevel.MEMORY_AND_DISK_SER_2,
      StorageLevel.OFF_HEAP,
      StorageLevel.NONE
    ].forEach(async (level, index) => {
      await df.persist(level).then(async persisted => {
        return persisted.storageLevel().then(level => {
          expect(level.equals(level)).toBe(true);
          persisted.unpersist(index % 2 === 0).then(async unpersisted => {
            return unpersisted.storageLevel().then(level => {
              expect(level.equals(StorageLevel.NONE)).toBe(true);
            });
          })
        })
      });
    })
  }));
});

test("selectExpr", async () => {
  const spark = await sharedSpark;
  await timeoutOrSatisfied(spark.sql("SELECT 1 + 1 as a").then(async df => {
    return df.selectExpr("a").schema().then(schema => {
      expect(schema.fields.length).toBe(1);
      expect(schema.fields[0].name).toBe("a");
      expect(schema.fields[0].dataType).toBe(DataTypes.IntegerType);
    });
  }));
});

test("select", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT 1 + 1 as a, 'spark' as b, named_struct('c', 3, 'd', 4) as c");
  const isEmpty = await df.select().isEmpty();
  expect(isEmpty).toBe(true);
  const schema = await df.select().schema();
  expect(schema.fields.length).toBe(0);
  const row = await df.select("a").head();
  expect(row[0]).toBe(2);
  const row1 = await df.select(new Column("b")).head()
  expect(row1[0]).toBe("spark");
  const row2 = await df.select("a", "b").head();
  expect(row2[0]).toBe(2);
  expect(row2[1]).toBe("spark");
  const row3 = await df.select(new Column("a"), new Column("b")).head();
  expect(row3[0]).toBe(2);
  expect(row3[1]).toBe("spark");
  const row4 = await df.select("*").head();
  expect(row4[0]).toBe(2);
  expect(row4[1]).toBe("spark");
  expect(row4[2].c).toBe(3);
  expect(row4[2].d).toBe(4);
  const row5 = await df.select("c.*").head();
  expect(row5[0]).toBe(3);
  expect(row5[1]).toBe(4);
});

test("col", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT 1 + 1 as a, 'spark' as b, named_struct('c', 3, 'd', 4) as c");
  const row = await df.select(df.col("a")).head();
  expect(row[0]).toBe(2);
  const row1 = await df.select(df.col("b")).head()
  expect(row1[0]).toBe("spark");
  const row2 = await df.select(df.col("a"), df.col("b")).head();
  expect(row2[0]).toBe(2);
  expect(row2[1]).toBe("spark");
  const row3 = await df.select(df.col("a"), df.col("b")).head();
  expect(row3[0]).toBe(2);
  expect(row3[1]).toBe("spark");
  const row4 = await df.select(df.col("*")).head();
  expect(row4[0]).toBe(2);
  expect(row4[1]).toBe("spark");
  expect(row4[2].c).toBe(3);
  expect(row4[2].d).toBe(4);
  const row5 = await df.select(df.col("c.d")).head();
  expect(row5[0]).toBe(4);
});

test("colRegex", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT 1 + 1 as aba, 'spark' as abb, named_struct('c', 3, 'd', 4) as abc");
  const row = await df.select(df.colRegex("`a.*`")).head();
  expect(row[0]).toBe(2);
  expect(row[1]).toBe("spark");
  expect(row[2].c).toBe(3);
  expect(row[2].d).toBe(4);
});

test("hint", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT * FROM range(1, 100, 1)");
  await df.hint("rebalance", 2).schema().then(schema => {
    expect(schema.fields.length).toBe(1);
    expect(schema.fields[0].name).toBe("id");
    expect(schema.fields[0].dataType).toBe(DataTypes.LongType);
  });
});

