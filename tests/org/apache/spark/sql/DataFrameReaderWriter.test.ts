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

import * as c from "../../../../../src/gen/spark/connect/commands_pb";
import { logger } from "../../../../../src/org/apache/spark/logger";
import { SaveMode } from "../../../../../src/org/apache/spark/sql/SaveMode";
import { AnalysisException } from "../../../../../src/org/apache/spark/sql/errors";
import { DataTypes } from "../../../../../src/org/apache/spark/sql/types/DataTypes";
import { sharedSpark, withTable, withTempDir } from "../../../../helpers";

/*eslint @typescript-eslint/no-explicit-any : "off"*/
test("DataFrameWriter mode", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).mode_).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect((writer.mode("append") as any).mode_).toBe(c.WriteOperation_SaveMode.APPEND);
  expect((writer.mode("errorifexists") as any).mode_).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect((writer.mode("error") as any).mode_).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect((writer.mode("default") as any).mode_).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect((writer.mode("ignore") as any).mode_).toBe(c.WriteOperation_SaveMode.IGNORE);
  expect((writer.mode("overwrite") as any).mode_).toBe(c.WriteOperation_SaveMode.OVERWRITE);

  expect((writer.mode(SaveMode.Append) as any).mode_).toBe(c.WriteOperation_SaveMode.APPEND);
  expect((writer.mode(SaveMode.ErrorIfExists) as any).mode_).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect((writer.mode(SaveMode.Ignore) as any).mode_).toBe(c.WriteOperation_SaveMode.IGNORE);
  expect((writer.mode(SaveMode.Overwrite) as any).mode_).toBe(c.WriteOperation_SaveMode.OVERWRITE);

  expect(() => (writer.mode("invalid") as any).mode_).toThrow("INVALID_SAVE_MODE");
  expect(() => (writer.mode("invalid") as any).mode_).toThrow("invalid");
});

test("DataFrameWriter format", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).source_).toBe(undefined);
  [ "parquet", "json", "csv", "jdbc", "unknown" ].forEach(format => {
    expect((writer.format(format) as any).source_).toBe(format);
  });
});


test("DataFrameWriter option", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).extraOptions_.size).toBe(0);
  expect((writer.option("key", "value") as any).extraOptions_.size).toBe(1);
  expect((writer as any).extraOptions_.get("key")).toBe("value");
  expect((writer.option("key2", "value2") as any).extraOptions_.size).toBe(2);
  expect((writer as any).extraOptions_.get("key2")).toBe("value2");
  expect((writer.option("key3", 1) as any).extraOptions_.size).toBe(3);
  expect((writer as any).extraOptions_.get("key3")).toBe("1");
  expect((writer.option("key4", 1.1) as any).extraOptions_.size).toBe(4);
  expect((writer as any).extraOptions_.get("key4")).toBe("1.1");
  expect((writer.option("key5", true) as any).extraOptions_.size).toBe(5);
  expect((writer as any).extraOptions_.get("key5")).toBe("true");
  expect((writer.options({ key6: "value6", key7: "2" }) as any).extraOptions_.size).toBe(7);
  expect((writer as any).extraOptions_.get("key6")).toBe("value6");
  expect((writer as any).extraOptions_.get("key7")).toBe("2");
});

test("DataFrameWriter partitionBy", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).partitioningColumns_.length).toBe(0);
  expect((writer.partitionBy("col1") as any).partitioningColumns_).toStrictEqual(["col1"]);
  expect((writer.partitionBy("col1", "col2") as any).partitioningColumns_).toStrictEqual(["col1", "col2"]);
  expect((writer.partitionBy("col1", "col2", "col3") as any).partitioningColumns_).toStrictEqual(["col1", "col2", "col3"]);
});

test("DataFrameWriter bucketBy", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).bucketColumnNames_.length).toBe(0);
  expect((writer as any).numBuckets_).toBe(undefined);
  expect((writer.bucketBy(10, "col1") as any).bucketColumnNames_).toStrictEqual(["col1"]);
  expect((writer as any).numBuckets_).toBe(10);
  expect((writer.bucketBy(10, "col1", "col2") as any).bucketColumnNames_).toStrictEqual(["col1", "col2"]);
  expect((writer as any).numBuckets_).toBe(10);
  expect((writer.bucketBy(10, "col1", "col2", "col3") as any).bucketColumnNames_).toStrictEqual(["col1", "col2", "col3"]);
  expect((writer as any).numBuckets_).toBe(10);
});

test("DataFrameWriter sortBy", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  const writer = df.write
  expect((writer as any).sortColumnNames_.length).toBe(0);
  expect((writer.sortBy("col1").bucketBy(2, "col1") as any).sortColumnNames_).toStrictEqual(["col1"]);
  expect((writer.sortBy("col1", "col2").bucketBy(2, "col1") as any).sortColumnNames_).toStrictEqual(["col1", "col2"]);
  expect((writer.sortBy("col1", "col2", "col3").bucketBy(2, "col1") as any).sortColumnNames_).toStrictEqual(["col1", "col2", "col3"]);
});

test("DataFrameWriter validatePartitioning", async () => {
  const spark = await sharedSpark;
  const df = spark.emptyDataFrame
  expect(() => df.write.clusterBy("col1").partitionBy("col2")).toThrow("[SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED]");
  expect(() => df.write.partitionBy("col1").clusterBy("col2")).toThrow("[SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED]");
  
  // sort by is validate in later stages
  [ undefined, "dummy" ].forEach(path => {
    df.write.sortBy("col1").save(path).catch(e => {
      expect((e as AnalysisException).condition).toBe("SORT_BY_WITHOUT_BUCKETING");
    });
  });
  expect(() => df.write.sortBy("col2").clusterBy("col1")).toThrow("[SORT_BY_WITHOUT_BUCKETING]");
});

test("DataFrameWriter save", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT id, 'Alice' as name from range(1, 10000, 1, 200) DISTRIBUTE BY id % 100");
  return withTable(spark, "people", async () => {
    await df.write.format("parquet").saveAsTable("people");
    expect.assertions(12);
    spark.table("people").schema().then(schema => {
      expect(schema.fields.length).toBe(2)
      expect(schema.fields[0].name).toBe("id")
      expect(schema.fields[0].dataType).toBe(DataTypes.LongType)
      expect(schema.fields[1].name).toBe("name")
      expect(schema.fields[1].dataType).toBe(DataTypes.StringType)
    });
    await df.write.mode("overwrite").parquet("people");
    return spark.table("people").schema().then(schema => {
      expect(schema.fields.length).toBe(2)
      expect(schema.fields[0].name).toBe("id")
      expect(schema.fields[0].dataType).toBe(DataTypes.LongType)
      expect(schema.fields[1].name).toBe("name")
      expect(schema.fields[1].dataType).toBe(DataTypes.StringType)
    });
  });
});

test("DataFrameWriter insertInto", async () => {
  return sharedSpark.then(spark => {
    return withTable(spark, "people", async () => {
      return spark.sql("CREATE TABLE people (id LONG, name STRING) USING parquet").then(() => {
        return spark.sql("SELECT id, 'Alice' as name from range(1, 10000, 1, 200)").then(df => {
          return df.write.mode("append").insertInto("people").then(() => {
            expect.assertions(5);
            return spark.table("people").schema().then(schema => {
              expect(schema.fields.length).toBe(2)
              expect(schema.fields[0].name).toBe("id")
              expect(schema.fields[0].dataType).toBe(DataTypes.LongType)
              expect(schema.fields[1].name).toBe("name")
              expect(schema.fields[1].dataType).toBe(DataTypes.StringType)
            });
          });
        });
      });
    });
  });
});

test("DataFrameWriter jdbc", async () => {
  expect.assertions(3);
  return sharedSpark.then(async spark => {
    const df = spark.emptyDataFrame
    expect(() => df.write.partitionBy("col1").jdbc("jdbc:postgresql:dbserver", "people", {})).toThrow("does not support partitioning.");
    expect(() => df.write.bucketBy(2, "col1").jdbc("jdbc:postgresql:dbserver", "people", {})).toThrow("does not support bucketBy right now.");
    expect(() => df.write.clusterBy("col1").jdbc("jdbc:postgresql:dbserver", "people", {})).toThrow("does not support clustering.");
    expect.assertions(3 + 1);
    return df.write.mode("overwrite").jdbc("jdbc:postgresql:dbserver", "people", {}).catch(e => {
      expect((e as Error).message).toMatch("No suitable driver");
    });
  })
});

test("DataFrameWriter parquet", async () => {
  return sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select 1 as a, 2 as b").then(async df => {
        expect.assertions(6);
        return df.write
          .mode("overwrite")
          .parquet(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            return spark.read.parquet(dir).schema().then(schema => {
              expect(schema.fields.length).toBe(2)
              expect(schema.fields[0].name).toBe("a")
              expect(schema.fields[0].dataType).toBe(DataTypes.IntegerType)
              expect(schema.fields[1].name).toBe("b")
              expect(schema.fields[1].dataType).toBe(DataTypes.IntegerType)
            });
        });
      });
    });
  });
});

test("DataFrameWriter orc", async () => {
  sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select 1 as a, 2 as b").then(async df => {
        return df.write
          .mode("overwrite")
          .orc(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            logger.info("orc response", resps);
            return spark.read.orc(dir).schema().then(schema => {
              expect(schema.fields.length).toBe(2)
              expect(schema.fields[0].name).toBe("a")
              expect(schema.fields[0].dataType).toBe(DataTypes.IntegerType)
              expect(schema.fields[1].name).toBe("b")
              expect(schema.fields[1].dataType).toBe(DataTypes.IntegerType)
            });
        });
      });
    });
  });
});

test("DataFrameWriter json", async () => {
  return sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select 1 as a, 2 as b").then(async df => {
        return df.write
          .mode("overwrite")
          .json(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            return spark.read.json(dir).schema().then(schema => {
              expect(schema.fields.length).toBe(2)
              expect(schema.fields[0].name).toBe("a")
              expect(schema.fields[0].dataType).toBe(DataTypes.LongType)
              expect(schema.fields[1].name).toBe("b")
              expect(schema.fields[1].dataType).toBe(DataTypes.LongType)
            });
        });
      });
    });
  });
});

test("DataFrameWriter csv", async () => {
  return sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select 1 as a, 2 as b").then(async df => {
        return df.write
          .mode("overwrite")
          .option("header", true)
          .csv(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            return spark.read.option("header", true).csv(dir).schema().then(schema => {
              expect(schema.fields.length).toBe(2)
              expect(schema.fields[0].name).toBe("a")
              expect(schema.fields[0].dataType).toBe(DataTypes.StringType)
              expect(schema.fields[1].name).toBe("b")
              expect(schema.fields[1].dataType).toBe(DataTypes.StringType)
            });
        });
      });
    });
  });
});

test("DataFrameWriter xml", async () => {
  return sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select 1 as a, 2 as b").then(async df => {
        return df.write
          .mode("overwrite")
          .option('rootTag', 'rows')
          .option('rowTag', 'row')
          .xml(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            return spark.read
              .option('rootTag', 'rows')
              .option('rowTag', 'row')
              .xml(dir).schema().then(schema => {
                expect(schema.fields.length).toBe(2)
                expect(schema.fields[0].name).toBe("a")
                expect(schema.fields[0].dataType).toBe(DataTypes.LongType)
                expect(schema.fields[1].name).toBe("b")
                expect(schema.fields[1].dataType).toBe(DataTypes.LongType)
              });
        });
      });
    });
  })
});


test("DataFrameWriter text", async () => {
  return sharedSpark.then(async spark => {
    return withTempDir(async dir => {
      return spark.sql("select '1' as a").then(async df => {
        return df.write
          .mode("overwrite")
          .text(dir)
          .then(async resps => {
            expect(resps.length).toBe(0);
            return spark.read.text(dir).schema().then(schema => {
              expect(schema.fields.length).toBe(1)
              expect(schema.fields[0].name).toBe("value")
              expect(schema.fields[0].dataType).toBe(DataTypes.StringType)
            });
        });
      });
    });
  });
});