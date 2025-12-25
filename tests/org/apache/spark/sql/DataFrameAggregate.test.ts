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

import { StructType } from '../../../../../src/org/apache/spark/sql/types/StructType';
import { sharedSpark } from '../../../../helpers';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types';
import { Row } from '../../../../../src/org/apache/spark/sql/Row';
import { col } from '../../../../../src/org/apache/spark/sql/functions';


const testSchema = new StructType()
  .add("name", DataTypes.StringType)
  .add("game", DataTypes.IntegerType)
  .add("goals", DataTypes.IntegerType);

const testRows = [
  new Row(testSchema, { name: "Messi", game: 1, goals: 1 }),
  new Row(testSchema, { name: "Messi", game: 2, goals: 2 }),
  new Row(testSchema, { name: "Messi", game: 3, goals: 0 }),
  new Row(testSchema, { name: "Messi", game: 4, goals: 1 }),
  new Row(testSchema, { name: "Ronaldo", game: 1, goals: 1 }),
  new Row(testSchema, { name: "Ronaldo", game: 2, goals: 2 }),
  new Row(testSchema, { name: "Ronaldo", game: 3, goals: 1 }),
  new Row(testSchema, { name: "Ronaldo", game: 4, goals: 1 }),
];

test("count", async () => {
  const spark = await sharedSpark;
  const df = spark.range(1, 1000);
  const count = await df.count();
  expect(count).toBe(999n);
});

test("rollup", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const rollup = df.rollup(col("name"), col("game"));
  await rollup.count().where("name is null").head().then((row) => {
    expect(row[2]).toBe(8n);
  });
  await rollup.sum("goals").where("name is null").head().then((row) => {
    expect(row[2]).toBe(9n);
  });
});

test("cube", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const cube = df.cube("name", "game");
  await cube.count().where("name is null and game is null").head().then((row) => {
    expect(row[2]).toBe(8n);
  });
  await cube.sum("goals").where("name is null and game is null").head().then((row) => {
    expect(row[2]).toBe(9n);
  });
});

test("groupingSets", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const groupingSets = df.groupingSets([[col("game")]]);
  await groupingSets.sum("goals").head().then((row) => {
    expect(row[0]).toBe(2n);
  });
});

test("unpivot", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const unpivot = df.unpivot([col("name")], [df.col("game"), df.col("goals")], "attribute", "value");
  await unpivot.head().then((row) => {
    expect(row[0]).toBe("Messi");
    expect(row[1]).toBe("game");
    expect(row[2]).toBe(1);
  });
});

test("tanspose", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const transpose = df.groupBy("name").sum("goals").transpose();
  await transpose.show()
  await transpose.head().then((row) => {
    expect(row[0]).toBe("sum(goals)");
    expect(row[1]).toBe(4n);
    expect(row[2]).toBe(5n);
  });
});

test("advanced aggregate helpers", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);
  const grouped = df.groupBy("name");

  await grouped.avg("goals").where("name = 'Ronaldo'").head().then((row) => {
    expect(Number(row[1])).toBeCloseTo(1.25, 3);
  });

  await grouped.min("goals").where("name = 'Messi'").head().then((row) => {
    expect(Number(row[1])).toBe(0);
  });

  await grouped.stddevSamp("goals").where("name = 'Messi'").head().then((row) => {
    expect(Number(row[1])).toBeCloseTo(0.81649, 4);
  });

  await grouped.varPop("goals").where("name = 'Ronaldo'").head().then((row) => {
    expect(Number(row[1])).toBeCloseTo(0.1875, 4);
  });
});

test("agg with map and pivot", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  await df.groupBy("name").agg({ goals: "max", game: "min" }).where("name = 'Messi'").head().then((row) => {
    expect(Number(row[1])).toBe(2);
    expect(Number(row[2])).toBe(1);
  });

  const pivoted = df.groupBy("game").pivot("name", ["Messi", "Ronaldo"]).sum("goals");
  await pivoted.where("game = 1").head().then((row) => {
    expect(Number(row[1])).toBe(1);
    expect(Number(row[2])).toBe(1);
  });
});
