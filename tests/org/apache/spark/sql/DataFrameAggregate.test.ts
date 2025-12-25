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

test("pivot with explicit values", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  // Pivot with explicit values
  const pivoted = df.groupBy("name").pivot("game", [1, 2, 3, 4]).sum("goals");
  const rows = await pivoted.collect();

  // Should have one row per name
  expect(rows.length).toBe(2);

  // Check the structure - should have name and 4 game columns
  const columns = await pivoted.columns();
  expect(columns.length).toBe(5); // name + 4 game columns
  expect(columns[0]).toBe("name");
});

test("pivot without explicit values", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  // Pivot without values - Spark will compute distinct values
  const pivoted = df.groupBy("name").pivot("game").sum("goals");
  const rows = await pivoted.collect();

  // Should have one row per name
  expect(rows.length).toBe(2);

  // Check that we got columns for each game
  const columns = await pivoted.columns();
  expect(columns.length).toBe(5); // name + 4 game columns
});

test("pivot with Column object", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  // Pivot using Column object instead of string
  const pivoted = df.groupBy("name").pivot(col("game"), [1, 2]).sum("goals");
  const rows = await pivoted.collect();

  expect(rows.length).toBe(2);

  const columns = await pivoted.columns();
  expect(columns.length).toBe(3); // name + 2 game columns
});

test("pivot error on non-groupBy", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  // Pivot should fail on rollup
  const rollup = df.rollup("name");
  expect(() => rollup.pivot("game")).toThrow("pivot can only be applied after groupBy");

  // Pivot should fail on cube
  const cube = df.cube("name");
  expect(() => cube.pivot("game")).toThrow("pivot can only be applied after groupBy");
});

test("pivot error on double pivot", async () => {
  const spark = await sharedSpark;
  const df = spark.createDataFrame(testRows, testSchema);

  // Pivot should fail when applied twice
  const grouped = df.groupBy("name").pivot("game", [1, 2]);
  expect(() => grouped.pivot("game")).toThrow("pivot cannot be applied on a pivot");
});

