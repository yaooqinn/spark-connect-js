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

import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types/DataTypes";
import * as util from "../../../../../../src/org/apache/spark/sql/types/utils";

test("hasCharVarchar", () => {
  const charType = DataTypes.createCharType(1);
  const varcharType = DataTypes.createVarCharType(1);
  expect(util.hasCharVarchar(DataTypes.BinaryType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.BooleanType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.ByteType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.DateType)).toBe(false);
  expect(util.hasCharVarchar(charType)).toBe(true);
  expect(util.hasCharVarchar(varcharType)).toBe(true);
  expect(util.hasCharVarchar(DataTypes.DoubleType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.FloatType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.IntegerType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.LongType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.NullType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.ShortType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.StringType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.TimestampType)).toBe(false);
  expect(util.hasCharVarchar(DataTypes.createDecimalType(10, 0))).toBe(false);
  expect(util.hasCharVarchar(DataTypes.createArrayType(DataTypes.StringType))).toBe(false);
  expect(util.hasCharVarchar(DataTypes.createArrayType(charType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createArrayType(varcharType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))).toBe(false);
  expect(util.hasCharVarchar(DataTypes.createMapType(charType, DataTypes.StringType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createMapType(varcharType, DataTypes.StringType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createMapType(DataTypes.StringType, charType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createMapType(DataTypes.StringType, varcharType))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createStructType([]))).toBe(false);
  expect(util.hasCharVarchar(DataTypes.createStructType([DataTypes.createStructField("v", charType, false)]))).toBe(true);
  expect(util.hasCharVarchar(DataTypes.createStructType([DataTypes.createStructField("v", varcharType, false)]))).toBe(true);
});

