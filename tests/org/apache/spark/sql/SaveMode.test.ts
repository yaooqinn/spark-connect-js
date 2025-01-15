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


import { SaveMode } from "../../../../../src/org/apache/spark/sql/SaveMode";
import * as c from "../../../../../src/gen/spark/connect/commands_pb";

test("SaveMode", () => {
  SaveMode.Append;
  expect(SaveMode.Append).toBe(c.WriteOperation_SaveMode.APPEND);
  expect(SaveMode.ErrorIfExists).toBe(c.WriteOperation_SaveMode.ERROR_IF_EXISTS);
  expect(SaveMode.Ignore).toBe(c.WriteOperation_SaveMode.IGNORE);
  expect(SaveMode.Overwrite).toBe(c.WriteOperation_SaveMode.OVERWRITE);
  expect(c.WriteOperation_SaveMode["APPEND" as keyof typeof c.WriteOperation_SaveMode]).toBe(SaveMode.Append);
  expect(c.WriteOperation_SaveMode["ERROR_IF_EXISTS" as keyof typeof c.WriteOperation_SaveMode]).toBe(SaveMode.ErrorIfExists);
  expect(c.WriteOperation_SaveMode["IGNORE" as keyof typeof c.WriteOperation_SaveMode]).toBe(SaveMode.Ignore);
  expect(c.WriteOperation_SaveMode["OVERWRITE" as keyof typeof c.WriteOperation_SaveMode]).toBe(SaveMode.Overwrite);
});