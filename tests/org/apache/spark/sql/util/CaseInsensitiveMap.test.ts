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

import { CaseInsensitiveMap } from '../../../../../../src/org/apache/spark/sql/util/CaseInsensitiveMap';


test("CaseInsensitiveMap", () => {
  const map = new CaseInsensitiveMap<string>();

  map.set("key", "value");
  expect(map.get("key")).toBe("value");
  expect(map.get("KEY")).toBe("value");
  expect(map.get("kEy")).toBe("value");
  expect(map.get("kEY")).toBe("value");
  expect(map.get("keY")).toBe("value");
  expect(map.get("KeY")).toBe("value");
  expect(map.get("kEy")).toBe("value");
  map.set("KEY", "VALUE");
  expect(map.get("key")).toBe("VALUE");
  expect(map.has("key")).toBe(true);
  expect(map.has("KEY")).toBe(true);
  expect(map.has("kEy")).toBe(true);

  for (const [key, value] of map.entries()) {
    expect(key).toBe("key");
    expect(value).toBe("VALUE");
  }

  for (const key of map.keys()) {
    expect(key).toBe("key");
  }

  for (const value of map.values()) {
    expect(value).toBe("VALUE");
  }

  map.forEach((value, key) => {
    expect(key).toBe("key");
    expect(value).toBe("VALUE");
  });

  expect(map.size).toBe(1);
  map.delete("key");
  expect(map.has("key")).toBe(false);
  expect(map.has("KEY")).toBe(false);

});