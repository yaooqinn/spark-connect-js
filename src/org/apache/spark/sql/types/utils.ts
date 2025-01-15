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

import { CharType } from "./CharType";
import { DataType } from "./data_types";
import { VarcharType } from "./VarcharType";

export function hasCharVarchar(dataType: DataType): boolean {
  return dataType.existsRecursively(dt => dt instanceof VarcharType || dt instanceof CharType);
};

export function failIfHasCharVarchar(dataType: DataType): void {
  if (hasCharVarchar(dataType)) {
    throw new Error("Cannot have char/varchar type in nested data type");
  }
};

export function quoteIdentifier(name: string): string {
  return "`" + name.replace("`", "``") + "`";
};

const validIdentPattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function quoteIfNeeded(part: string): string {
  return validIdentPattern.test(part) ? part : quoteIdentifier(part);
};

const TIMEZONE_OFFSET_MILLIS = new Date().getTimezoneOffset() * 60 * 1000; 
