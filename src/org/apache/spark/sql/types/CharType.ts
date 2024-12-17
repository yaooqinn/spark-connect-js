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

import { StringType } from "./StringType";

export class CharType extends StringType {
  length: number;

  constructor(length: number) {
    super("UTF8_BINARY");
    if (length <= 0) {
      throw new Error("CharType requires the length to be greater than 0");
    }
    this.length = length;
  }

  override defaultSize(): number {
    return this.length;
  }

  override typeName(): string {
    return `char(${this.length})`;
  }

  override toString(): string {
    return `CharType(${this.length})`;
  }

  static readonly INSTANCE: StringType = new StringType();
}