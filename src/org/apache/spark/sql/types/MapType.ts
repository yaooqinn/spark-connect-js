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

import { DataType } from "./data_types";

export class MapType extends DataType {
  keyType: DataType;
  valueType: DataType;
  valueContainsNull: boolean;

  constructor(keyType: DataType, valueType: DataType, valueContainsNull: boolean) {
    super();
    this.keyType = keyType;
    this.valueType = valueType;
    this.valueContainsNull = valueContainsNull;
  }

  override defaultSize(): number {
    return 1 * this.keyType.defaultSize() + 1 * this.valueType.defaultSize();
  }

  override simpleString(): string {
    return `map<${this.keyType.simpleString()},${this.valueType.simpleString()}>`;
  }

  override catalogString(): string {
    return `map<${this.keyType.catalogString()},${this.valueType.catalogString()}>`;
  }

  override sql(): string {
    return `MAP<${this.keyType.sql()},${this.valueType.sql()}>`;
  }

  override toString(): string {
    return `MapType(${this.keyType}, ${this.valueType}, ${this.valueContainsNull})`;
  }

  override existsRecursively(f: (dt: DataType) => boolean): boolean {
    return f(this) || this.keyType.existsRecursively(f) || this.valueType.existsRecursively(f);
  }
}