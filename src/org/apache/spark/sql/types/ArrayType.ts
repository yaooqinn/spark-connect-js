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

export class ArrayType extends DataType {
  elementType: DataType;
  containsNull: boolean;

  constructor(elementType: DataType, containsNull: boolean) {
    super();
    this.elementType = elementType;
    this.containsNull = containsNull;
  }

  override defaultSize(): number {
    return 1 * this.elementType.defaultSize();
  }

  override simpleString(): string {
    return `array<${this.elementType.simpleString()}>`;
  }

  override catalogString(): string {
    return `array<${this.elementType.catalogString()}>`;
  }

  override sql(): string {
    return `ARRAY<${this.elementType.sql()}>`;
  }

  override toString(): string {
    return `ArrayType(${this.elementType}, ${this.containsNull})`;
  }

  override existsRecursively(f: (dt: DataType) => boolean): boolean {
    return f(this) || this.elementType.existsRecursively(f);
  }
}