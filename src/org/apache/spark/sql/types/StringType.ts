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

import { AtomicType } from "./data_types";

export class StringType extends AtomicType {
  collation: string;

  constructor(collation: string = "UTF8_BINARY") {
    super();
    this.collation = collation;
  }

  protected isUTF8BinaryCollation(): boolean {
      return 'UTF8_BINARY' == this.collation;
  }


  override defaultSize(): number {
    return 20;
  }

  override typeName(): string {
    return this.isUTF8BinaryCollation() ? "string" : `string collate ${this.collation}`;
  }

  override toString(): string {
    return this.isUTF8BinaryCollation() ? "StringType" : `StringType(${this.collation})`;
  }

  static readonly INSTANCE: StringType = new StringType();
}
