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
import { DataTypes } from "./DataTypes";
import { Metadata } from "./metadata";
import { StructField } from "./StructField";

export class StructType extends DataType {
  fields: StructField[];
  constructor(...fields: StructField[]) {
    super();
    this.fields = fields;
  }

  override defaultSize(): number {
    return this.fields.map(f => f.dataType.defaultSize()).reduce((a, b) => a + b, 0);
  }

  // TODO: Support truncating long strings
  override simpleString(): string {
    return `struct<${this.fields.map(f => `${f.name}:${f.dataType.simpleString()}`).join(", ")}>`;
  }

  override catalogString(): string {
    return `struct<${this.fields.map(f => `${f.name}:${f.dataType.catalogString()}`).join(", ")}>`;
  }

  override sql(): string {
    return `STRUCT<${this.fields.map(f => `${f.sql()}`).join(", ")}>`;
  }

  override existsRecursively(f: (dt: DataType) => boolean): boolean {
    return f(this) || this.fields.some(field => field.dataType.existsRecursively(f));
  }

  fieldNames(): string[] {
    return this.fields.map(f => f.name);
  }

  names(): string[] {
    return this.fieldNames();
  }

  toDDL(): string {
    return this.fields.map(f => f.toDDL()).join(", ");
  }

  toString(): string {
    return `StructType(${this.fields.map(f => f.toString()).join(", ")})`;
  }

  add(field: StructField): StructType;
  add(name: string, dataType: DataType): StructType;
  add(name: string, dataType: DataType, nullable: boolean): StructType;
  add(name: string, dataType: DataType, nullable: boolean, metadata: Metadata): StructType;
  add(...args: any[]): StructType {
    if (args.length === 1) {
      this.fields.push(args[0]);
    } else if (args.length === 2) {
      const field = DataTypes.createStructField(args[0], args[1], true, Metadata.empty());
      this.fields.push(field);
    } else if (args.length === 3) {
      const field = DataTypes.createStructField(args[0], args[1], args[2], Metadata.empty());
      this.fields.push(field);
    } else {
      const field = DataTypes.createStructField(args[0], args[1], args[2], args[3]);
      this.fields.push(field);
    }
    return this;
  }
}