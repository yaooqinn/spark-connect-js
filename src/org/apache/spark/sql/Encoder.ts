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

import { DataType } from "./types/data_types";
import { DataTypes } from "./types/DataTypes";
import { StructType } from "./types/StructType";

export interface Encoder<T> {
  schema: StructType;
};

export abstract class AgnosticEncoder<T> implements Encoder<T> {
  nullable: boolean;
  schema: StructType;
  constructor(
    public isPrimitive: boolean,
    public dataType: DataType,
    public isStruct: boolean = false) {
    this.nullable = !this.isPrimitive;
    this.schema = DataTypes.createStructType([DataTypes.createStructField("value", this.dataType, this.nullable)]);
  }
};

export class BooleanEncoder extends AgnosticEncoder<boolean> {
  constructor() {
    super(true, DataTypes.BooleanType);
  }
  static readonly INSTANCE: BooleanEncoder = new BooleanEncoder();
}