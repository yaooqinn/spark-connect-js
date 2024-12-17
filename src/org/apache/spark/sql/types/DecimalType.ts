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


import { FractionalType } from './data_types';

export class DecimalType extends FractionalType {
  precision: number;
  scale: number;

  constructor(precision: number = 10, scale: number = 0) {
    super();
    this.precision = precision;
    this.scale = scale;
  }

  override defaultSize(): number {
    return this.precision <= 18 ? 8 : 16;
  }

  override typeName(): string { 
    return `decimal(${this.precision},${this.scale})`
  };

  static readonly MAX_PRECISION: number = 38;
  static readonly MAX_SCALE: number = 38;
  static readonly DEFAULT_SCALE: number = 18;
  static readonly USER_DEFAULT: DecimalType = new DecimalType();
  static readonly SYSTEM_DEFAULT: DecimalType = new DecimalType(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
}