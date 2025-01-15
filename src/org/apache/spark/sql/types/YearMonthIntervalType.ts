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

import { AnsiIntervalType } from "./data_types";

export class YearMonthIntervalType extends AnsiIntervalType {
  startField: number;
  endField: number;

  constructor(
      startField: number = YearMonthIntervalType.YEAR,
      endField: number = YearMonthIntervalType.MONTH) {
    super();
    if (startField > endField) {
      throw new Error("startField must be less than or equal to endField");
    }
    this.startField = startField;
    this.endField = endField;
  }

  override defaultSize(): number {
    return 4;
  }

  override typeName(): string {
    if (this.startField === this.endField) {
      return `interval ${this.fieldToString(this.startField)}`;
    } else {
      return `interval ${this.fieldToString(this.startField)} to ${this.fieldToString(this.endField)}`;
    }
  }

  private fieldToString(field: number): string {
    switch (field) {
      case YearMonthIntervalType.YEAR:
        return "year";
      case YearMonthIntervalType.MONTH:
        return "month";
      default:
        throw new Error("Invalid field value");
    }
  }

  static readonly YEAR = 0;
  static readonly MONTH = 1;
  static readonly DEFAULT: YearMonthIntervalType = new YearMonthIntervalType(YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH);
}