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

import { Column } from "./Column";
import { WindowSpec } from "./WindowSpec";

/**
 * Utility functions for defining window specifications.
 */
export class Window {
  /**
   * Value representing unbounded preceding in window frames.
   */
  static readonly unboundedPreceding = Number.MIN_SAFE_INTEGER;

  /**
   * Value representing unbounded following in window frames.
   */
  static readonly unboundedFollowing = Number.MAX_SAFE_INTEGER;

  /**
   * Value representing the current row in window frames.
   */
  static readonly currentRow = 0;

  /**
   * Creates a WindowSpec with the partitioning defined.
   */
  static partitionBy(...cols: (string | Column)[]): WindowSpec {
    return new WindowSpec().partitionBy(...cols);
  }

  /**
   * Creates a WindowSpec with the ordering defined.
   */
  static orderBy(...cols: (string | Column)[]): WindowSpec {
    return new WindowSpec().orderBy(...cols);
  }

  /**
   * Creates a WindowSpec with the frame boundaries defined, from `start` (inclusive) to `end` (inclusive).
   * Both `start` and `end` are relative positions from the current row. For example,
   * "0" means "current row", while "-1" means the row before the current row, and
   * "5" means the fifth row after the current row.
   */
  static rowsBetween(start: number, end: number): WindowSpec {
    return new WindowSpec().rowsBetween(start, end);
  }

  /**
   * Creates a WindowSpec with the frame boundaries defined, from `start` (inclusive) to `end` (inclusive).
   * Both `start` and `end` are relative from the current row. For example,
   * "0" means "current row", while "-1" means one off before the current row, and
   * "5" means the five off after the current row.
   */
  static rangeBetween(start: number, end: number): WindowSpec {
    return new WindowSpec().rangeBetween(start, end);
  }
}
