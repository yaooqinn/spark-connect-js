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

import { Window } from '../../../../../src/org/apache/spark/sql/Window';
import { WindowSpec } from '../../../../../src/org/apache/spark/sql/WindowSpec';
import { Column } from '../../../../../src/org/apache/spark/sql/Column';
import * as F from '../../../../../src/org/apache/spark/sql/functions';

describe('Window', () => {
  test('should have correct constant values', () => {
    expect(Window.currentRow).toBe(0);
    expect(Window.unboundedPreceding).toBe(Number.MIN_SAFE_INTEGER);
    expect(Window.unboundedFollowing).toBe(Number.MAX_SAFE_INTEGER);
  });

  test('should create WindowSpec with partitionBy', () => {
    const spec = Window.partitionBy('department');
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should create WindowSpec with orderBy', () => {
    const spec = Window.orderBy('salary');
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should create WindowSpec with rowsBetween', () => {
    const spec = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should create WindowSpec with rangeBetween', () => {
    const spec = Window.rangeBetween(-1, 1);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should chain WindowSpec methods', () => {
    const spec = Window.partitionBy('department')
      .orderBy('salary')
      .rowsBetween(Window.unboundedPreceding, Window.currentRow);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should support Column objects in partitionBy', () => {
    const spec = Window.partitionBy(new Column('department'));
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should support Column objects in orderBy', () => {
    const spec = Window.orderBy(new Column('salary'));
    expect(spec).toBeInstanceOf(WindowSpec);
  });
});

describe('WindowSpec', () => {
  test('should be chainable', () => {
    const spec = new WindowSpec()
      .partitionBy('department')
      .orderBy('salary')
      .rowsBetween(Window.unboundedPreceding, Window.currentRow);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should support multiple partition columns', () => {
    const spec = Window.partitionBy('department', 'division');
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should support multiple order columns', () => {
    const spec = Window.orderBy('salary', 'name');
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should handle unbounded preceding to current row', () => {
    const spec = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should handle unbounded following', () => {
    const spec = Window.rangeBetween(Window.currentRow, Window.unboundedFollowing);
    expect(spec).toBeInstanceOf(WindowSpec);
  });

  test('should handle numeric boundaries', () => {
    const spec = Window.rowsBetween(-1, 1);
    expect(spec).toBeInstanceOf(WindowSpec);
  });
});

describe('Column.over()', () => {
  test('should create window expression with WindowSpec', () => {
    const windowSpec = Window.partitionBy('department').orderBy('salary');
    const col = F.row_number().over(windowSpec);
    expect(col).toBeInstanceOf(Column);
  });

  test('should throw error when argument is not WindowSpec', () => {
    expect(() => {
      F.row_number().over({} as any);
    }).toThrow('Argument to over() must be a WindowSpec');
  });
});

describe('Window functions', () => {
  test('row_number should return Column', () => {
    const col = F.row_number();
    expect(col).toBeInstanceOf(Column);
  });

  test('rank should return Column', () => {
    const col = F.rank();
    expect(col).toBeInstanceOf(Column);
  });

  test('dense_rank should return Column', () => {
    const col = F.dense_rank();
    expect(col).toBeInstanceOf(Column);
  });

  test('percent_rank should return Column', () => {
    const col = F.percent_rank();
    expect(col).toBeInstanceOf(Column);
  });

  test('cume_dist should return Column', () => {
    const col = F.cume_dist();
    expect(col).toBeInstanceOf(Column);
  });

  test('ntile should return Column', () => {
    const col = F.ntile(4);
    expect(col).toBeInstanceOf(Column);
  });

  test('lag should return Column', () => {
    const col = F.lag('salary', 1);
    expect(col).toBeInstanceOf(Column);
  });

  test('lead should return Column', () => {
    const col = F.lead('salary', 1);
    expect(col).toBeInstanceOf(Column);
  });

  test('nth_value should return Column', () => {
    const col = F.nth_value('salary', 2);
    expect(col).toBeInstanceOf(Column);
  });
});
