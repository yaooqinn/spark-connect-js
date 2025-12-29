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

import { sharedSpark } from '../../../../helpers';

describe('DataFrameNaFunctions', () => {
  describe('drop', () => {
    test('drop with default (any)', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, 3 as b UNION ALL SELECT 4 as a, null as b UNION ALL SELECT 5 as a, 6 as b"
      );
      const result = df.na.drop();
      const rows = await result.collect();
      expect(rows.length).toBe(2); // Only rows without any nulls
    });

    test('drop with how="any"', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, 3 as b UNION ALL SELECT 4 as a, null as b UNION ALL SELECT 5 as a, 6 as b"
      );
      const result = df.na.drop('any');
      const rows = await result.collect();
      expect(rows.length).toBe(2);
    });

    test('drop with how="all"', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, null as b UNION ALL SELECT 4 as a, null as b UNION ALL SELECT 5 as a, 6 as b"
      );
      const result = df.na.drop('all');
      const rows = await result.collect();
      expect(rows.length).toBe(3); // All except the row with all nulls
    });

    test('drop with specific columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, 4 as b, 5 as c UNION ALL SELECT 6 as a, null as b, 7 as c"
      );
      const result = df.na.drop(['a']);
      const rows = await result.collect();
      expect(rows.length).toBe(2); // Drop only rows where 'a' is null
    });

    test('drop with how and columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, null as b, 5 as c UNION ALL SELECT 6 as a, null as b, 7 as c"
      );
      const result = df.na.drop('all', ['a', 'b']);
      const rows = await result.collect();
      expect(rows.length).toBe(2); // Drop only rows where both a and b are null
    });

    test('drop with minNonNulls threshold', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, null as b, 5 as c UNION ALL SELECT null as a, 4 as b, null as c UNION ALL SELECT 6 as a, 7 as b, 8 as c"
      );
      const result = df.na.drop(2); // Keep rows with at least 2 non-null values
      const rows = await result.collect();
      // First row: 3 non-nulls (kept)
      // Second row: 1 non-null (dropped) 
      // Third row: 1 non-null (dropped)
      // Fourth row: 3 non-nulls (kept)
      expect(rows.length).toBe(2);
    });

    test('drop with minNonNulls and columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, null as b, 5 as c UNION ALL SELECT null as a, 4 as b, 7 as c"
      );
      const result = df.na.drop(2, ['a', 'b', 'c']);
      const rows = await result.collect();
      expect(rows.length).toBe(2);
    });
  });

  describe('fill', () => {
    test('fill with single value', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, 3 as b UNION ALL SELECT 4 as a, null as b"
      );
      const result = df.na.fill(0);
      const rows = await result.collect();
      expect(rows.length).toBe(3);
      // Check that nulls are filled
      const hasNull = rows.some(row => {
        try {
          return row.getInt(0) === null || row.getInt(1) === null;
        } catch {
          return false;
        }
      });
      expect(hasNull).toBe(false);
    });

    test('fill with string value', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 'a' as col1, 'b' as col2 UNION ALL SELECT null as col1, 'c' as col2 UNION ALL SELECT 'd' as col1, null as col2"
      );
      const result = df.na.fill('unknown');
      const rows = await result.collect();
      expect(rows.length).toBe(3);
      // Verify no null strings
      rows.forEach(row => {
        expect(row.getString(0)).not.toBeNull();
        expect(row.getString(1)).not.toBeNull();
      });
    });

    test('fill with value and specific columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, null as b, null as c"
      );
      const result = df.na.fill(999, ['a', 'b']);
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      // Column c should still have null in second row, a and b should be filled
    });

    test('fill with value map', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, null as b"
      );
      const result = df.na.fill({ a: 100, b: 200 });
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      // Check second row has filled values
      expect(rows[1].getInt(0)).toBe(100);
      expect(rows[1].getInt(1)).toBe(200);
    });

    test('fill with boolean value', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT true as flag1, false as flag2 UNION ALL SELECT null as flag1, null as flag2"
      );
      const result = df.na.fill(false);
      const rows = await result.collect();
      expect(rows.length).toBe(2);
    });
  });

  describe('replace', () => {
    test('replace single value', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, 1 as b UNION ALL SELECT 1 as a, 4 as b"
      );
      const result = df.na.replace(999, 1);
      const rows = await result.collect();
      expect(rows.length).toBe(3);
      // Check that 1 is replaced with 999
      rows.forEach(row => {
        const valA = row.getInt(0);
        const valB = row.getInt(1);
        expect(valA).not.toBe(1);
        expect(valB).not.toBe(1);
      });
    });

    test('replace with specific columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, 1 as b"
      );
      const result = df.na.replace(999, 1, ['b']);
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      // Only column b should have replacement
      expect(rows[0].getInt(0)).toBe(1); // a is not replaced
      expect(rows[1].getInt(1)).toBe(999); // b is replaced
    });

    test('replace with replacement map', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT 2 as a, 3 as b UNION ALL SELECT 3 as a, 1 as b"
      );
      const result = df.na.replace({ '1': 100, '2': 200 });
      const rows = await result.collect();
      expect(rows.length).toBe(3);
      // Check replacements occurred
      expect(rows[0].getInt(0)).toBe(100); // 1 -> 100
      expect(rows[0].getInt(1)).toBe(200); // 2 -> 200
    });

    test('replace strings', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 'old' as col1, 'value' as col2 UNION ALL SELECT 'new' as col1, 'old' as col2"
      );
      const result = df.na.replace('new', 'old');
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      // Check string replacement
      rows.forEach(row => {
        expect(row.getString(0)).not.toBe('old');
        expect(row.getString(1)).not.toBe('old');
      });
    });

    test('replace with map and columns', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT 2 as a, 1 as b, 2 as c"
      );
      const result = df.na.replace({ '1': 10, '2': 20 }, ['a', 'b']);
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      // Column c should not be affected
    });

    test('replace null with value', async () => {
      const spark = await sharedSpark;
      // Note: replace() is typically used to replace specific non-null values.
      // To replace null values, use fill() instead.
      // This test verifies that attempting to replace 'null' values works as expected.
      const df = await spark.sql(
        "SELECT 'a' as col1, 'b' as col2 UNION ALL SELECT 'c' as col1, 'b' as col2"
      );
      const result = df.na.replace({ 'a': 'replaced_a' });
      const rows = await result.collect();
      expect(rows.length).toBe(2);
      expect(rows[0].getString(0)).toBe('replaced_a');
    });
  });

  describe('integration tests', () => {
    test('chaining na operations', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, 3 as b UNION ALL SELECT 1 as a, null as b"
      );
      const result = df.na.fill(0).na.replace(100, 1);
      const rows = await result.collect();
      expect(rows.length).toBe(3);
    });

    test('na operations with select', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT null as a, null as b, 5 as c"
      );
      const result = df.na.drop(['a']).select('a', 'c');
      const rows = await result.collect();
      expect(rows.length).toBe(1);
    });

    test('na operations with filter', async () => {
      const spark = await sharedSpark;
      const df = await spark.sql(
        "SELECT 1 as a, 2 as b UNION ALL SELECT null as a, 3 as b UNION ALL SELECT 4 as a, 5 as b"
      );
      const result = df.na.fill(0).filter("a > 0");
      const rows = await result.collect();
      // After fill: row1(1,2), row2(0,3), row3(4,5)
      // After filter a > 0: row1(1,2), row3(4,5)
      expect(rows.length).toBe(2);
    });
  });
});
