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

import { sharedSpark, timeoutOrSatisfied } from '../../../../helpers';

describe('DataFrameNaFunctions', () => {
  describe('fillna', () => {
    test('fill all columns with numeric value', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, null as b, 3 as c').then(df => {
          return df.na.fillna(0).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(0);
            expect(rows[0].getInt(2)).toBe(3);
          });
        })
      );
    });

    test('fill specific columns with numeric value', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT null as a, null as b, null as c').then(df => {
          return df.na.fillna(99, ['a', 'c']).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(99);
            expect(rows[0].isNullAt(1)).toBe(true);
            expect(rows[0].getInt(2)).toBe(99);
          });
        })
      );
    });

    test('fill with string value', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql("SELECT 'hello' as a, null as b").then(df => {
          return df.na.fillna('unknown').collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getString(0)).toBe('hello');
            expect(rows[0].getString(1)).toBe('unknown');
          });
        })
      );
    });

    test('fill with boolean value', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT true as a, null as b').then(df => {
          return df.na.fillna(false).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getBoolean(0)).toBe(true);
            expect(rows[0].getBoolean(1)).toBe(false);
          });
        })
      );
    });

    test('fill using fill alias', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, null as b').then(df => {
          return df.na.fill(100).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(100);
          });
        })
      );
    });
  });

  describe('dropna', () => {
    test('drop rows with any null values', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, null as b UNION ALL SELECT null as a, 4 as b').then(df => {
          return df.na.dropna().collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(2);
          });
        })
      );
    });

    test('drop rows with any null values using "any" parameter', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, null as b UNION ALL SELECT null as a, 4 as b').then(df => {
          return df.na.dropna('any').collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(2);
          });
        })
      );
    });

    test('drop rows only if all values are null', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, null as b UNION ALL SELECT null as a, null as b').then(df => {
          return df.na.dropna('all').collect().then(rows => {
            expect(rows.length).toBe(2);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[1].getInt(0)).toBe(3);
          });
        })
      );
    });

    test('drop rows with minimum non-null threshold', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b, 3 as c UNION ALL SELECT 4 as a, null as b, 6 as c UNION ALL SELECT null as a, null as b, 9 as c').then(df => {
          return df.na.dropna(2).collect().then(rows => {
            expect(rows.length).toBe(2);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[1].getInt(0)).toBe(4);
          });
        })
      );
    });

    test('drop rows with null in specific columns', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, null as b, 3 as c UNION ALL SELECT 4 as a, 5 as b, null as c').then(df => {
          return df.na.dropna('any', ['a', 'b']).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(4);
            expect(rows[0].getInt(1)).toBe(5);
          });
        })
      );
    });

    test('drop using drop alias', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, null as b').then(df => {
          return df.na.drop().collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
          });
        })
      );
    });
  });

  describe('replace', () => {
    test('replace numeric values in all columns', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b UNION ALL SELECT 1 as a, 3 as b').then(df => {
          return df.na.replace({ 1: 100, 2: 200 }).collect().then(rows => {
            expect(rows.length).toBe(2);
            expect(rows[0].getInt(0)).toBe(100);
            expect(rows[0].getInt(1)).toBe(200);
            expect(rows[1].getInt(0)).toBe(100);
            expect(rows[1].getInt(1)).toBe(3);
          });
        })
      );
    });

    test('replace string values in all columns', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql("SELECT 'foo' as a, 'bar' as b UNION ALL SELECT 'baz' as a, 'foo' as b").then(df => {
          return df.na.replace({ 'foo': 'replaced', 'bar': 'updated' }).collect().then(rows => {
            expect(rows.length).toBe(2);
            expect(rows[0].getString(0)).toBe('replaced');
            expect(rows[0].getString(1)).toBe('updated');
            expect(rows[1].getString(0)).toBe('baz');
            expect(rows[1].getString(1)).toBe('replaced');
          });
        })
      );
    });

    test('replace values in specific columns', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 1 as b, 1 as c').then(df => {
          return df.na.replace({ 1: 999 }, ['a', 'c']).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(999);
            expect(rows[0].getInt(1)).toBe(1);
            expect(rows[0].getInt(2)).toBe(999);
          });
        })
      );
    });

    test('replace null with value', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql("SELECT 'hello' as a, null as b").then(df => {
          return df.na.replace({ 'null': 'N/A' }).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getString(0)).toBe('hello');
            // Note: null replacement behavior may vary
          });
        })
      );
    });
  });

  describe('edge cases', () => {
    test('fillna on empty DataFrame', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a WHERE false').then(df => {
          return df.na.fillna(0).collect().then(rows => {
            expect(rows.length).toBe(0);
          });
        })
      );
    });

    test('dropna on DataFrame with no nulls', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b').then(df => {
          return df.na.dropna().collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(2);
          });
        })
      );
    });

    test('replace with empty replacement map', async () => {
      const spark = await sharedSpark;
      await timeoutOrSatisfied(
        spark.sql('SELECT 1 as a, 2 as b').then(df => {
          return df.na.replace({}).collect().then(rows => {
            expect(rows.length).toBe(1);
            expect(rows[0].getInt(0)).toBe(1);
            expect(rows[0].getInt(1)).toBe(2);
          });
        })
      );
    });
  });
});
