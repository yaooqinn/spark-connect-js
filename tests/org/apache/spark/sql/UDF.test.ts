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
import { udf } from '../../../../../src/org/apache/spark/sql/functions';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';

describe('UDF Tests', () => {
  test('register UDF and use in SQL', async () => {
    const spark = await sharedSpark;
    
    // Register a simple UDF that doubles a number
    await spark.udf.register('doubleValue', (x: number) => x * 2, 'int');
    
    // Use the UDF in SQL
    await timeoutOrSatisfied(
      spark.sql('SELECT doubleValue(5) as result').then(df => {
        return df.collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getInt(0)).toBe(10);
        });
      })
    );
  });

  test('register UDF with string return type', async () => {
    const spark = await sharedSpark;
    
    // Register a UDF that converts number to string
    await spark.udf.register('numToStr', (x: number) => `Number: ${x}`, 'string');
    
    // Use the UDF in SQL
    await timeoutOrSatisfied(
      spark.sql('SELECT numToStr(42) as result').then(df => {
        return df.collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getString(0)).toBe('Number: 42');
        });
      })
    );
  });

  test('register Java UDF', async () => {
    const spark = await sharedSpark;
    
    // Register a Java UDF (this would require a Java class to exist on the server)
    // This test just ensures the API works without errors
    await expect(
      spark.udf.registerJavaFunction('javaUdf', 'com.example.MyUDF', DataTypes.IntegerType)
    ).resolves.not.toThrow();
  });

  test('use inline UDF with DataFrame API', async () => {
    const spark = await sharedSpark;
    
    // Create an inline UDF
    // Note: This test shows the API structure
    // Full integration would require applying the UDF to a column
    udf((x: number) => x * 2, 'int');
    
    // Create a DataFrame to verify basic functionality
    await timeoutOrSatisfied(
      spark.range(5).then(df => {
        return df.schema().then(schema => {
          expect(schema).toBeDefined();
        });
      })
    );
  });

  test('UDF with different data types', async () => {
    const spark = await sharedSpark;
    
    // Test with double
    await spark.udf.register('addOne', (x: number) => x + 1.5, 'double');
    
    await timeoutOrSatisfied(
      spark.sql('SELECT addOne(10.5) as result').then(df => {
        return df.collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getDouble(0)).toBeCloseTo(12.0, 1);
        });
      })
    );

    // Test with boolean
    await spark.udf.register('isEven', (x: number) => x % 2 == 0, 'boolean');
    
    await timeoutOrSatisfied(
      spark.sql('SELECT isEven(4) as result').then(df => {
        return df.collect().then(rows => {
          expect(rows.length).toBe(1);
          expect(rows[0].getBoolean(0)).toBe(true);
        });
      })
    );
  });
});
