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
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';

describe('Java UDF Tests', () => {
  test('register and use Java UDF with return type', async () => {
    const spark = await sharedSpark;
    
    // Register StringLengthTest Java UDF - calculates sum of lengths of two strings
    await spark.udf.registerJava('strLenSum', 'test.org.apache.spark.sql.JavaUDFSuite.StringLengthTest', DataTypes.IntegerType);
    
    // Test that the UDF was registered successfully by using it in SQL
    const df = await spark.sql("SELECT strLenSum('hello', 'world') as result");
    const rows = await df.collect();
    expect(rows.length).toBe(1);
    expect(rows[0].getInt(0)).toBe(10); // 5 + 5 = 10
  });

  test('register and use Java UDF without return type', async () => {
    const spark = await sharedSpark;
    
    // Register StringLengthTest Java UDF without specifying return type (let server decide)
    await spark.udf.registerJava('strLenSumAuto', 'test.org.apache.spark.sql.JavaUDFSuite.StringLengthTest');
    
    // Test that the UDF was registered successfully by using it in SQL
    const df = await spark.sql("SELECT strLenSumAuto('test', 'case') as result");
    const rows = await df.collect();
    expect(rows.length).toBe(1);
    expect(rows[0].getInt(0)).toBe(8); // 4 + 4 = 8
  });
});
