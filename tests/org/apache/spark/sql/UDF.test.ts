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
    
    // Register BytesToString Java UDF - converts byte array to String
    await spark.udf.registerJava('bytesToString', 'org.apache.spark.api.python.BytesToString', DataTypes.StringType);
    
    // Test that the UDF was registered successfully by using it in SQL
    const df = await spark.sql("SELECT bytesToString(cast('test' as binary)) as result");
    const rows = await df.collect();
    expect(rows.length).toBe(1);
    expect(rows[0].getString(0)).toBe('test');
  });

  test('register and use Java UDF without return type', async () => {
    const spark = await sharedSpark;
    
    // Register BytesToString Java UDF without specifying return type (let server decide)
    await spark.udf.registerJava('bytesToStringAuto', 'org.apache.spark.api.python.BytesToString');
    
    // Test that the UDF was registered successfully by using it in SQL
    const df = await spark.sql("SELECT bytesToStringAuto(cast('hello' as binary)) as result");
    const rows = await df.collect();
    expect(rows.length).toBe(1);
    expect(rows[0].getString(0)).toBe('hello');
  });
});
