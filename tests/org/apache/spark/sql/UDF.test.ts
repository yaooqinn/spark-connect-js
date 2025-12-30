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
  test('register Java UDF with return type', async () => {
    const spark = await sharedSpark;
    
    // Register a Java UDF with explicit return type
    // This test just ensures the API works without errors
    await expect(
      spark.udf.registerJava('javaUdf', 'com.example.MyUDF', DataTypes.IntegerType)
    ).resolves.not.toThrow();
  });

  test('register Java UDF without return type', async () => {
    const spark = await sharedSpark;
    
    // Register a Java UDF without return type (let server decide)
    // This test just ensures the API works without errors
    await expect(
      spark.udf.registerJava('javaUdfAuto', 'com.example.MyAutoUDF')
    ).resolves.not.toThrow();
  });
});
