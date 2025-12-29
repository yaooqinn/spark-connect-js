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
import { CommonInlineUserDefinedFunctionBuilder } from '../../../../../src/org/apache/spark/sql/proto/expression/udf/CommonInlineUserDefinedFunctionBuilder';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';
import { col } from '../../../../../src/org/apache/spark/sql/functions';

describe('MapPartitions Operations', () => {
  
  test('mapPartitions creates correct plan structure', async () => {
    const spark = await sharedSpark;
    const df = await spark.sql('SELECT 1 as id, "a" as value');
    
    // Create a simple Python UDF for testing plan creation
    // This Python code would double each id value
    const pythonCode = `
def process_partition(partition):
    for row in partition:
        yield (row.id * 2, row.value)
`;
    
    const udf = new CommonInlineUserDefinedFunctionBuilder('process_partition', true)
      .withPythonUDF(
        DataTypes.createStructType([
          DataTypes.createStructField('id', DataTypes.IntegerType, false),
          DataTypes.createStructField('value', DataTypes.StringType, false),
        ]),
        200, // MAP_ITER eval type
        new TextEncoder().encode(pythonCode),
        '3.8',
        []
      )
      .build();
    
    // Create the mapPartitions DataFrame
    const result = df.mapPartitions(udf);
    
    // Verify that the plan contains mapPartitions relation
    expect(result.plan.relation).toBeDefined();
    expect(result.plan.relation?.relType.case).toBe('mapPartitions');
    
    if (result.plan.relation?.relType.case === 'mapPartitions') {
      expect(result.plan.relation.relType.value.func).toBeDefined();
      expect(result.plan.relation.relType.value.func?.functionName).toBe('process_partition');
    }
  });

  test('groupMap creates correct plan structure', async () => {
    const spark = await sharedSpark;
    const df = await spark.sql('SELECT 1 as category, 10 as value UNION ALL SELECT 1 as category, 20 as value');
    
    // Create a simple Python UDF for group aggregation
    const pythonCode = `
def aggregate_group(key, rows):
    total = sum(row.value for row in rows)
    yield (key.category, total)
`;
    
    const udf = new CommonInlineUserDefinedFunctionBuilder('aggregate_group', true)
      .withPythonUDF(
        DataTypes.createStructType([
          DataTypes.createStructField('category', DataTypes.IntegerType, false),
          DataTypes.createStructField('total', DataTypes.IntegerType, false),
        ]),
        200, // MAP_ITER eval type
        new TextEncoder().encode(pythonCode),
        '3.8',
        []
      )
      .build();
    
    // Create the groupMap result
    const grouped = df.groupBy('category');
    const result = grouped.groupMap(udf);
    
    // Verify that the plan contains groupMap relation
    expect(result.plan.relation).toBeDefined();
    expect(result.plan.relation?.relType.case).toBe('groupMap');
    
    if (result.plan.relation?.relType.case === 'groupMap') {
      expect(result.plan.relation.relType.value.func).toBeDefined();
      expect(result.plan.relation.relType.value.func?.functionName).toBe('aggregate_group');
      expect(result.plan.relation.relType.value.groupingExpressions).toHaveLength(1);
    }
  });

  test('coGroupMap creates correct plan structure', async () => {
    const spark = await sharedSpark;
    const df1 = await spark.sql('SELECT 1 as id, "left" as side');
    const df2 = await spark.sql('SELECT 1 as id, "right" as side');
    
    // Create a simple Python UDF for co-group join
    const pythonCode = `
def join_groups(key, left_rows, right_rows):
    left_list = list(left_rows)
    right_list = list(right_rows)
    for l in left_list:
        for r in right_list:
            yield (key.id, l.side, r.side)
`;
    
    const udf = new CommonInlineUserDefinedFunctionBuilder('join_groups', true)
      .withPythonUDF(
        DataTypes.createStructType([
          DataTypes.createStructField('id', DataTypes.IntegerType, false),
          DataTypes.createStructField('left_side', DataTypes.StringType, false),
          DataTypes.createStructField('right_side', DataTypes.StringType, false),
        ]),
        200, // MAP_ITER eval type
        new TextEncoder().encode(pythonCode),
        '3.8',
        []
      )
      .build();
    
    // Create the coGroupMap result
    const result = df1.coGroupMap(df2, [col('id')], udf);
    
    // Verify that the plan contains coGroupMap relation
    expect(result.plan.relation).toBeDefined();
    expect(result.plan.relation?.relType.case).toBe('coGroupMap');
    
    if (result.plan.relation?.relType.case === 'coGroupMap') {
      expect(result.plan.relation.relType.value.func).toBeDefined();
      expect(result.plan.relation.relType.value.func?.functionName).toBe('join_groups');
      expect(result.plan.relation.relType.value.inputGroupingExpressions).toHaveLength(1);
      expect(result.plan.relation.relType.value.otherGroupingExpressions).toHaveLength(1);
    }
  });
});
