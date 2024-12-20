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

import { SparkSession } from '../../../../../src/org/apache/spark/sql/SparkSession';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';
import { logger } from '../../../../../src/org/apache/spark/logger';

test("abc", async () => {
  const spark = SparkSession
    .builder()
    .appName('test')
    .config('spark.sql.shuffle.partitions', '2')
    .getOrCreate();

  spark.then(s => {
    s.sql("SELECT 1 + 1 as a").then(df => {
      df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.IntegerType);
        expect(schema.fields[0].name).toBe("a");
        expect(schema.fields[0].nullable).toBe(false);
      });
    }).catch(e => {
      logger.error(e);
    });

    s.sql("SHOW TABLES").then(df => {
      df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.StringType);
        expect(schema.fields[0].name).toBe("namespace");
        expect(schema.fields[0].nullable).toBe(false);
        expect(schema.fields[1].dataType).toBe(DataTypes.StringType);
        expect(schema.fields[1].name).toBe("tableName");
        expect(schema.fields[1].nullable).toBe(false);
        expect(schema.fields[2].dataType).toBe(DataTypes.BooleanType);
        expect(schema.fields[2].name).toBe("isTemporary");
        expect(schema.fields[2].nullable).toBe(false);
      });

      df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.StringType);
        expect(schema.fields[0].name).toBe("namespace");
        expect(schema.fields[0].nullable).toBe(false);
        expect(schema.fields[1].dataType).toBe(DataTypes.StringType);
        expect(schema.fields[1].name).toBe("tableName");
        expect(schema.fields[1].nullable).toBe(false);
        expect(schema.fields[2].dataType).toBe(DataTypes.BooleanType);
        expect(schema.fields[2].name).toBe("isTemporary");
        expect(schema.fields[2].nullable).toBe(false);
      });
    });
  });
}, 30000);