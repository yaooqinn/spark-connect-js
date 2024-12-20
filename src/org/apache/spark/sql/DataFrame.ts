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

import { create } from '@bufbuild/protobuf';
import * as b from '../../../../gen/spark/connect/base_pb';
import { SparkSession } from './SparkSession';
import { DataTypes } from './types/DataTypes';
import { StructType } from './types/StructType';

import { logger } from '../logger';

export class DataFrame {
  private spark_: SparkSession;
  private plan_: b.Plan;
  private cachedSchema_: StructType | undefined = undefined;

  constructor(spark: SparkSession, plan: b.Plan) {
    this.spark_ = spark;
    this.plan_ = plan;
  }

  async schema(): Promise<StructType> {
    if (this.cachedSchema_) {
      return this.cachedSchema_;
    }

    const planReqSchema = create(b.AnalyzePlanRequest_SchemaSchema,
      { plan: this.plan_});

    return this.spark_.client.analyze(req =>
      req.analyze = { value: planReqSchema, case: "schema"}).then(resp => {
        if (resp.result.case === "schema" && resp.result.value.schema) {
          logger.debug("Schema in protobuf:", JSON.stringify(resp.result.value.schema));
          this.cachedSchema_ = DataTypes.fromProto(resp.result.value.schema) as StructType;
          logger.debug("Schema in typecript:", this.cachedSchema_);
          return this.cachedSchema_;
        } else {
          throw new Error("Failed to get schema");
        }
      });
  }
}