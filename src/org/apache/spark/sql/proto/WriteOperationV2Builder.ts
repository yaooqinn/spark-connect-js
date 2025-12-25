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

import { create } from "@bufbuild/protobuf";
import { WriteOperationV2, WriteOperationV2Schema, WriteOperationV2_Mode } from "../../../../../gen/spark/connect/commands_pb";
import { Expression } from "../../../../../gen/spark/connect/expressions_pb";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";

export class WriteOperationV2Builder {
  private writeOp: WriteOperationV2 = create(WriteOperationV2Schema, {});

  constructor() {}

  withInput(input: Relation) {
    this.writeOp.input = input;
    return this;
  }

  withTableName(tableName: string) {
    this.writeOp.tableName = tableName;
    return this;
  }

  withProvider(provider: string) {
    this.writeOp.provider = provider;
    return this;
  }

  withPartitioningColumns(columns: Expression[]) {
    this.writeOp.partitioningColumns = columns;
    return this;
  }

  withOptions(options: Record<string, string>) {
    this.writeOp.options = options;
    return this;
  }

  withTableProperties(properties: Record<string, string>) {
    this.writeOp.tableProperties = properties;
    return this;
  }

  withMode(mode: WriteOperationV2_Mode) {
    this.writeOp.mode = mode;
    return this;
  }

  withClusteringColumns(columns: string[]) {
    this.writeOp.clusteringColumns = columns;
    return this;
  }

  withOverwriteCondition(condition: Expression) {
    this.writeOp.overwriteCondition = condition;
    return this;
  }

  build(): WriteOperationV2 {
    return this.writeOp;
  }
}
