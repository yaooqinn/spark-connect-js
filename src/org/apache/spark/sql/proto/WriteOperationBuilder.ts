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
import {
  WriteOperation,
  WriteOperationSchema,
  WriteOperation_SaveMode,
  WriteOperation_SaveTableSchema,
  WriteOperation_SaveTable_TableSaveMethod,
  WriteOperation_BucketBySchema
} from "../../../../../gen/spark/connect/commands_pb";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";
import { AnalysisException } from "../errors";

export class WriteOperationBuilder {
  private writeOp: WriteOperation = create(WriteOperationSchema, {});

  withInput(input: Relation) {
    this.writeOp.input = input;
    return this;
  }

  withMode(mode: WriteOperation_SaveMode): WriteOperationBuilder;
  withMode(mode: string): WriteOperationBuilder;
  withMode(mode: WriteOperation_SaveMode | string): WriteOperationBuilder {
    if (typeof mode === 'string') {
      let normalizedMode = mode.toUpperCase();
      if (normalizedMode === "ERROR" || normalizedMode === "ERRORIFEXISTS" || normalizedMode === "DEFAULT") {
        normalizedMode = "ERROR_IF_EXISTS";
      }
      const protoMode = WriteOperation_SaveMode[normalizedMode as keyof typeof WriteOperation_SaveMode];
      // Runtime validation for invalid mode strings
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (protoMode === undefined) {
        throw new AnalysisException(
          "INVALID_SAVE_MODE",
          `The specified save mode "${mode}" is invalid. Valid save modes include "append", "overwrite", "ignore", "error", "errorifexists", and "default"`,
          "42000"
        );
      }
      this.writeOp.mode = protoMode;
    } else {
      this.writeOp.mode = mode;
    }
    return this;
  }

  withSource(source: string) {
    this.writeOp.source = source;
    return this;
  }

  withPath(path: string) {
    this.writeOp.saveType = { case: "path", value: path };
    return this;
  }

  withTable(tableName: string, saveMethod: WriteOperation_SaveTable_TableSaveMethod) {
    this.writeOp.saveType = {
      case: "table",
      value: create(WriteOperation_SaveTableSchema, {
        tableName: tableName,
        saveMethod: saveMethod
      })
    };
    return this;
  }

  withOptions(options: Record<string, string>) {
    this.writeOp.options = options;
    return this;
  }

  withPartitioningColumns(columns: string[]) {
    this.writeOp.partitioningColumns = columns;
    return this;
  }

  withClusteringColumns(columns: string[]) {
    this.writeOp.clusteringColumns = columns;
    return this;
  }

  withBucketBy(bucketColumnNames: string[], numBuckets: number) {
    this.writeOp.bucketBy = create(WriteOperation_BucketBySchema, {
      bucketColumnNames: bucketColumnNames,
      numBuckets: numBuckets
    });
    return this;
  }

  withSortColumnNames(columnNames: string[]) {
    this.writeOp.sortColumnNames = columnNames;
    return this;
  }

  build(): WriteOperation {
    return this.writeOp;
  }
}
