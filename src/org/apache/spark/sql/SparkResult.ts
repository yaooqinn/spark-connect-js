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

import { Schema, Table, tableFromIPC, util } from 'apache-arrow';
import * as b from '../../../../gen/spark/connect/base_pb';
import { tableToRows } from './arrow/ArrowUtils';
import { Row } from './Row';
import { DataTypes } from './types/DataTypes';
import { StructType } from './types/StructType';

const { compareSchemas } = util;

export class SparkResult {
  private operationId_: string | undefined = undefined;
  private structType: StructType | undefined = undefined;
  private arrowSchema: Schema | undefined = undefined;
  private numRecords: number = 0;

  constructor(
    private readonly responses: Iterator<b.ExecutePlanResponse>,
    // public readonly encoder: Encoder<T>,
  ) { }


  schema(): StructType {
    if (!this.structType) {
      this.processResponses(false, true);
    }
    return this.structType as StructType;
  }

  operationId(): string {
    if (!this.operationId_) {
      this.processResponses(true);
    }
    return this.operationId_ as string;
  }

  toArray(): Row[] {
    const tables = this.processResponses();
    const results = tables.flatMap(table => tableToRows(table, this.schema()));
    return results;
  }

  processResponses(
      stopOnOperationId: boolean = false,
      stopOnSchema: boolean = false,
      stopOnArrowSchema: boolean = false,
      stopOnFirstNonEmptyResponse = false): Table[] {
    const tables: Table[] = [];
    let stop: boolean = false;
    let next = this.responses.next();
    while (!stop && !next.done) {

      const response = next.value;
      
      // TODO: Handle Metrics

      if (this.operationId === undefined) {
        this.operationId_ = response.operationId;
      }
      stop = stop || stopOnOperationId;
      
      const responseType = response.responseType

      if (responseType.case === 'executionProgress') {
        // TODO: Handle ExecutionProgress
      }

      if (response.schema) {
        this.structType = DataTypes.fromProtoType(response.schema) as StructType;
        stop = stop || stopOnSchema;
      }

      if (responseType.case === 'arrowBatch') {
        const arrowBatch = responseType.value;
        const table = tableFromIPC(arrowBatch.data);
        if (!this.arrowSchema) {
          this.arrowSchema = table.schema;
          stop = stop || stopOnArrowSchema;
        } else if (!compareSchemas(this.arrowSchema, table.schema)) {
          throw new Error(`Schema Mismatch between expected and received schema:
            Expected: ${this.arrowSchema}
            Received: ${table.schema}`);
        }

        if (!this.structType) {
          this.structType = DataTypes.fromArrowSchema(table.schema);
        }
        // console.log(table.schema)

        if (arrowBatch.startOffset) {
          if (this.numRecords !== Number(arrowBatch.startOffset)) {
            throw new Error(`Expected arrow batch to start at row offset ${this.numRecords} in results,
              but received arrow batch starting at offset ${arrowBatch.startOffset}`);
          }
        }
        if (table.numRows !== Number(arrowBatch.rowCount)) {
          throw new Error(`Expected ${arrowBatch.rowCount} rows in arrow batch but got ${table.numRows}.`);
        }

        if (table.numRows > 0) {
          this.numRecords += table.numRows;
          tables.push(table);
          stop = stop || stopOnFirstNonEmptyResponse;
        }
      }
      next = this.responses.next();
    }
    return tables;
  }
}