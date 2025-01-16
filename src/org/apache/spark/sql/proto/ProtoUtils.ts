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
import { Table as ArrowTable, tableToIPC } from "apache-arrow";
import { LocalRelation, LocalRelationSchema } from "../../../../../gen/spark/connect/relations_pb";
import { StructType } from "../types/StructType";

export function createLocalRelation(
    schema: string = "",
    data: Uint8Array | undefined = undefined): LocalRelation {
  return create(LocalRelationSchema, { schema: schema, data: data} );
}

export function createLocalRelationFromArrowTable(
    table: ArrowTable, schema: StructType): LocalRelation {
  if (table.numCols !== schema.fields.length) {
    throw new Error("Arrow table schema does not match StructType schema");
  }
  return createLocalRelation(schema.toDDL(), tableToIPC(table));
}
