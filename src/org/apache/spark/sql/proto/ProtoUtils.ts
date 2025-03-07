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
import { Aggregate_GroupType, LocalRelation, LocalRelationSchema, SetOperation_SetOpType } from "../../../../../gen/spark/connect/relations_pb";
import { StructType } from "../types/StructType";
import { StorageLevel } from "../../storage/StorageLevel";
import { StorageLevelSchema, StorageLevel as StorageLevelPB } from "../../../../../gen/spark/connect/common_pb";
import { GroupType } from "./aggregate/GroupType";

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

export function createStorageLevelPB(storageLevel: StorageLevel): StorageLevelPB {
  return create(StorageLevelSchema, {
    useDisk: storageLevel.useDisk,
    useMemory: storageLevel.useMemory,
    useOffHeap: storageLevel.useOffHeap,
    deserialized: storageLevel.deserialized,
    replication: storageLevel.replication
  });
}

export function toGroupTypePB(groupType: GroupType): Aggregate_GroupType {
  switch (groupType) {
    case GroupType.GROUP_TYPE_GROUPBY:
      return Aggregate_GroupType.GROUPBY;
    case GroupType.GROUP_TYPE_CUBE:
      return Aggregate_GroupType.CUBE;
    case GroupType.GROUP_TYPE_ROLLUP:
      return Aggregate_GroupType.ROLLUP;
    case GroupType.GROUP_TYPE_PIVOT:
      return Aggregate_GroupType.PIVOT;
    case GroupType.GROUPING_SETS:
      return Aggregate_GroupType.GROUPING_SETS;
  }
}

export function toSetOpTypePB(setOpType?: string): SetOperation_SetOpType {
  switch (setOpType) {
    case "union":
      return SetOperation_SetOpType.UNION;
    case "intersect":
      return SetOperation_SetOpType.INTERSECT;
    case "except":
      return SetOperation_SetOpType.EXCEPT;
    default:
      throw new Error(`Unsupported set operation type: ${setOpType}`);
  }
}
