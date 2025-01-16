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
import { Plan, PlanSchema } from "../../../../../gen/spark/connect/base_pb";
import { Command } from "../../../../../gen/spark/connect/commands_pb";
import { LimitSchema, LocalRelation, LocalRelationSchema, OffsetSchema, RangeSchema, Read, Read_DataSourceSchema, Read_NamedTableSchema, ReadSchema, Relation, RelationCommon, RelationSchema, TailSchema, ToDFSchema, ToSchemaSchema } from "../../../../../gen/spark/connect/relations_pb";
import { DataTypes } from "../types";
import { StructType } from "../types/StructType";
import { CaseInsensitiveMap } from "../util/CaseInsensitiveMap";

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

export class RelationBuilder {
  private relation: Relation = create(RelationSchema, {});
  constructor() {}
  setRelationCommon(common: RelationCommon) {
    this.relation.common = common;
    return this;
  }
  setLocalRelation(localRelation: LocalRelation) {
    this.relation.relType = { case: "localRelation", value: localRelation }
    return this;
  }
  setRange(start: bigint, end: bigint, step: bigint, numPartitions?: number) {
    const range = create(RangeSchema, { start: start, end: end, step: step, numPartitions: numPartitions });
    this.relation.relType = { case: "range", value: range }
    return this;
  }
  setRead(read: Read) {
    this.relation.relType = { case: "read", value: read }
    return this;
  }
  setReadTable(
      table: string,
      options: CaseInsensitiveMap<string>,
      isStreaming: boolean = false) {
    const read = create(Read_NamedTableSchema, { unparsedIdentifier: table, options: options.toIndexSignature() });
    return this.setRead(create(ReadSchema, { readType: { case: "namedTable", value: read }, isStreaming: isStreaming }));
  }
  setReadDataSource(
      format: string | undefined,
      schema: string | undefined,
      paths: string[],
      predicates: string[],
      options: CaseInsensitiveMap<string>,
      isStreaming: boolean = false) {
    const datasource = create(Read_DataSourceSchema,
      {
        format: format,
        schema: schema,
        paths: paths,
        predicates: predicates,
        options: options.toIndexSignature()
      });
    return this.setRead(create(ReadSchema, { readType: { case: "dataSource", value: datasource }, isStreaming: isStreaming }));
  }
  setLimit(limit: number, input?: Relation) {
    const limitOp = create(LimitSchema, { input: input, limit: limit });
    this.relation.relType = { case: "limit", value: limitOp }
    return this;
  }
  setOffset(offset: number, input?: Relation) {
    const offsetOp = create(OffsetSchema, { input: input, offset: offset });
    this.relation.relType = { case: "offset", value: offsetOp }
    return this;
  }
  setTail(limit: number, input?: Relation) {
    const tailOp = create(TailSchema, { input: input, limit: limit });
    this.relation.relType = { case: "tail", value: tailOp }
    return this;
  }
  setToDf(columns: string[], input?: Relation) {
    const toDf = create(ToDFSchema, { input: input, columnNames: columns });
    this.relation.relType = { case: "toDf", value: toDf }
    return this;
  }
  setToSchema(schema: StructType, input?: Relation) {
    const toSchema = create(ToSchemaSchema, { input: input, schema: DataTypes.toProtoType(schema) });
    this.relation.relType = { case: "toSchema", value: toSchema }
    return this;
  }
  build(): Relation {
    return this.relation;
  }
}

export class PlanBuilder {
  private plan: Plan = create(PlanSchema, {});
  constructor() {}
  withRelationBuilder(f: (builder: RelationBuilder) => void) {
    const builder = new RelationBuilder();
    f(builder);
    this.setRelation(builder.build());
    return this;
  }
  setRelation(relation: Relation) {
    this.plan.opType = { case: "root", value: relation };
    return this;
  }
  setCommand(command: Command) {
    this.plan.opType = { case: "command", value: command };
    return this;
  }
  build(): Plan {
    return this.plan;
  }
}
