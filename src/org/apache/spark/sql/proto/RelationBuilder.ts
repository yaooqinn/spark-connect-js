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
import { LimitSchema, LocalRelation, OffsetSchema, RangeSchema, Read, Read_DataSourceSchema, Read_NamedTableSchema, ReadSchema, Relation, RelationCommon, RelationSchema, ShowStringSchema, TailSchema, ToDFSchema, ToSchemaSchema } from "../../../../../gen/spark/connect/relations_pb";
import { CaseInsensitiveMap } from "../util/CaseInsensitiveMap";
import { StructType } from "../types/StructType";
import { DataTypes } from "../types";
import { Catalog } from "../../../../../gen/spark/connect/catalog_pb";
import { CatalogBuilder } from "./CatalogBuilder";

export class RelationBuilder {
  private relation: Relation = create(RelationSchema, {});
  constructor() {}
  withRelationCommon(common: RelationCommon) {
    this.relation.common = common;
    return this;
  }
  withLocalRelation(localRelation: LocalRelation) {
    this.relation.relType = { case: "localRelation", value: localRelation }
    return this;
  }
  withRange(start: bigint, end: bigint, step: bigint, numPartitions?: number) {
    const range = create(RangeSchema, { start: start, end: end, step: step, numPartitions: numPartitions });
    this.relation.relType = { case: "range", value: range }
    return this;
  }
  withRead(read: Read) {
    this.relation.relType = { case: "read", value: read }
    return this;
  }
  withReadTable(
      table: string,
      options: CaseInsensitiveMap<string>,
      isStreaming: boolean = false) {
    const read = create(Read_NamedTableSchema, { unparsedIdentifier: table, options: options.toIndexSignature() });
    return this.withRead(create(ReadSchema, { readType: { case: "namedTable", value: read }, isStreaming: isStreaming }));
  }
  withReadDataSource(
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
    return this.withRead(create(ReadSchema, { readType: { case: "dataSource", value: datasource }, isStreaming: isStreaming }));
  }
  withLimit(limit: number, input?: Relation) {
    const limitOp = create(LimitSchema, { input: input, limit: limit });
    this.relation.relType = { case: "limit", value: limitOp }
    return this;
  }
  withOffset(offset: number, input?: Relation) {
    const offsetOp = create(OffsetSchema, { input: input, offset: offset });
    this.relation.relType = { case: "offset", value: offsetOp }
    return this;
  }
  withTail(limit: number, input?: Relation) {
    const tailOp = create(TailSchema, { input: input, limit: limit });
    this.relation.relType = { case: "tail", value: tailOp }
    return this;
  }
  withToDf(columns: string[], input?: Relation) {
    const toDf = create(ToDFSchema, { input: input, columnNames: columns });
    this.relation.relType = { case: "toDf", value: toDf }
    return this;
  }
  withToSchema(schema: StructType, input?: Relation) {
    const toSchema = create(ToSchemaSchema, { input: input, schema: DataTypes.toProtoType(schema) });
    this.relation.relType = { case: "toSchema", value: toSchema }
    return this;
  }
  withShowString(numRows: number, truncate: number, vertical: boolean, input?: Relation) {
    const showString = create(ShowStringSchema, { numRows: numRows, truncate: truncate, vertical: vertical, input: input });
    this.relation.relType = { case: "showString", value: showString }
    return this;
  }
  withCatalog(catalog: Catalog) {
    this.relation.relType = { case: "catalog", value: catalog }
  }
  withCatalogBuilder(f: (builder: CatalogBuilder) => void) {
    const catalogBuilder = new CatalogBuilder();
    f(catalogBuilder);
    this.withCatalog(catalogBuilder.build());
  }
  build(): Relation {
    return this.relation;
  }
}