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
import { Aggregate, FilterSchema, HintSchema, LimitSchema, LocalRelation, OffsetSchema, ProjectSchema, RangeSchema, Read, Read_DataSourceSchema, Read_NamedTableSchema, ReadSchema, Relation, RelationCommon, RelationSchema, ShowStringSchema, TailSchema, ToDFSchema, ToSchemaSchema, Unpivot_ValuesSchema, UnpivotSchema } from "../../../../../gen/spark/connect/relations_pb";
import { CaseInsensitiveMap } from "../util/CaseInsensitiveMap";
import { StructType } from "../types/StructType";
import { DataTypes } from "../types";
import { Catalog } from "../../../../../gen/spark/connect/catalog_pb";
import { CatalogBuilder } from "./CatalogBuilder";
import { Expression } from "../../../../../gen/spark/connect/expressions_pb";
import { lit } from "../functions";
import { AggregateBuilder } from "./aggregate/AggregateBuilder";
import { Column } from "../Column";

export class RelationBuilder {
  private relation: Relation = create(RelationSchema, {});
  constructor() {}
  withRelationCommon(common: RelationCommon) {
    this.relation.common = common;
    return this;
  }
  withProject(expressions: Expression[], input?: Relation) {
    const project = create(ProjectSchema, { input: input, expressions: expressions });
    this.relation.relType = { case: "project", value: project }
    return this;
  }
  withFilter(condition: Expression, input?: Relation) {
    const filter = create(FilterSchema, { input: input, condition: condition });
    this.relation.relType = { case: "filter", value: filter }
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
  withHint(name: string, parameters: any[], input?: Relation) {
    const hint = create(HintSchema, { name: name, parameters: parameters.map(p => lit(p).expr), input: input });
    this.relation.relType = { case: "hint", value: hint}
    return this;
  }
  withAggregate(aggregate: Aggregate) {
    this.relation.relType = { case: "aggregate", value: aggregate }
    return this;
  }
  withAggregateBuilder(f: (b: AggregateBuilder) => void) {
    const builder = new AggregateBuilder();
    f(builder);
    this.relation.relType = { case: "aggregate", value: builder.build() }
    return this;
  }
  withUnpivot(
      ids: Column[],
      variableColumnName: string,
      valueColumnName: string,
      values?: Column[],
      input?: Relation) {
    const unpivot = create(UnpivotSchema, {
      input: input,
      ids: ids.map(id => id.expr),
      values: values !== undefined ? create(Unpivot_ValuesSchema, { values: values.map(v => v.expr) }) : undefined,
      variableColumnName: variableColumnName,
      valueColumnName: valueColumnName
    });
    this.relation.relType = { case: "unpivot", value: unpivot }
    return this;
  }

  build(): Relation {
    return this.relation;
  }
}
