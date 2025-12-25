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
import { CacheTableSchema, Catalog, CatalogSchema, ClearCacheSchema, CreateExternalTableSchema, CreateTableSchema, CurrentCatalogSchema, CurrentDatabaseSchema, DatabaseExistsSchema, DropGlobalTempViewSchema, DropTempViewSchema, FunctionExistsSchema, GetDatabaseSchema, GetFunctionSchema, GetTableSchema, IsCachedSchema, ListCatalogsSchema, ListColumnsSchema, ListDatabasesSchema, ListFunctionsSchema, ListTablesSchema, RecoverPartitionsSchema, RefreshByPathSchema, RefreshTableSchema, SetCurrentCatalogSchema, SetCurrentDatabaseSchema, TableExistsSchema, UncacheTableSchema } from "../../../../../gen/spark/connect/catalog_pb";
import { StructType } from "../types/StructType";
import { DataTypes } from "../types";
import { mapToIndexSignature } from "../util/helpers";
import { StorageLevel } from "../../storage/StorageLevel";
import { createStorageLevelPB } from "./ProtoUtils";

export class CatalogBuilder {
  private catalog: Catalog = create(CatalogSchema, {});
  constructor() { }

  withCurrentDatabase() {
    const currentDatabase = create(CurrentDatabaseSchema, { });
    this.catalog.catType = { case: "currentDatabase", value: currentDatabase };
    return this;
  }
  withSetCurrentDatabase(dbName: string) {
    const setCurrentDatabase = create(SetCurrentDatabaseSchema, { dbName: dbName });
    this.catalog.catType = { case: "setCurrentDatabase", value: setCurrentDatabase };
    return this;
  }
  withListDatabases(pattern?: string) {
    const listDatabases = create(ListDatabasesSchema, { pattern: pattern });
    this.catalog.catType = { case: "listDatabases", value: listDatabases };
    return this;
  }
  withListTables(dbName?: string, pattern?: string) {
    const listTables = create(ListTablesSchema, { pattern: pattern, dbName: dbName });
    this.catalog.catType = { case: "listTables", value: listTables };
    return this;
  }
  withListFunctions(dbName?: string, pattern?: string) {
    const listFunctions = create(ListFunctionsSchema, { pattern: pattern, dbName: dbName });
    this.catalog.catType = { case: "listFunctions", value: listFunctions };
    return this;
  }
  withListColumns(dbName: string | undefined, tableName: string) {
    const listColumns = create(ListColumnsSchema, { dbName: dbName, tableName: tableName });
    this.catalog.catType = { case: "listColumns", value: listColumns };
    return this;
  }
  withGetDatabase(dbName: string) {
    const getDatabase = create(GetDatabaseSchema, {dbName: dbName});
    this.catalog.catType = { case: "getDatabase", value: getDatabase };
    return this;
  }
  withGetTable(dbName: string | undefined, tableName: string) {
    const getTable = create(GetTableSchema, {dbName: dbName, tableName: tableName});
    this.catalog.catType = { case: "getTable", value: getTable };
    return this;
  }
  withGetFunction(dbName: string | undefined, functionName: string) {
    const getFunction = create(GetFunctionSchema, {dbName: dbName, functionName: functionName});
    this.catalog.catType = { case: "getFunction", value: getFunction };
    return this;
  }
  withDatabaseExists(dbName: string) {
    const databaseExists = create(DatabaseExistsSchema, {dbName: dbName});
    this.catalog.catType = { case: "databaseExists", value: databaseExists };
    return this;
  }
  withTableExists(dbName: string | undefined, tableName: string) {
    const tableExists = create(TableExistsSchema, {dbName: dbName, tableName: tableName});
    this.catalog.catType = { case: "tableExists", value: tableExists };
    return this;
  }
  withFunctionExists(dbName: string | undefined, functionName: string) {
    const functionExists = create(FunctionExistsSchema, {dbName: dbName, functionName: functionName});
    this.catalog.catType = { case: "functionExists", value: functionExists };
    return this;
  }
  withCreateExternalTable(
      tableName: string,
      path?: string,
      source?: string,
      schema?: StructType,
      options?: Map<string, string>) {
    const createExternalTable = create(CreateExternalTableSchema,
      {
        tableName: tableName,
        path: path,
        source: source,
        schema: schema ? DataTypes.toProtoType(schema) : undefined,
        options: options ? mapToIndexSignature(options) : {}
      });
    this.catalog.catType = { case: "createExternalTable", value: createExternalTable };
    return this;
  }
  withCreateTable(
      tableName: string,
      path?: string,
      source?: string,
      description?: string,
      schema?: StructType,
      options?: Map<string, string>) {
    const createTable = create(CreateTableSchema,
      {
        tableName: tableName,
        path: path,
        source: source,
        description: description,
        schema: schema ? DataTypes.toProtoType(schema) : undefined,
        options: options ? mapToIndexSignature(options) : {}
      });
    this.catalog.catType = { case: "createTable", value: createTable };
    return this;
  }
  withDropTempView(viewName: string) {
    const dropTempView = create(DropTempViewSchema, { viewName: viewName });
    this.catalog.catType = { case: "dropTempView", value: dropTempView };
    return this;
  }
  withDropGlobalTempView(viewName: string) {
    const dropGlobalTempView = create(DropGlobalTempViewSchema, { viewName: viewName });
    this.catalog.catType = { case: "dropGlobalTempView", value: dropGlobalTempView };
    return this;
  }
  withRecoverPartitions(tableName?: string) {
    const recoverPartitions = create(RecoverPartitionsSchema, { tableName: tableName });
    this.catalog.catType = { case: "recoverPartitions", value: recoverPartitions };
    return this;
  }
  withIsCached(tableName: string) {
    const isCached = create(IsCachedSchema, { tableName: tableName });
    this.catalog.catType = { case: "isCached", value: isCached };
    return this;
  }
  withCacheTable(tableName: string, storageLevel?: StorageLevel) {
    const level = storageLevel ? createStorageLevelPB(storageLevel) : undefined;
    const cacheTable = create(CacheTableSchema, { tableName: tableName, storageLevel: level });
    this.catalog.catType = { case: "cacheTable", value: cacheTable };
    return this;
  }
  withUncacheTable(tableName: string) {
    const uncacheTable = create(UncacheTableSchema, { tableName: tableName });
    this.catalog.catType = { case: "uncacheTable", value: uncacheTable };
    return this;
  }
  withClearCache() {
    const clearCache = create(ClearCacheSchema, { });
    this.catalog.catType = { case: "clearCache", value: clearCache };
    return this;
  }
  withRefreshTable(tableName: string) {
    const refreshTable = create(RefreshTableSchema, { tableName: tableName });
    this.catalog.catType = { case: "refreshTable", value: refreshTable };
    return this;
  }
  withRefreshByPath(path: string) {
    const refreshByPath = create(RefreshByPathSchema, { path: path });
    this.catalog.catType = { case: "refreshByPath", value: refreshByPath };
    return this;
  }
  withCurrentCatalog() {
    const currentCatalog = create(CurrentCatalogSchema, { });
    this.catalog.catType = { case: "currentCatalog", value: currentCatalog };
    return this;
  }
  withSetCurrentCatalog(catalogName: string) {
    const setCurrentCatalog = create(SetCurrentCatalogSchema, { catalogName: catalogName });
    this.catalog.catType = { case: "setCurrentCatalog", value: setCurrentCatalog };
    return this;
  }
  withListCatalogs(pattern?: string) {
    const listCatalogs = create(ListCatalogsSchema, { pattern: pattern });
    this.catalog.catType = { case: "listCatalogs", value: listCatalogs };
    return this;
  }
  build(): Catalog {
    return this.catalog;
  }
}