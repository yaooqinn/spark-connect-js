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

import { StorageLevel } from '../../storage/StorageLevel';
import { DataFrame } from '../DataFrame';
import { CatalogBuilder } from '../proto/CatalogBuilder';
import { Row } from '../Row';
import { SparkSession } from '../SparkSession';
import { DataTypes } from '../types';
import { StructType } from '../types/StructType';
import { Database } from './Database';
import { CatalogFunction } from './Function';
import { Table } from './Table';

/**
 * Catalog interface for Spark. To access this, use `SparkSession.catalog`.
 * 
 * @since 1.0.0
 * @author Kent Yao <yao@apache.org>
 * @example
 * ```typescript
 *   const spark = await SparkSession.builder().appName("catalog").getOrCreate();
 *   const catalog = spark.catalog;
 *   const currentDatabase = await catalog.currentDatabase();
 *   console.log(currentDatabase);
 * ```
 */
export class Catalog {
  constructor(
    public readonly spark: SparkSession) { }

  /**
   * Returns the current database (namespace) in this session.
   * @async
   * @returns {Promise<string>} A Promise that resolves with the current database name.
   */
  async currentDatabase(): Promise<string> {
    return this.catalogBuilderToDF(c => c.withCurrentDatabase()).head().then(row => row.getString(0));
  }

  /**
   * Sets the current database (namespace) in this session.
   * @async
   * @param {string} dbName The database name to set as the current database.
   * @returns {Promise<void>} A Promise that resolves with no value.
   */
  async setCurrentDatabase(dbName: string): Promise<void> {
    const plan = this.spark.planFromRelationBuilder(b => b.withCatalogBuilder(c => c.withSetCurrentDatabase(dbName)));
    return this.spark.client.execute(plan).then(() => {});
  }
  /**
   * Returns a list of databases (namespaces) available within the current catalog.
   *
   * @returns {DataFrame} A DataFrame with a single string column named "namespace".
   */
  listDatabases(): DataFrame;
  /**
   * Returns a list of databases (namespaces) which name match the specify pattern and available
   * within the current catalog.
   *
   * @param {string} pattern The pattern to match against the database names.
   * @returns {DataFrame} A DataFrame with a single string column named "namespace".
   */
  listDatabases(pattern: string): DataFrame;
  /** @ignore */
  listDatabases(pattern?: string): DataFrame {
    return this.catalogBuilderToDF(c => c.withListDatabases(pattern));
  }

  /**
   * Returns a list of tables/views in the current database (namespace). This includes all
   * temporary views.
   *
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the table/view.
   *   - catalog: The name of the catalog that the table/view belongs to.
   *   - namespace: The namespace (database) that the table/view belongs to.
   *   - description: The description of the table/view.
   *   - tableType: The type of the table
   *   - isTemporary: Whether the table/view is temporary or not.
   */
  listTables(): DataFrame;
  /**
   * Returns a list of tables/views in the specified database (namespace). This includes all
   * temporary views.
   *
   * @param {string} dbName The name of the database to list tables/views from.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the table/view.
   *   - catalog: The name of the catalog that the table/view belongs to.
   *   - namespace: The namespace (database) that the table/view belongs to.
   *   - description: The description of the table/view.
   *    tableType: The type of the table
   *   - isTemporary: Whether the table/view is temporary or not.
   */
  listTables(dbName: string): DataFrame;
  /**
   * Returns a list of tables/views in the specified database (namespace) which name match the
   * specified pattern. This includes all temporary views.
   *
   * @param {string} dbName The name of the database to list tables/views from.
   * @param {string} pattern The pattern to match against the table/view names.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the table/view.
   *   - catalog: The name of the catalog that the table/view belongs to.
   *   - namespace: The namespace (database) that the table/view belongs to.
   *   - description: The description of the table/view.
   *   - tableType: The type of the table
   *   - isTemporary: Whether the table/view is temporary or not.
   */
  listTables(dbName: string, pattern: string): DataFrame;
  /** @ignore */
  listTables(dbName?: string, pattern?: string): DataFrame {
    return this.catalogBuilderToDF(c => c.withListTables(dbName, pattern));
  }

  /**
   * Returns a list of functions registered in the current database (namespace). This includes all
   * temporary functions.
   * 
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the function.
   *   - catalog: The name of the catalog that the function belongs to.
   *   - namespace: The namespace (database) that the function belongs to.
   *   - description: The description of the function.
   *   - className: The class name of the function.
   *   - isTemporary: Whether the function is temporary or not.
   *
   */
  listFunctions(): DataFrame;
  /**
   * Returns a list of functions registered in the specified database (namespace). This includes all
   * temporary functions.
   * 
   * @param {string} dbName The name of the database to list functions from.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the function.
   *   - catalog: The name of the catalog that the function belongs to.
   *   - namespace: The namespace (database) that the function belongs to.
   *   - description: The description of the function.
   *   - className: The class name of the function.
   *   - isTemporary: Whether the function is temporary or not.
   */
  listFunctions(dbName: string): DataFrame;
  /**
   * Returns a list of functions registered in the specified database (namespace) which name match the
   * specified pattern. This includes all temporary functions.
   * 
   * @param {string} dbName The name of the database to list functions from.
   * @param {string} pattern The pattern to match against the function names.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *   - name: The name of the function.
   *   - catalog: The name of the catalog that the function belongs to.
   *   - namespace: The namespace (database) that the function belongs to.
   *   - description: The description of the function.
   *   - className: The class name of the function.
   *   - isTemporary: Whether the function is temporary or not.
   */
  listFunctions(dbName: string, pattern: string): DataFrame;
  /** @ignore */
  listFunctions(dbName?: string, pattern?: string): DataFrame {
    return this.catalogBuilderToDF(c => c.withListFunctions(dbName, pattern));
  }

    /**
   * Returns a list of columns for the given table/view or temporary view.
   *
   * @param {string} tableName The name of the table/view or temporary view to list columns from.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *  - name: The name of the column.
   *  - description: The description of the column.
   *  - dataType: The data type of the column.
   *  - nullable: Indicates if values of this column can be `null` values.
   *  - isPartition: Indicates if this column is a partition column.
   *  - isBucket: Indicates if this column is a bucket column.
   *  - isCluster: Indicates if this column is a clustering column.
   */
  listColumns(tableName: string): DataFrame;
  /**
   * Returns a list of columns for the given table/view or temporary view in the specified database
   * (namespace).
   *
   * @param {string} dbName The name of the database to list columns from.
   * @param {string} tableName The name of the table/view or temporary view to list columns from.
   * @returns {DataFrame} A DataFrame with the following string columns:
   *  - name: The name of the column.
   *  - description: The description of the column.
   *  - dataType: The data type of the column.
   *  - nullable: Indicates if values of this column can be `null` values.
   *  - isPartition: Indicates if this column is a partition column.
   *  - isBucket: Indicates if this column is a bucket column.
   *  - isCluster: Indicates if this column is a clustering column.
   */
  listColumns(dbName: string, tableName: string): DataFrame;
  /** @ignore */
  listColumns(dbOrTableName: string, tableName?: string): DataFrame {
    if (tableName === undefined) {
      return this.catalogBuilderToDF(c => c.withListColumns(undefined, dbOrTableName));
    } else  {
      return this.catalogBuilderToDF(c => c.withListColumns(dbOrTableName, tableName));
    }
  }

  /**
   * Get the database (namespace) with the specified name (can be qualified with catalog). This
   * throws an AnalysisException when the database (namespace) cannot be found.
   *
   * @param {string} dbName The name of the database to get.
   * @returns {Promise<Database>} A Promise that resolves with the database.
   */
  async getDatabase(dbName: string): Promise<Database> {
    const row = await this.catalogBuilderToDF(c => c.withGetDatabase(dbName)).head();
    return new Database(row[0], row[1], row[2], row[3]);
  }

  /**
   * Get the table or view with the specified name in the specified database under the Hive
   * Metastore. This throws an AnalysisException when no Table can be found.
   *
   * To get table/view in other catalogs, please use `getTable(tableName)` with qualified
   * table/view name instead.
   *
   * @param {string} tableName The name of the table/view to get.
   * @returns {Promise<Table>} A Promise that resolves with the table/view.
   */
  async getTable(tableName: string): Promise<Table>;
  /**
   * Get the table or view with the specified name in the specified database under the Hive
   * Metastore. This throws an AnalysisException when no Table can be found.
   *
   * To get table/view in other catalogs, please use `getTable(dbName, tableName)` with qualified
   * table/view name instead.
   *
   * @param {string} dbName The name of the database to get the table/view from.
   * @param {string} tableName The name of the table/view to get.
   * @returns {Promise<Table>} A Promise that resolves with the table/view.
   */
  async getTable(dbName: string, tableName: string): Promise<Table>;
  /** @ignore */
  async getTable(dbOrTableName: string, tableName?: string): Promise<Table> {
    if (tableName === undefined) {
      return this.catalogBuilderToRow(c => c.withGetTable(undefined, dbOrTableName)).then(row =>
        new Table(row[0], row[1], row[2], row[3], row[4], row[5]));
    } else  {
      return this.catalogBuilderToRow(c => c.withGetTable(dbOrTableName, tableName)).then(row =>
        new Table(row[0], row[1], row[2], row[3], row[4], row[5]));
    }
  }

    /**
   * Get the function with the specified name. This function can be a temporary function or a
   * function. This throws an AnalysisException when the function cannot be found.
   *
   * @param {string} functionName The name of the function to get.
   * @returns {Promise<CatalogFunction>} A Promise that resolves with the function.
   */
  async getFunction(functionName: string): Promise<CatalogFunction>;
  /**
   * Get the function with the specified name in the specified database. This function can be a
   * temporary function or a function. This throws an AnalysisException when the function cannot be
   * found.
   *
   * @param {string} dbName The name of the database to get the function from.
   * @param {string} functionName The name of the function to get.
   * @returns {Promise<CatalogFunction>} A Promise that resolves with the function.
   */
  async getFunction(dbName: string, functionName: string): Promise<CatalogFunction>;
  /** @ignore */
  async getFunction(dbOrFunctionName: string, functionName?: string): Promise<CatalogFunction> {
    if (functionName === undefined) {
      return this.catalogBuilderToRow(c => c.withGetFunction(undefined, dbOrFunctionName)).then(row =>
        new CatalogFunction(row[0], row[1], row[2], row[3], row[4], row[5]));
    } else  {
      return this.catalogBuilderToRow(c => c.withGetFunction(dbOrFunctionName, functionName)).then(row =>
        new CatalogFunction(row[0], row[1], row[2], row[3], row[4], row[5]));
    }
  }

  /**
   * Check if the database with the specified name exists.
   *
   * @param {string} dbName The name of the database to check.
   * @returns {Promise<boolean>} A Promise that resolves with a boolean indicating if the database
   * exists.
   */
  async databaseExists(dbName: string): Promise<boolean> {
    return this.catalogBuilderToRow(c => c.withDatabaseExists(dbName)).then(row => row.getBoolean(0));
  }

  /**
   * Check if the table or view with the specified name exists in the specified database.
   *
   * @param {string} tableName The name of the table/view to check.
   * @returns {Promise<boolean>} A Promise that resolves with a boolean indicating if the table/view
   * exists.
   */
  async tableExists(tableName: string): Promise<boolean>;
  /**
   * Check if the table or view with the specified name exists in the specified database.
   *
   * @param {string} dbName The name of the database to check.
   * @param {string} tableName The name of the table/view to check.
   * @returns {Promise<boolean>} A Promise that resolves with a boolean indicating if the table/view
   * exists.
   */
  async tableExists(dbName: string, tableName: string): Promise<boolean>;
  /** @ignore */
  async tableExists(dbOrTableName: string, tableName?: string): Promise<boolean> {
    if (tableName === undefined) {
      return this.catalogBuilderToRow(c => c.withTableExists(undefined, dbOrTableName)).then(row => row.getBoolean(0));
    } else  {
      return this.catalogBuilderToRow(c => c.withTableExists(dbOrTableName, tableName)).then(row => row.getBoolean(0));
    }
  }
 
  /**
   * Check if the function with the specified name exists.
   *
   * @param {string} functionName The name of the function to check.
   * @returns {Promise<boolean>} A Promise that resolves with a boolean indicating if the function
   * exists.
   */
  async functionExists(functionName: string): Promise<boolean>;
  /**
   * Check if the function with the specified name exists in the specified database.
   *
   * @param {string} dbName The name of the database to check.
   * @param {string} functionName The name of the function to check.
   * @returns {Promise<boolean>} A Promise that resolves with a boolean indicating if the function
   * exists.
   */
  async functionExists(dbName: string, functionName: string): Promise<boolean>;
  /** @ignore */
  async functionExists(dbOrFunctionName: string, functionName?: string): Promise<boolean> {
    if (functionName === undefined) {
      return this.catalogBuilderToRow(c => c.withFunctionExists(undefined, dbOrFunctionName)).then(row => row.getBoolean(0));
    } else  {
      return this.catalogBuilderToRow(c => c.withFunctionExists(dbOrFunctionName, functionName)).then(row => row.getBoolean(0));
    }
  }

  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame. It will use the efault data source configured by spark.sql.sources.default.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} path The path to the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, path: string): DataFrame;
  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} path The path to the data source.
   * @param {string} source The name of the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, path: string, source: string): DataFrame;
  /**
   * Creates a table based on the dataset in a data source and a set of options. Then, returns the
   * corresponding DataFrame.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} source The name of the data source.
   * @param {Map<string, string>} options The options for the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, source: string, options: Map<string, string>): DataFrame;
  /**
   * Creates a table based on the dataset in a data source and a schema. Then, returns the
   * corresponding DataFrame.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} source The name of the data source.
   * @param {string} description The description of the table.
   * @param {Map<string, string>} options The options for the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, source: string, description: string, options: Map<string, string>): DataFrame;
  /**
   * Creates a table based on the dataset in a data source, a schema, and a set of options. Then,
   * returns the corresponding DataFrame.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} source The name of the data source.
   * @param {StructType} schema The schema of the table.
   * @param {Map<string, string>} options The options for the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, source: string, schema: StructType, options: Map<string, string>): DataFrame;
  /**
   * Creates a table based on the dataset in a data source, a schema, a description, and a set of
   * options. Then, returns the corresponding DataFrame.
   *
   * @param {string} tableName The name of the table to create.
   * @param {string} source The name of the data source.
   * @param {StructType} schema The schema of the table.
   * @param {string} description The description of the table.
   * @param {Map<string, string>} options The options for the data source.
   * @returns {DataFrame} A DataFrame representing the table.
   */
  createTable(tableName: string, source: string, schema: StructType, description: string, options: Map<string, string>): DataFrame;
  /** @ignore */
  createTable(...args: any[]): DataFrame {
    let path: string | undefined = undefined;
    let source: string | undefined = undefined;
    let description: string | undefined = undefined;
    let schema: StructType | undefined = undefined;
    let options: Map<string, string> | undefined = undefined;
    if (args.length === 2) {
      path = args[1];
    } else if (args.length === 3) {
      if (typeof args[2] === 'string') {
        path = args[1];
        source = args[2];
      } else {
        source = args[1];
        schema = DataTypes.createStructType([]);
        options = args[2];
      }
    } else if (args.length === 4) {
      source = args[1];
      options = args[3];
      if (typeof args[2] === 'string') {
        description = args[2];
        schema = DataTypes.createStructType([]);
      } else {
        schema = args[2];
      }
    } else {
      source = args[1];
      schema = args[2];
      description = args[3];
      options = args[4];
    }
    return this.catalogBuilderToDF(c => c.withCreateTable(args[0], path, source, description, schema, options));
  }

  async dropTempView(viewName: string): Promise<boolean> {
    return this.catalogBuilderToRow(c => c.withDropTempView(viewName)).then(row => row.getBoolean(0));
  }

  async dropGlobalTempView(viewName: string): Promise<boolean> {
    return this.catalogBuilderToRow(c => c.withDropGlobalTempView(viewName)).then(row => row.getBoolean(0));
  }


  async recoverPartitions(tableName?: string): Promise<void> {
    return this.catalogBuilderToRow(c => c.withRecoverPartitions(tableName)).then(() => {});
  }

  async isCached(tableName: string): Promise<boolean> {
    return this.catalogBuilderToRow(c => c.withIsCached(tableName)).then(row => row.getBoolean(0));
  }

  async cacheTable(tableName: string): Promise<void>;
  async cacheTable(tableName: string, storageLevel: StorageLevel): Promise<void>;
  /** @ignore */
  async cacheTable(tableName: string, storageLevel?: StorageLevel): Promise<void> {
    return this.catalogBuilderToRow(c => c.withCacheTable(tableName, storageLevel)).then(() => {});
  }

  async uncacheTable(tableName: string): Promise<void> {
    return this.catalogBuilderToRow(c => c.withUncacheTable(tableName)).then(() => {});
  }

  async clearCache(): Promise<void> {
    return this.catalogBuilderToRow(c => c.withClearCache()).then(() => {});
  }

  async refreshTable(tableName: string): Promise<void> {
    return this.catalogBuilderToRow(c => c.withRefreshTable(tableName)).then(() => {});
  }

  async refreshByPath(path: string): Promise<void> {
    return this.catalogBuilderToRow(c => c.withRefreshByPath(path)).then(() => {});
  }

  async currentCatalog(): Promise<string> {
    return this.catalogBuilderToRow(c => c.withCurrentCatalog()).then(row => row.getString(0));
  }

  async setCurrentCatalog(catalogName: string): Promise<void> {
    return this.catalogBuilderToRow(c => c.withSetCurrentCatalog(catalogName)).then(() => {});
  }

  listCatalogs(): DataFrame;
  listCatalogs(pattern: string): DataFrame;
  /** @ignore */
  listCatalogs(pattern?: string): DataFrame {
    return this.catalogBuilderToDF(c => c.withListCatalogs(pattern));
  }

  private catalogBuilderToDF(f: (builder: CatalogBuilder) => void): DataFrame {
    return this.spark.relationBuilderToDF(b =>b.withCatalogBuilder(f));
  }

  private catalogBuilderToRow(f: (builder: CatalogBuilder) => void): Promise<Row> {
    return this.spark.relationBuilderToDF(b =>b.withCatalogBuilder(f)).head();
  }
}
