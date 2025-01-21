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

import { DataFrame } from "./DataFrame";
import { SparkSession } from "./SparkSession";
import { StructType } from "./types/StructType";
import { failIfHasCharVarchar } from "./types/utils";
import { CaseInsensitiveMap } from "./util/CaseInsensitiveMap";

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read()` to access this.
 *
 * @since 1.0.0
 * @author Kent Yao <yao@apache.org>
 * 
 * TODO: Some featuers are not implemented yet:
 *  - [ ] Add support of legacy char/varchar as string type for `schema` method
 *  - [ ] Add support of `json(jsonDataset: Dataset[string])` method
 *  - [ ] Add support of `csv(csvDataset: Dataset[string])` method
 *  - [ ] Add support of `xml(xmlDataset: Dataset[string])` method
 *  - [ ] Add support of `textFile(path: string, ...paths: string[]): Dataset[string]` method
 */
export class DataFrameReader {
  private source?: string = undefined;
  private userSpecifiedSchema?: string = undefined;
  private extraOptions = new CaseInsensitiveMap<string>();
  constructor(public spark: SparkSession) {}

  /**
   * Specifies the input data source format.
   *
   */
  format(source: string): DataFrameReader {
    this.source = source;
    return this;
  }

  schema(schema: StructType | string): DataFrameReader {
    if (typeof schema === "string") {
      this.userSpecifiedSchema = schema;
    } else {
      // TODO: support legacy char/varchar as string type
      failIfHasCharVarchar(schema);
      this.userSpecifiedSchema = schema.toDDL();
    }
    return this;
  }

  option(key: string, value: string | number | boolean): DataFrameReader {
    this.extraOptions.set(key, value.toString());
    return this;
  }

  options(opts: {[key: string]: string }): DataFrameReader {
    for (const key in opts) {
      this.option(key, opts[key]);
    }
    return this;
  }

  /**
   * Loads input in as a DataFrame, for data sources that require a path (e.g. local or distributed file system).
   * 
   */
  load(paths: string[] | string = []): DataFrame {
    if (typeof paths === "string") {
      paths = [paths];
    }
    return this.loadInternal(paths);
  }

  jdbc(
      url: string,
      table: string,
      columnName: string | undefined = undefined,
      lowerBound: number | undefined = undefined,
      upperBound: number | undefined = undefined,
      numPartitions: number | undefined = undefined,
      predicates: string[] = [],
      properties: {[key: string]: string}): DataFrame {
    if (columnName) {
      properties["partitionColumn"] = columnName;
    }
    if (lowerBound) {
      properties["lowerBound"] = lowerBound.toString();
    }
    if (upperBound) {
      properties["upperBound"] = upperBound.toString();
    }
    if (numPartitions) {
      properties["numPartitions"] = numPartitions.toString();
    }

    return this.format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .options(properties)
      .loadInternal([], predicates);
  }

  json(path: string, ...paths: string[]): DataFrame {
    return this.format("json").loadInternal([path, ...paths]);
  }

  csv(path: string, ...paths: string[]): DataFrame {
    return this.format("csv").loadInternal([path, ...paths]);
  }

  xml(path: string, ...paths: string[]): DataFrame {
    return this.format("xml").loadInternal([path, ...paths]);
  }

  parquet(path: string, ...paths: string[]): DataFrame {
    return this.format("parquet").loadInternal([path, ...paths]);
  }

  orc(path: string, ...paths: string[]): DataFrame {
    return this.format("orc").loadInternal([path, ...paths]);
  }

  text(path: string, ...paths: string[]): DataFrame {
    return this.format("text").loadInternal([path, ...paths]);
  }

  table(tableName: string): DataFrame {
    return this.spark.relationBuilderToDF(b => b.withReadTable(tableName, this.extraOptions));
  }
    
  /**
   * Loads input in as a DataFrame, for data sources that require a path (e.g. local or distributed file system).
   * 
   */
  private loadInternal(paths: string[] = [], predicates: string[] = []): DataFrame {
    return this.spark.relationBuilderToDF(b => b.withReadDataSource(this.source, this.userSpecifiedSchema, paths, predicates, this.extraOptions));
  }
}