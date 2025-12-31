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
 * Interface used to load a streaming [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 1.0.0
 */
export class DataStreamReader {
  private source?: string = undefined;
  private userSpecifiedSchema?: string = undefined;
  private extraOptions = new CaseInsensitiveMap<string>();
  
  constructor(public spark: SparkSession) {}

  /**
   * Specifies the input data source format.
   */
  format(source: string): DataStreamReader {
    this.source = source;
    return this;
  }

  /**
   * Specifies the schema for the input data source.
   */
  schema(schema: StructType | string): DataStreamReader {
    if (typeof schema === "string") {
      this.userSpecifiedSchema = schema;
    } else {
      failIfHasCharVarchar(schema);
      this.userSpecifiedSchema = schema.toDDL();
    }
    return this;
  }

  /**
   * Adds an input option for the underlying data source.
   */
  option(key: string, value: string | number | boolean): DataStreamReader {
    this.extraOptions.set(key, value.toString());
    return this;
  }

  /**
   * Adds input options for the underlying data source.
   */
  options(opts: {[key: string]: string }): DataStreamReader {
    for (const key in opts) {
      this.option(key, opts[key]);
    }
    return this;
  }

  /**
   * Loads input as a streaming DataFrame.
   */
  load(path?: string): DataFrame {
    const paths = path ? [path] : [];
    return this.loadInternal(paths);
  }

  /**
   * Loads a JSON file stream and returns the results as a streaming DataFrame.
   */
  json(path: string): DataFrame {
    return this.format("json").loadInternal([path]);
  }

  /**
   * Loads a CSV file stream and returns the results as a streaming DataFrame.
   */
  csv(path: string): DataFrame {
    return this.format("csv").loadInternal([path]);
  }

  /**
   * Loads a Parquet file stream and returns the results as a streaming DataFrame.
   */
  parquet(path: string): DataFrame {
    return this.format("parquet").loadInternal([path]);
  }

  /**
   * Loads an ORC file stream and returns the results as a streaming DataFrame.
   */
  orc(path: string): DataFrame {
    return this.format("orc").loadInternal([path]);
  }

  /**
   * Loads a text file stream and returns the results as a streaming DataFrame.
   */
  text(path: string): DataFrame {
    return this.format("text").loadInternal([path]);
  }

  /**
   * Returns a streaming DataFrame representing the stream of a table.
   */
  table(tableName: string): DataFrame {
    return this.spark.relationBuilderToDF(b => b.withReadTable(tableName, this.extraOptions, true));
  }
    
  /**
   * Loads input as a streaming DataFrame.
   */
  private loadInternal(paths: string[] = []): DataFrame {
    return this.spark.relationBuilderToDF(b => b.withReadDataSource(this.source, this.userSpecifiedSchema, paths, [], this.extraOptions, true));
  }
}
