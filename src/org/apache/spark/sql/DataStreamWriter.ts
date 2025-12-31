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
import * as c from "../../../../gen/spark/connect/commands_pb";
import { DataFrame } from "./DataFrame";
import { StreamingQuery } from "./StreamingQuery";
import { CaseInsensitiveMap } from "./util/CaseInsensitiveMap";

/**
 * Interface used to write a streaming [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `DataFrame.writeStream` to access this.
 * 
 * @since 1.0.0
 */
export class DataStreamWriter {
  private source_?: string = undefined;
  private extraOptions_ = new CaseInsensitiveMap<string>();
  private partitioningColumns_: string[] = [];
  private clusteringColumns_: string[] = [];
  private outputMode_: string = "append";
  private queryName_?: string = undefined;
  private trigger_?: 
    | { case: "processingTimeInterval", value: string }
    | { case: "availableNow", value: boolean }
    | { case: "once", value: boolean }
    | { case: "continuousCheckpointInterval", value: string }
    = undefined;

  constructor(public df: DataFrame) {}

  /**
   * Specifies the underlying output data source.
   */
  format(source: string): DataStreamWriter {
    this.source_ = source;
    return this;
  }

  /**
   * Adds an output option for the underlying data source.
   */
  option(key: string, value: string | number | boolean): DataStreamWriter {
    this.extraOptions_.set(key, value.toString());
    return this;
  }

  /**
   * Adds output options for the underlying data source.
   */
  options(opts: {[key: string]: string }): DataStreamWriter {
    for (const key in opts) {
      this.option(key, opts[key]);
    }
    return this;
  }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   * 
   * @param mode can be "append", "complete", or "update"
   */
  outputMode(mode: "append" | "complete" | "update"): DataStreamWriter {
    this.outputMode_ = mode;
    return this;
  }

  /**
   * Specifies the name of the StreamingQuery that can be started with start().
   */
  queryName(name: string): DataStreamWriter {
    this.queryName_ = name;
    return this;
  }

  /**
   * Set the trigger for the stream query. If not set, it will run the query in micro-batch mode.
   * 
   * @param condition The trigger condition
   */
  trigger(condition: TriggerCondition): DataStreamWriter {
    if ("processingTime" in condition) {
      this.trigger_ = { case: "processingTimeInterval", value: condition.processingTime };
    } else if ("once" in condition) {
      this.trigger_ = { case: "once", value: condition.once };
    } else if ("availableNow" in condition) {
      this.trigger_ = { case: "availableNow", value: condition.availableNow };
    } else if ("continuous" in condition) {
      this.trigger_ = { case: "continuousCheckpointInterval", value: condition.continuous };
    }
    return this;
  }

  /**
   * Partitions the output by the given columns on the file system.
   */
  partitionBy(...cols: string[]): DataStreamWriter {
    this.partitioningColumns_ = cols;
    return this;
  }

  /**
   * Clusters the output by the given columns on the storage.
   */
  clusterBy(...cols: string[]): DataStreamWriter {
    this.clusteringColumns_ = cols;
    return this;
  }

  /**
   * Starts the execution of the streaming query.
   * 
   * @param path Optional path for file-based sinks
   */
  async start(path?: string): Promise<StreamingQuery> {
    const write = create(c.WriteStreamOperationStartSchema, {
      input: this.df.plan.relation,
      format: this.source_ || "",
      options: this.extraOptions_.toIndexSignature(),
      partitioningColumnNames: this.partitioningColumns_,
      clusteringColumnNames: this.clusteringColumns_,
      outputMode: this.outputMode_,
      queryName: this.queryName_ || ""
    });

    if (path) {
      write.sinkDestination = { case: "path", value: path };
    }

    if (this.trigger_) {
      write.trigger = this.trigger_;
    }

    const command = create(c.CommandSchema, {
      commandType: { case: "writeStreamOperationStart", value: write }
    });

    const resps = await this.df.spark.execute(command);
    const resp = resps.filter(r => r.isWriteStreamOperationStartResult)[0];
    
    if (resp && resp.writeStreamOperationStartResult) {
      const result = resp.writeStreamOperationStartResult;
      return new StreamingQuery(
        this.df.spark,
        result.queryId?.id || "",
        result.queryId?.runId || "",
        result.name
      );
    }
    
    throw new Error("Failed to start streaming query");
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given table.
   */
  async toTable(tableName: string): Promise<StreamingQuery> {
    const write = create(c.WriteStreamOperationStartSchema, {
      input: this.df.plan.relation,
      format: this.source_ || "",
      options: this.extraOptions_.toIndexSignature(),
      partitioningColumnNames: this.partitioningColumns_,
      clusteringColumnNames: this.clusteringColumns_,
      outputMode: this.outputMode_,
      queryName: this.queryName_ || "",
      sinkDestination: { case: "tableName", value: tableName }
    });

    if (this.trigger_) {
      write.trigger = this.trigger_;
    }

    const command = create(c.CommandSchema, {
      commandType: { case: "writeStreamOperationStart", value: write }
    });

    const resps = await this.df.spark.execute(command);
    const resp = resps.filter(r => r.isWriteStreamOperationStartResult)[0];
    
    if (resp && resp.writeStreamOperationStartResult) {
      const result = resp.writeStreamOperationStartResult;
      return new StreamingQuery(
        this.df.spark,
        result.queryId?.id || "",
        result.queryId?.runId || "",
        result.name
      );
    }
    
    throw new Error("Failed to start streaming query");
  }
}

/**
 * Defines the trigger condition for a streaming query.
 */
export type TriggerCondition = 
  | { processingTime: string }
  | { once: boolean }
  | { availableNow: boolean }
  | { continuous: string };
