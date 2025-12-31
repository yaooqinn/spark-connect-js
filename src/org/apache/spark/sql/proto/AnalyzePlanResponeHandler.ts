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

import { AnalyzePlanResponse, AnalyzePlanResponse_Explain, AnalyzePlanResponse_GetStorageLevel, AnalyzePlanResponse_InputFiles, AnalyzePlanResponse_IsLocal, AnalyzePlanResponse_IsStreaming, AnalyzePlanResponse_SameSemantics, AnalyzePlanResponse_Schema, AnalyzePlanResponse_SemanticHash, AnalyzePlanResponse_SparkVersion, AnalyzePlanResponse_TreeString } from "../../../../../gen/spark/connect/base_pb";
import { DataType } from "../../../../../gen/spark/connect/types_pb";
import { StorageLevel } from "../../storage/StorageLevel";

/**
 * Discriminated union type for AnalyzePlanResponse result types.
 * 
 * @remarks
 * This type ensures type safety when handling different analysis result types from Spark Connect.
 * Each variant represents a specific analysis operation result.
 */
type AnalyzeResultType = 
  | { case: "schema"; value: AnalyzePlanResponse_Schema }
  | { case: "explain"; value: AnalyzePlanResponse_Explain }
  | { case: "treeString"; value: AnalyzePlanResponse_TreeString }
  | { case: "isLocal"; value: AnalyzePlanResponse_IsLocal }
  | { case: "isStreaming"; value: AnalyzePlanResponse_IsStreaming }
  | { case: "inputFiles"; value: AnalyzePlanResponse_InputFiles }
  | { case: "sparkVersion"; value: AnalyzePlanResponse_SparkVersion }
  | { case: "ddlParse"; value: unknown }
  | { case: "sameSemantics"; value: AnalyzePlanResponse_SameSemantics }
  | { case: "semanticHash"; value: AnalyzePlanResponse_SemanticHash }
  | { case: "persist"; value: unknown }
  | { case: "unpersist"; value: unknown }
  | { case: "getStorageLevel"; value: AnalyzePlanResponse_GetStorageLevel }
  | { case: undefined; value: undefined };

/**
 * Handles responses from AnalyzePlanRequest operations.
 * 
 * @remarks
 * This class wraps AnalyzePlanResponse and provides type-safe access to different
 * analysis result types using discriminated unions. It validates the result type before
 * accessing specific values.
 * 
 * @example
 * ```typescript
 * const handler = new AnalyzePlanResponseHandler(response);
 * const schema = handler.schema; // Type-safe access
 * const explain = handler.explain; // Throws if not explain result
 * ```
 * 
 * @group internal
 */
export class AnalyzePlanResponseHandler {
  private readonly resultType: string | undefined;
  private readonly resultValue: AnalyzeResultType;
  
  constructor(public readonly response: AnalyzePlanResponse) {
    this.resultType = response.result.case;
    this.resultValue = response.result as AnalyzeResultType;
  }

  /**
   * Returns the session ID for this response.
   * 
   * @returns The session identifier
   * @group core
   */
  get sessionId(): string {
    return this.response.sessionId;
  }

  /**
   * Returns the server-side session ID.
   * 
   * @returns The server-side session identifier
   * @group core
   */
  get serverSideSessionId(): string {
    return this.response.serverSideSessionId;
  }

  /**
   * Returns the raw result structure.
   * 
   * @returns The result discriminated union
   * @group core
   */
  get result(): AnalyzeResultType {
    return this.resultValue;
  }

  /**
   * Returns the schema data type from the analysis result.
   * 
   * @returns The DataType representing the schema
   * @throws Error if the result type is not schema or if schema is undefined
   * @group result
   */
  get schema(): DataType {
    if (this.resultValue.case !== "schema") {
      throw this.unexpectedType("schema");
    }
    const schema = this.resultValue.value.schema;
    if(!schema) {
      throw new Error("Schema is not defined");
    } else {
      return schema;
    }
  }

  /**
   * Returns the explain string from the analysis result.
   * 
   * @returns The explain plan string
   * @throws Error if the result type is not explain
   * @group result
   */
  get explain(): string {
    if (this.resultValue.case !== "explain") {
      throw this.unexpectedType("explain");
    }
    return this.resultValue.value.explainString;
  }

  /**
   * Returns the tree string representation of the plan.
   * 
   * @returns The tree structure as a string
   * @throws Error if the result type is not treeString
   * @group result
   */
  get treeString(): string {
    if (this.resultValue.case !== "treeString") {
      throw this.unexpectedType("treeString");
    }
    return this.resultValue.value.treeString;
  }

  /**
   * Returns whether the DataFrame can be executed locally.
   * 
   * @returns True if the DataFrame can run locally
   * @throws Error if the result type is not isLocal
   * @group result
   */
  get isLocal(): boolean {
    if (this.resultValue.case !== "isLocal") {
      throw this.unexpectedType("isLocal");
    }
    return this.resultValue.value.isLocal;
  }

  /**
   * Returns whether the DataFrame is a streaming DataFrame.
   * 
   * @returns True if this is a streaming DataFrame
   * @throws Error if the result type is not isStreaming
   * @group result
   */
  get isStreaming(): boolean {
    if (this.resultValue.case !== "isStreaming") {
      throw this.unexpectedType("isStreaming");
    }
    return this.resultValue.value.isStreaming;
  }

  /**
   * Returns the Spark version string.
   * 
   * @returns The Spark version
   * @throws Error if the result type is not sparkVersion
   * @group result
   */
  get version(): string {
    if (this.resultValue.case !== "sparkVersion") {
      throw this.unexpectedType("sparkVersion");
    }
    return this.resultValue.value.version;
  }

  /**
   * Returns the list of input files for the DataFrame.
   * 
   * @returns Array of input file paths
   * @throws Error if the result type is not inputFiles
   * @group result
   */
  get inputFiles(): string[] {
    if (this.resultValue.case !== "inputFiles") {
      throw this.unexpectedType("inputFiles");
    }
    return this.resultValue.value.files;
  }

  /**
   * Returns whether two DataFrames have the same semantics.
   * 
   * @returns True if the DataFrames have the same semantics
   * @throws Error if the result type is not sameSemantics
   * @group result
   */
  get sameSemantics(): boolean {
    if (this.resultValue.case !== "sameSemantics") {
      throw this.unexpectedType("sameSemantics");
    }
    return this.resultValue.value.result;
  }

  /**
   * Returns the semantic hash of the DataFrame.
   * 
   * @returns The semantic hash value
   * @throws Error if the result type is not semanticHash
   * @group result
   */
  get semanticHash(): number {
    if (this.resultValue.case !== "semanticHash") {
      throw this.unexpectedType("semanticHash");
    }
    return this.resultValue.value.result;
  }

  /**
   * Returns the result of a persist operation.
   * 
   * @returns True if persist was successful
   * @throws Error if the result type is not persist
   * @group result
   */
  get persist(): boolean {
    if (this.resultValue.case !== "persist") {
      throw this.unexpectedType("persist");
    }
    return true;
  }

  /**
   * Returns the result of an unpersist operation.
   * 
   * @returns True if unpersist was successful
   * @throws Error if the result type is not unpersist
   * @group result
   */
  get unpersist(): boolean {
    if (this.resultValue.case !== "unpersist") {
      throw this.unexpectedType("unpersist");
    }
    return true;
  }

  /**
   * Returns the storage level of the DataFrame.
   * 
   * @returns The StorageLevel configuration
   * @throws Error if the result type is not getStorageLevel
   * @group result
   */
  get getStorageLevel(): StorageLevel {
    if (this.resultValue.case !== "getStorageLevel") {
      throw this.unexpectedType("getStorageLevel");
    }
    const storageLevel = this.resultValue.value.storageLevel;
    if (!storageLevel) {
      return StorageLevel.NONE;
    } 
    return new StorageLevel(storageLevel.useDisk, storageLevel.useMemory, storageLevel.useOffHeap, storageLevel.deserialized, storageLevel.replication);
  }

  private unexpectedType(typeName: string) {
    if (this.resultType !== typeName) {
      throw new Error(`Expected ${typeName} but got ${this.resultType}`);
    }
  }
}
