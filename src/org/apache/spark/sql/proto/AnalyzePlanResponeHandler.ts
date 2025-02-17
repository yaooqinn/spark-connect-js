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

export class AnalyzePlanResponseHandler {
  private resultType: any;
  private resultValue: any;
  
  constructor(public readonly response: AnalyzePlanResponse) {
    this.resultType = response.result.case;
    this.resultValue = response.result.value;
  }

  get sessionId(): string {
    return this.response.sessionId;
  }

  get serverSideSessionId(): string {
    return this.response.serverSideSessionId;
  }

  get result(): any {
    return this.response.result;
  }

  get schema(): DataType {
    if (this.resultType !== "schema") {
      throw this.unexpectedType("schema");
    }
    const schema = (this.resultValue as AnalyzePlanResponse_Schema).schema;
    if(!schema) {
      throw new Error("Schema is not defined");
    } else {
      return schema;
    }
  }

  get explain(): string {
    if (this.resultType !== "explain") {
      throw this.unexpectedType("explain");
    }
    return (this.resultValue as AnalyzePlanResponse_Explain).explainString;
  }

  get treeString(): string {
    if (this.resultType !== "treeString") {
      throw this.unexpectedType("treeString");
    }
    return (this.resultValue as AnalyzePlanResponse_TreeString).treeString;
  }

  get isLocal(): boolean {
    if (this.resultType !== "isLocal") {
      throw this.unexpectedType("isLocal");
    }
    return (this.resultValue as AnalyzePlanResponse_IsLocal).isLocal;
  }

  get isStreaming(): boolean {
    if (this.resultType !== "isStreaming") {
      throw this.unexpectedType("isStreaming");
    }
    return (this.resultValue as AnalyzePlanResponse_IsStreaming).isStreaming;
  }

  get version(): string {
    if (this.resultType !== "sparkVersion") {
      throw this.unexpectedType("sparkVersion");
    }
    return (this.resultValue as AnalyzePlanResponse_SparkVersion).version;
  }

  get inputFiles(): string[] {
    if (this.resultType !== "inputFiles") {
      throw this.unexpectedType("inputFiles");
    }
    return (this.resultValue as AnalyzePlanResponse_InputFiles).files;
  }

  get sameSemantics(): boolean {
    if (this.resultType !== "sameSemantics") {
      throw this.unexpectedType("sameSemantics");
    }
    return (this.resultValue as AnalyzePlanResponse_SameSemantics).result;
  }

  get semanticHash(): number {
    if (this.resultType !== "semanticHash") {
      throw this.unexpectedType("semanticHash");
    }
    return (this.resultValue as AnalyzePlanResponse_SemanticHash).result;
  }

  get persist(): boolean {
    if (this.resultType !== "persist") {
      throw this.unexpectedType("persist");
    }
    return true;
  }

  get unpersist(): boolean {
    if (this.resultType !== "unpersist") {
      throw this.unexpectedType("unpersist");
    }
    return true;
  }

  get getStorageLevel(): StorageLevel {
    if (this.resultType !== "getStorageLevel") {
      throw this.unexpectedType("getStorageLevel");
    }
    const storageLevel = (this.resultValue as AnalyzePlanResponse_GetStorageLevel).storageLevel;
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
