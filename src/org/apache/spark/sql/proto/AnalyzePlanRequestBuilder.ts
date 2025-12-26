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
import { AnalyzePlanRequest, AnalyzePlanRequest_DDLParseSchema, AnalyzePlanRequest_Explain_ExplainMode, AnalyzePlanRequest_ExplainSchema, AnalyzePlanRequest_GetStorageLevelSchema, AnalyzePlanRequest_InputFilesSchema, AnalyzePlanRequest_IsLocalSchema, AnalyzePlanRequest_IsStreamingSchema, AnalyzePlanRequest_PersistSchema, AnalyzePlanRequest_SameSemanticsSchema, AnalyzePlanRequest_SchemaSchema, AnalyzePlanRequest_SemanticHashSchema, AnalyzePlanRequest_SparkVersionSchema, AnalyzePlanRequest_TreeStringSchema, AnalyzePlanRequest_UnpersistSchema, AnalyzePlanRequestSchema, Plan, UserContext } from "../../../../../gen/spark/connect/base_pb";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";
import { StorageLevel } from "../../storage/StorageLevel";
import { createStorageLevelPB } from "./ProtoUtils";

export class AnalyzePlanRequestBuilder {
  private request: AnalyzePlanRequest = create(AnalyzePlanRequestSchema, {});
  constructor() {}
  sessionId(sessionId: string) {
    this.request.sessionId = sessionId;
    return this;
  }
  userContext(userContext: UserContext) {
    this.request.userContext = userContext;
    return this;
  }
  clientType(clientType: string) {
    this.request.clientType = clientType;
    return this;
  }
  setSchema(plan: Plan) {
    this.request.analyze = { case: "schema", value : create(AnalyzePlanRequest_SchemaSchema, { plan: plan }) };
    return this;
  }
  withExplain(plan: Plan): AnalyzePlanRequestBuilder;
  withExplain(plan: Plan, mode: boolean): AnalyzePlanRequestBuilder;
  withExplain(plan: Plan, mode: string): AnalyzePlanRequestBuilder;
  withExplain(plan: Plan, mode: boolean | string | undefined = undefined): AnalyzePlanRequestBuilder {
    const explain = create(AnalyzePlanRequest_ExplainSchema, { plan: plan });
    if (typeof mode === "undefined") {
      explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.SIMPLE;
    } else if (typeof mode === "boolean") {
      explain.explainMode = mode ? AnalyzePlanRequest_Explain_ExplainMode.EXTENDED : AnalyzePlanRequest_Explain_ExplainMode.SIMPLE;
    } else {
      switch (mode.trim().toLowerCase()) {
        case "simple":
          explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.SIMPLE;
          break;
        case "extended":
          explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.EXTENDED;
          break;
        case "codegen":
          explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.CODEGEN;
          break;
        case "cost":
          explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.COST;
          break;
        case "formatted":
          explain.explainMode = AnalyzePlanRequest_Explain_ExplainMode.FORMATTED;
          break;
        default:
          throw new Error("Unsupported explain mode: " + mode);
      }
    }
    this.request.analyze = { case: "explain", value : explain };
    return this;
  }
  withTreeString(plan: Plan, level: number) {
    const treeString = create(AnalyzePlanRequest_TreeStringSchema, { plan: plan, level: level }); 
    this.request.analyze = { case: "treeString", value : treeString };
    return this;
  }
  withIsLocal(plan: Plan) {
    const isLocal = create(AnalyzePlanRequest_IsLocalSchema, { plan: plan });
    this.request.analyze = { case: "isLocal", value : isLocal };
    return this;
  }
  withIsStreaming(plan: Plan) {
    const isStreaming = create(AnalyzePlanRequest_IsStreamingSchema, { plan: plan });
    this.request.analyze = { case: "isStreaming", value : isStreaming };
    return this;
  }
  withInputFiles(plan: Plan) {
    const inputFiles = create(AnalyzePlanRequest_InputFilesSchema, { plan: plan });
    this.request.analyze = { case: "inputFiles", value : inputFiles };
    return this;
  }
  withSparkVersion() {
    const sparkVersion = create(AnalyzePlanRequest_SparkVersionSchema, {});
    this.request.analyze = { case: "sparkVersion", value : sparkVersion };
    return this;
  }
  withDdlParser(ddlString: string) {
    const ddlParser = create(AnalyzePlanRequest_DDLParseSchema, { ddlString: ddlString });
    this.request.analyze = { case: "ddlParse", value : ddlParser };
    return this;
  }
  withSameSemantics(target: Plan, other: Plan) {
    const sameSemantics = create(AnalyzePlanRequest_SameSemanticsSchema, { targetPlan: target, otherPlan: other });
    this.request.analyze = { case: "sameSemantics", value : sameSemantics };
    return this;
  }

  withSemanticHash(plan: Plan) {
    const semanticHash = create(AnalyzePlanRequest_SemanticHashSchema, { plan: plan });
    this.request.analyze = { case: "semanticHash", value : semanticHash };
    return this;
  }
  withPersist(relation?: Relation, lv?: StorageLevel) {
    const storageLevelPB = lv ? createStorageLevelPB(lv) : undefined;
    const persist = create(AnalyzePlanRequest_PersistSchema, { relation: relation, storageLevel: storageLevelPB });
    this.request.analyze = { case: "persist", value : persist };
    return this;
  }
  withUnpersist(relation?: Relation, blocking?: boolean) {
    const unpersist = create(AnalyzePlanRequest_UnpersistSchema, { relation: relation, blocking: blocking });
    this.request.analyze = { case: "unpersist", value : unpersist };
    return this;
  }

  withGetStorageLevel(relation?: Relation) {
    const getStorageLevel = create(AnalyzePlanRequest_GetStorageLevelSchema, { relation: relation });
    this.request.analyze = { case: "getStorageLevel", value : getStorageLevel };
    return this;
  }
  build(): AnalyzePlanRequest {
    return this.request;
  }
}
