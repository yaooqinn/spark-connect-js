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
import { Command, CommandSchema, SqlCommandSchema, CheckpointCommandSchema, WriteOperationV2, WriteOperation } from "../../../../../gen/spark/connect/commands_pb";
import { CommonInlineUserDefinedFunction } from "../../../../../gen/spark/connect/expressions_pb";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";
import { StorageLevel } from "../../storage/StorageLevel";
import { createStorageLevelPB } from "./ProtoUtils";

export class CommandBuilder {
  private command: Command = create(CommandSchema, {});

  withSqlCommand(sql: string) {
    const sqlCmd = create(SqlCommandSchema, { sql: sql });
    this.command.commandType = { case: "sqlCommand", value: sqlCmd };
    return this;
  }

  withRegisterFunction(udf: CommonInlineUserDefinedFunction) {
    this.command.commandType = { case: "registerFunction", value: udf };
    return this;
  }

  withWriteOperation(writeOp: WriteOperation) {
    this.command.commandType = { case: "writeOperation", value: writeOp };
    return this;
  }

  withWriteOperationV2(writeOp: WriteOperationV2) {
    this.command.commandType = { case: "writeOperationV2", value: writeOp };
    return this;
  }

  withCheckpointCommand(relation: Relation, local: boolean, eager: boolean, storageLevel?: StorageLevel) {
    const storageLevelPB = storageLevel ? createStorageLevelPB(storageLevel) : undefined;
    
    const checkpointCmd = create(CheckpointCommandSchema, {
      relation: relation,
      local: local,
      eager: eager,
      storageLevel: storageLevelPB
    });
    this.command.commandType = { case: "checkpointCommand", value: checkpointCmd };
    return this;
  }

  build(): Command {
    return this.command;
  }
}
