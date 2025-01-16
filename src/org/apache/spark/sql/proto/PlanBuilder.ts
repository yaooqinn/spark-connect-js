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
import { Plan, PlanSchema } from "../../../../../gen/spark/connect/base_pb";
import { RelationBuilder } from "./RelationBuilder";
import { Relation } from "../../../../../gen/spark/connect/relations_pb";
import { Command } from "../../../../../gen/spark/connect/commands_pb";
import { CommandBuilder } from "./CommandBuilder";

export class PlanBuilder {
  private plan: Plan = create(PlanSchema, {});
  constructor() {}
  withRelationBuilder(f: (builder: RelationBuilder) => void) {
    const builder = new RelationBuilder();
    f(builder);
    this.withRelation(builder.build());
    return this;
  }
  withRelation(relation: Relation) {
    this.plan.opType = { case: "root", value: relation };
    return this;
  }
  withCommandBuilder(f: (builder: CommandBuilder) => void) {
    const builder = new CommandBuilder();
    f(builder);
    this.withCommand(builder.build());
    return this;
  }
  withCommand(command: Command) {
    this.plan.opType = { case: "command", value: command };
    return this;
  }
  build(): Plan {
    return this.plan;
  }
}
