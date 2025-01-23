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

import { Expression } from "../../../../gen/spark/connect/expressions_pb";
import { ExpressionBuilder } from "./proto/expression/ExpressionBuilder";

export class Column {
  private expr_: Expression
  constructor(f: (builder: ExpressionBuilder) => void);
  constructor(name: string);
  constructor(name: string, planId?: bigint);
  constructor(arg1: string | ((builder: ExpressionBuilder) => void), arg2?: bigint) {
    if (typeof arg1 === "string") {
      if (arg1 === "*") {
        this.expr_ = new ExpressionBuilder().withUnresolvedStar().build();
      } else if (arg1.endsWith(".*")) {
        this.expr_ = new ExpressionBuilder().withUnresolvedStar(arg1, arg2).build();
      } else {
        this.expr_ = new ExpressionBuilder().withUnresolvedAttribute(arg1, arg2).build();
      }
    } else {
      const builder = new ExpressionBuilder();
      arg1(builder);
      this.expr_ = builder.build();
    }
  }

  get expr(): Expression {
    return this.expr_;
  }
}
