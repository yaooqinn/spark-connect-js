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
import { Expression, Expression_SortOrder, Expression_Window, Expression_WindowSchema } from "../../../../../../../gen/spark/connect/expressions_pb";
import { WindowFrameBuilder } from "./WindowFlameBuilder";

export class WindowBuilder {
  private window: Expression_Window = create(Expression_WindowSchema, {});
  constructor() {}

  withWindowFunction(windowFunction: Expression) {
    this.window.windowFunction = windowFunction;
    return this;
  }

  withPartitionSpec(partitionSpec: Expression[]) {
    this.window.partitionSpec = partitionSpec;
    return this;
  }

  withOrderSpec(orderSpec: Expression_SortOrder[]) {
    this.window.orderSpec = orderSpec;
    return this;
  }
  
  withWindowFrameBuilder(f: (b: WindowFrameBuilder) => void) {
    const builder = new WindowFrameBuilder();
    f(builder);
    this.window.frameSpec = builder.build();
    return this;
  }

  build(): Expression_Window {
    return this.window;
  }
}