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
import { Expression, Expression_Window_WindowFrame, Expression_Window_WindowFrame_FrameBoundary, Expression_Window_WindowFrame_FrameBoundarySchema, Expression_Window_WindowFrame_FrameType, Expression_Window_WindowFrameSchema } from "../../../../../../../gen/spark/connect/expressions_pb";

export class WindowFrameBuilder {
  private windowFrame: Expression_Window_WindowFrame = create(Expression_Window_WindowFrameSchema, {});
  constructor() {}

  withFrameType(frameType: Expression_Window_WindowFrame_FrameType) {
    this.windowFrame.frameType = frameType;
    return this;
  }

  withCurrentRowBoundaries() {
    const bound = this.currentRowBondary();
    this.windowFrame.lower = bound;
    this.windowFrame.upper = bound;
    return this;
  }

  withUnboundedBoundaries() {
    const bound = this.unboundedBondary();
    this.windowFrame.lower = bound;
    this.windowFrame.upper = bound;
    return this;
  }

  withExpressionBoundaries(lower: Expression, upper: Expression) {
    this.windowFrame.lower = this.expressionBondary(lower);
    this.windowFrame.upper = this.expressionBondary(upper);
    return this;
  }

  build(): Expression_Window_WindowFrame {
    return this.windowFrame;
  }

  private currentRowBondary(): Expression_Window_WindowFrame_FrameBoundary {
    return create(Expression_Window_WindowFrame_FrameBoundarySchema, { boundary: { case: "currentRow", value: true } });
  }

  private unboundedBondary(): Expression_Window_WindowFrame_FrameBoundary {
    return create(Expression_Window_WindowFrame_FrameBoundarySchema, { boundary: { case: "unbounded", value: true } });
  }

  private expressionBondary(value: Expression): Expression_Window_WindowFrame_FrameBoundary {
    return create(Expression_Window_WindowFrame_FrameBoundarySchema, { boundary: { case: "value", value: value } });
  }

}