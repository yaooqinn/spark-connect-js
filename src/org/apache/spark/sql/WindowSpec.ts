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

import { Expression, Expression_SortOrder, Expression_Window_WindowFrame_FrameType } from "../../../../gen/spark/connect/expressions_pb";
import { Column } from "./Column";
import { sortOrder } from "./proto/expression/utils";
import { WindowBuilder } from "./proto/expression/window/WindowBuilder";
import { WindowFrameBuilder } from "./proto/expression/window/WindowFlameBuilder";
import { lit } from "./functions";

/**
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 */
export class WindowSpec {
  private partitionSpec_: Expression[] = [];
  private orderSpec_: Expression_SortOrder[] = [];
  private frameSpec_?: { frameType: Expression_Window_WindowFrame_FrameType, lower: number, upper: number };

  constructor() {}

  /**
   * Defines the partitioning columns in a WindowSpec.
   */
  partitionBy(...cols: (string | Column)[]): WindowSpec {
    const newSpec = new WindowSpec();
    newSpec.partitionSpec_ = cols.map(c => typeof c === "string" ? new Column(c).expr : c.expr);
    newSpec.orderSpec_ = this.orderSpec_;
    newSpec.frameSpec_ = this.frameSpec_;
    return newSpec;
  }

  /**
   * Defines the ordering columns in a WindowSpec.
   */
  orderBy(...cols: (string | Column)[]): WindowSpec {
    const newSpec = new WindowSpec();
    newSpec.partitionSpec_ = this.partitionSpec_;
    newSpec.orderSpec_ = cols.map(c => {
      const col = typeof c === "string" ? new Column(c) : c;
      return sortOrder(col.expr, true, true);
    });
    newSpec.frameSpec_ = this.frameSpec_;
    return newSpec;
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   * Both `start` and `end` are relative positions from the current row. For example,
   * "0" means "current row", while "-1" means the row before the current row, and
   * "5" means the fifth row after the current row.
   */
  rowsBetween(start: number, end: number): WindowSpec {
    const newSpec = new WindowSpec();
    newSpec.partitionSpec_ = this.partitionSpec_;
    newSpec.orderSpec_ = this.orderSpec_;
    newSpec.frameSpec_ = { 
      frameType: Expression_Window_WindowFrame_FrameType.ROW, 
      lower: start, 
      upper: end 
    };
    return newSpec;
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   * Both `start` and `end` are relative from the current row. For example,
   * "0" means "current row", while "-1" means one off before the current row, and
   * "5" means the five off after the current row.
   */
  rangeBetween(start: number, end: number): WindowSpec {
    const newSpec = new WindowSpec();
    newSpec.partitionSpec_ = this.partitionSpec_;
    newSpec.orderSpec_ = this.orderSpec_;
    newSpec.frameSpec_ = { 
      frameType: Expression_Window_WindowFrame_FrameType.RANGE, 
      lower: start, 
      upper: end 
    };
    return newSpec;
  }

  /**
   * Internal method to convert this WindowSpec to an Expression.
   * @internal
   */
  toExpression(windowFunction: Expression): Expression {
    // Lazy load to avoid circular dependency
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const ExpressionBuilder = require("./proto/expression/ExpressionBuilder").ExpressionBuilder;
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const Window = require("./Window").Window;
    const builder = new ExpressionBuilder();
    
    builder.withWindowBuilder((wb: WindowBuilder) => {
      wb.withWindowFunction(windowFunction);
      
      if (this.partitionSpec_.length > 0) {
        wb.withPartitionSpec(this.partitionSpec_);
      }
      
      if (this.orderSpec_.length > 0) {
        wb.withOrderSpec(this.orderSpec_);
      }
      
      if (this.frameSpec_) {
        wb.withWindowFrameBuilder((fb: WindowFrameBuilder) => {
          fb.withFrameType(this.frameSpec_!.frameType);
          
          const lower = this.frameSpec_!.lower;
          const upper = this.frameSpec_!.upper;
          
          // Handle special boundary values
          if (lower === Window.unboundedPreceding && upper === Window.unboundedFollowing) {
            fb.withUnboundedBoundaries();
          } else if (lower === Window.currentRow && upper === Window.currentRow) {
            fb.withCurrentRowBoundaries();
          } else {
            // Use expression boundaries for numeric values
            fb.withExpressionBoundaries(
              lit(lower).expr,
              lit(upper).expr
            );
          }
        });
      }
    });
    
    return builder.build();
  }
}
