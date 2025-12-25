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

  asFunction(name: string, ...inputs: Column[]): Column {
    return Column.fn(name, this, false, ...inputs);
  }

  static fn(name: string, column: string | Column, isDistinct: boolean, ...inputs: Column[]): Column {
    const args: Column[] = [];
    if (typeof column === "string") {
      args.push(new Column(column));
    } else {
      args.push(column);
    }
    args.push(...inputs);
    return new Column(b => b.withUnresolvedFunction(name, args.map(i => i.expr), isDistinct, false));
  }

  get expr(): Expression {
    return this.expr_;
  }

  /**
   * True if the current expression is NaN.
   *
   * @group expr_ops
   * @since 1.5.0
   */
  get isNaN(): Column {
    return Column.fn("isNaN", this, false);
  }

  get isNull(): Column {
    return Column.fn("isNull", this, false);
  }

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated to
   * the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)` with
   * explicit metadata.
   *
   * @group expr_ops
   */
  as(alias: string): Column {
    return this.name(alias);
  }

  add(other: Column): Column {
    return Column.fn("+", this, false, other);
  }

  plus(other: Column): Column {
    return this.add(other);
  }

  get asc(): Column {
    return this.asc_nulls_first;
  }

  get asc_nulls_first(): Column {
    return new Column(b => b.withSortOrder(this.expr, true, true));
  }

  get asc_nulls_last(): Column {
    return new Column(b => b.withSortOrder(this.expr, true, false));
  }

  get desc(): Column {
    return this.desc_nulls_first;
  }

  get desc_nulls_first(): Column {
    return new Column(b => b.withSortOrder(this.expr, false, true));
  }

  get desc_nulls_last(): Column {
    return new Column(b => b.withSortOrder(this.expr, false, false));
  }

  name(alias: string): Column {
    return new Column(b => b.withAlias([alias], this.expr));
  }

  /**
   * Define a windowing column.
   *
   * {{{
   *   val w = Window.partitionBy("name").orderBy("id")
   *   df.select(
   *     sum("price").over(w.rangeBetween(Window.unboundedPreceding, 2)),
   *     avg("price").over(w.rowsBetween(Window.currentRow, 4))
   *   )
   * }}}
   *
   * @group expr_ops
   */
  over(window: any): Column {
    // Lazy load to avoid circular dependency
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const WindowSpec = require("./WindowSpec").WindowSpec;
    if (!(window instanceof WindowSpec)) {
      throw new Error("Argument to over() must be a WindowSpec");
    }
    const windowExpr = window.toExpression(this.expr);
    // Create a new Column from the window expression
    const col = new Column("");
    (col as any).expr_ = windowExpr;
    return col;
  }
}
