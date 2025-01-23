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
import { CallFunctionSchema, Expression, Expression_AliasSchema, Expression_Cast_EvalMode, Expression_CastSchema, Expression_ExpressionStringSchema, Expression_LambdaFunction, Expression_LambdaFunctionSchema, Expression_Literal, Expression_SortOrder_NullOrdering, Expression_SortOrder_SortDirection, Expression_SortOrderSchema, Expression_UnresolvedAttribute, Expression_UnresolvedAttributeSchema, Expression_UnresolvedExtractValueSchema, Expression_UnresolvedFunction, Expression_UnresolvedFunctionSchema, Expression_UnresolvedNamedLambdaVariable, Expression_UnresolvedNamedLambdaVariableSchema, Expression_UnresolvedRegexSchema, Expression_UnresolvedStarSchema, Expression_UpdateFieldsSchema, Expression_Window_WindowFrame_FrameType, Expression_Window_WindowFrameSchema, Expression_WindowSchema, ExpressionCommon, ExpressionSchema, MergeAction_ActionType, MergeAction_Assignment, MergeActionSchema, NamedArgumentExpressionSchema, TypedAggregateExpressionSchema } from "../../../../../../gen/spark/connect/expressions_pb";
import { LiteralBuilder } from "./LiteralBuilder";
import { DataType } from "../../types/data_types";
import { DataTypes } from "../../types";
import { scalarScalaUdf, sortOrder } from "./utils";
import { WindowBuilder } from "./window/WindowBuilder";
import { CommonInlineUserDefinedFunctionBuilder } from "./udf/CommonInlineUserDefinedFunctionBuilder";

export class ExpressionBuilder {
  private expression: Expression = create(ExpressionSchema, {});
  constructor() {}

  withExpressionCommon(common: ExpressionCommon) {
    this.expression.common = common;
    return this;
  }

  withLiteralBuilder(f: (builder: LiteralBuilder) => void) {
    const builder = new LiteralBuilder();
    f(builder);
    return this.withLiteral(builder.build());
  }

  withLiteral(literal: Expression_Literal) {
    this.expression.exprType = { case: "literal", value: literal };
    return this;
  }

  withUnresolvedAttribute(u: Expression_UnresolvedAttribute): ExpressionBuilder;
  withUnresolvedAttribute(unparsedIdentifier: string, planId?: bigint, isMetadataColumn?: boolean): ExpressionBuilder;
  withUnresolvedAttribute(idOrAttr: string | Expression_UnresolvedAttribute, planId?: bigint, isMetadataColumn?: boolean) {
    if (typeof idOrAttr === 'string') {
      const u = create(Expression_UnresolvedAttributeSchema, {
        unparsedIdentifier: idOrAttr,
        planId: planId,
        isMetadataColumn: isMetadataColumn
      });
      this.expression.exprType = { case: "unresolvedAttribute", value: u };
    } else {
      this.expression.exprType = { case: "unresolvedAttribute", value: idOrAttr };
    }
    return this;
  }

  withUnresolvedFunction(f: Expression_UnresolvedFunction): ExpressionBuilder;
  withUnresolvedFunction(f: string, args: Expression[], isDistinct?: boolean, isUserDefinedFunction?: boolean): ExpressionBuilder;
  withUnresolvedFunction(f: string | Expression_UnresolvedFunction, args?: Expression[], isDistinct?: boolean, isUserDefinedFunction?: boolean) {
    if (typeof f === 'string') {
      const u = create(Expression_UnresolvedFunctionSchema, {
        functionName: f,
        arguments: args,
        isDistinct: isDistinct,
        isUserDefinedFunction: isUserDefinedFunction
      });
      this.expression.exprType = { case: "unresolvedFunction", value: u };
    } else {
      this.expression.exprType = { case: "unresolvedFunction", value: f };
    }
    return this;
  }

  withExpressionString(expression: string) {
    const expr = create(Expression_ExpressionStringSchema, { expression: expression });
    this.expression.exprType = { case: "expressionString", value: expr };
    return this;
  }

  withUnresolvedStar(unparsedTarget?: string, planId?: bigint) {
    const star = create(Expression_UnresolvedStarSchema, { unparsedTarget: unparsedTarget, planId: planId });
    this.expression.exprType = { case: "unresolvedStar", value: star };
    return this;
  }

  withAlias(nameParts: string[], expr: Expression, metadata?: string) {
    const alias = create(Expression_AliasSchema, { name: nameParts, expr: expr, metadata: metadata });
    this.expression.exprType = { case: "alias", value: alias };
    return this;
  }

  withCast(dataType: string | DataType, expr: Expression, evalMode?: Expression_Cast_EvalMode) {
    const cast = create(Expression_CastSchema, { expr: expr, evalMode: evalMode });
    if (typeof dataType === 'string') {
      cast.castToType = { case: "typeStr", value: dataType }
    } else {
      cast.castToType = { case: "type", value: DataTypes.toProtoType(dataType) }
    }
    this.expression.exprType = { case: "cast", value: cast };
    return this;
  }

  withUnresolvedRegex(colName: string, planId?: bigint) {
    const unresolvedRegex = create(Expression_UnresolvedRegexSchema, { colName: colName, planId: planId});
    this.expression.exprType = { case: "unresolvedRegex", value: unresolvedRegex };
    return this;
  }

  withSortOrder(child: Expression, asc: boolean = true, nullsFirst: boolean = true) {
    const order = sortOrder(child, asc, nullsFirst);
    this.expression.exprType = { case: "sortOrder", value: order };
    return this;
  }

  withLambdaFunction(func: Expression, namePartsList: string[][]) {
    const variables = namePartsList.map(arg => create(Expression_UnresolvedNamedLambdaVariableSchema, { nameParts: arg }));
    const lambda = create(Expression_LambdaFunctionSchema, { function: func, arguments: variables });
    this.expression.exprType = { case: "lambdaFunction", value: lambda };
    return this;
  }

  withWindowBuilder(f: (b: WindowBuilder) => void) {
    const builder = new WindowBuilder();
    f(builder);
    this.expression.exprType = { case: "window", value: builder.build() };
    return this;
  }

  withUnresolvedExtractValue(child: Expression, extraction: Expression) {
    const extract = create(Expression_UnresolvedExtractValueSchema, { child: child, extraction: extraction });
    this.expression.exprType = { case: "unresolvedExtractValue", value: extract };
    return this;
  }

  withUpdateFields(structExpression: Expression, fieldName: string, valueExpression?: Expression) {
    const update = create(Expression_UpdateFieldsSchema,
      {
        structExpression: structExpression,
        fieldName: fieldName,
        valueExpression: valueExpression
      });
    this.expression.exprType = { case: "updateFields", value: update };
    return this;
  }

  withUnresolvedNamedLambdaVariable(nameParts: string[]) {
    const variable = create(Expression_UnresolvedNamedLambdaVariableSchema, { nameParts: nameParts });
    this.expression.exprType = { case: "unresolvedNamedLambdaVariable", value: variable };
    return this;
  }

  withCommonInlineUserDefinedFunctionBuilder(
      functionName: string,
      deterministic: boolean,
      f: (b: CommonInlineUserDefinedFunctionBuilder) => void) {
    const builder = new CommonInlineUserDefinedFunctionBuilder(functionName, deterministic);
    f(builder);
    this.expression.exprType = { case: "commonInlineUserDefinedFunction", value: builder.build() };
    return this;
  }

  withCallFunction(functionName: string, args: Expression[]) {
    const call = create(CallFunctionSchema, { functionName: functionName, arguments: args });
    this.expression.exprType = { case: "callFunction", value: call };
    return this;
  }

  withNamedArgumentExpression(name: string, expr: Expression) {
    const named = create(NamedArgumentExpressionSchema, { key: name, value: expr });
    this.expression.exprType = { case: "namedArgumentExpression", value: named };
    return this;
  }

  withMergeAction(
      actionType: MergeAction_ActionType,
      condition: Expression,
      assignments: MergeAction_Assignment[]) {
    const mergeAction = create(MergeActionSchema, { actionType: actionType, condition: condition, assignments: assignments });
    this.expression.exprType = { case: "mergeAction", value: mergeAction };
    return this;
  }

  withTypedAggregateExpression(
      payload: Uint8Array,
      inputTypes: DataType[],
      outputType: DataType,
      nullable: boolean,
      aggregate: boolean) {
    const udf = scalarScalaUdf(payload, inputTypes, outputType, nullable, aggregate);
    const expr = create(TypedAggregateExpressionSchema, { scalarScalaUdf: udf });
    this.expression.exprType = { case: "typedAggregateExpression", value: expr };
    return this;
  }

  // TODO: withAny?

  build() {
    return this.expression;
  }
}