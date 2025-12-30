import { create } from "@bufbuild/protobuf";
import { CommonInlineUserDefinedFunction, CommonInlineUserDefinedFunctionSchema, Expression, PythonUDFSchema } from "../../../../../../../gen/spark/connect/expressions_pb";
import { DataType, DataTypes } from "../../../types";
import { javaUDF, scalarScalaUdf } from "../utils";

export class CommonInlineUserDefinedFunctionBuilder {
  private udf: CommonInlineUserDefinedFunction =
    create(CommonInlineUserDefinedFunctionSchema, {});

  constructor(functionName: string, determinstic: boolean) {
    this.udf.functionName = functionName;
    this.udf.deterministic = determinstic;
  }

  withArguments(args: Expression[]): CommonInlineUserDefinedFunctionBuilder {
    this.udf.arguments = args;
    return this;
  }

  withPythonUDF(
      outputType: DataType,
      evalType: number,
      command: Uint8Array,
      pythonVer: string,
      additionalIncludes: string[]): CommonInlineUserDefinedFunctionBuilder {
    const udf = create(PythonUDFSchema, {
      outputType: DataTypes.toProtoType(outputType),
      evalType: evalType,
      command: command,
      pythonVer: pythonVer,
      additionalIncludes: additionalIncludes
    });
    this.udf.function = { case: "pythonUdf", value: udf };
    return this;
  }

  withScalarScalaUDF(
      payload: Uint8Array,
      inputTypes: DataType[],
      outputType: DataType,
      nullable: boolean,
      aggregate: boolean): CommonInlineUserDefinedFunctionBuilder {
    this.udf.function = { case: "scalarScalaUdf", value: scalarScalaUdf(payload, inputTypes, outputType, nullable, aggregate) };
    return this;
  }

  withJavaUDF(className: string, outputType: DataType | undefined, aggregate: boolean): CommonInlineUserDefinedFunctionBuilder {
    this.udf.function = { case: "javaUdf", value: javaUDF(className, outputType, aggregate) };
    return this;
  }

  build(): CommonInlineUserDefinedFunction {
    return this.udf;
  }
}