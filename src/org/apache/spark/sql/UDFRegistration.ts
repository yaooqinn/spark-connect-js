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

import { SparkSession } from './SparkSession';
import { DataType } from './types/data_types';
import { CommonInlineUserDefinedFunctionBuilder } from './proto/expression/udf/CommonInlineUserDefinedFunctionBuilder';
import { CommandBuilder } from './proto/CommandBuilder';
import { parseDataType, serializeFunctionToPython } from './util/udf_utils';
import { DataTypes } from './types/DataTypes';

/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this.
 * 
 * @since 1.0.0
 */
export class UDFRegistration {
  constructor(private spark: SparkSession) {}

  /**
   * Register a Python user-defined function.
   * 
   * @param name - The name of the UDF
   * @param func - The JavaScript function to be converted to a Python UDF
   * @param returnType - The return type of the UDF (DataType or string)
   * @returns void
   * 
   * @example
   * ```typescript
   * spark.udf.register("myUdf", (x: number) => x * 2, "int");
   * const result = await spark.sql("SELECT myUdf(id) FROM range(10)");
   * ```
   */
  async register(name: string, func: (...args: any[]) => any, returnType: DataType | string): Promise<void> {
    const dataType = typeof returnType === 'string' 
      ? parseDataType(returnType) 
      : returnType;
    
    // Serialize the JavaScript function to Python code
    const pythonCode = serializeFunctionToPython(func);
    const command = Buffer.from(pythonCode, 'utf-8');

    // Create UDF using the builder
    const udf = new CommonInlineUserDefinedFunctionBuilder(name, true)
      .withPythonUDF(dataType, 200, command, "3.8", [])
      .build();

    // Register the UDF via command
    const cmd = new CommandBuilder().withRegisterFunction(udf).build();
    await this.spark.execute(cmd);
  }

  /**
   * Register a Java user-defined function.
   * 
   * @param name - The name of the UDF
   * @param className - The fully qualified Java class name
   * @param returnType - Optional return type of the UDF
   * @returns void
   * 
   * @example
   * ```typescript
   * spark.udf.registerJavaFunction("javaUdf", "com.example.MyUDF", DataTypes.IntegerType);
   * ```
   */
  async registerJavaFunction(name: string, className: string, returnType?: DataType): Promise<void> {
    const dataType = returnType || DataTypes.NullType;
    
    // Create Java UDF using the builder
    const udf = new CommonInlineUserDefinedFunctionBuilder(name, true)
      .withJavaUDF(className, dataType, false)
      .build();

    // Register the UDF via command
    const cmd = new CommandBuilder().withRegisterFunction(udf).build();
    await this.spark.execute(cmd);
  }
}
