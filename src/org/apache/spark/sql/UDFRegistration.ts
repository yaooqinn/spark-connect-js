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
import { CommandBuilder } from './proto/CommandBuilder';

/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this.
 * 
 * @since 1.0.0
 */
export class UDFRegistration {
  constructor(private spark: SparkSession) {}

  /**
   * Register a Java user-defined function.
   * 
   * @param name - The name of the UDF
   * @param className - The fully qualified Java class name
   * @param returnType - Optional return type of the UDF. If undefined, the server will determine the type.
   * @returns Promise<void>
   * 
   * @example
   * ```typescript
   * spark.udf.registerJava("javaUdf", "com.example.MyUDF", DataTypes.IntegerType);
   * // Or let server decide the return type:
   * spark.udf.registerJava("javaUdf", "com.example.MyUDF");
   * ```
   */
  async registerJava(name: string, className: string, returnType?: DataType): Promise<void> {
    // Register the UDF via command builder
    const cmd = new CommandBuilder()
      .withRegisterFunctionBuilder(name, true, (builder) => {
        builder.withJavaUDF(className, returnType, false);
      })
      .build();
    await this.spark.execute(cmd);
  }
}
