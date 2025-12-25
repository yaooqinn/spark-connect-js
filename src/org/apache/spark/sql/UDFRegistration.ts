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
import { DataTypes } from './types/DataTypes';
import { CommonInlineUserDefinedFunctionBuilder } from './proto/expression/udf/CommonInlineUserDefinedFunctionBuilder';
import { CommandBuilder } from './proto/CommandBuilder';

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
      ? this.parseDataType(returnType) 
      : returnType;
    
    // Serialize the JavaScript function to Python code
    const pythonCode = this.serializeFunctionToPython(func);
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

  /**
   * Parse a string data type to a DataType object.
   * 
   * @param typeString - The data type as a string
   * @returns DataType
   * @private
   */
  private parseDataType(typeString: string): DataType {
    const lowerType = typeString.toLowerCase();
    switch (lowerType) {
      case 'string':
        return DataTypes.StringType;
      case 'int':
      case 'integer':
        return DataTypes.IntegerType;
      case 'long':
      case 'bigint':
        return DataTypes.LongType;
      case 'double':
        return DataTypes.DoubleType;
      case 'float':
        return DataTypes.FloatType;
      case 'boolean':
        return DataTypes.BooleanType;
      case 'byte':
        return DataTypes.ByteType;
      case 'short':
        return DataTypes.ShortType;
      case 'binary':
        return DataTypes.BinaryType;
      case 'date':
        return DataTypes.DateType;
      case 'timestamp':
        return DataTypes.TimestampType;
      default:
        // For complex types, use unparsed
        return DataTypes.createUnparsedDataType(typeString);
    }
  }

  /**
   * Serialize a JavaScript function to Python code.
   * This is a simple implementation that converts basic JS functions to Python.
   * 
   * @param func - The JavaScript function
   * @returns Python code as a string
   * @private
   */
  private serializeFunctionToPython(func: (...args: any[]) => any): string {
    const funcStr = func.toString();
    
    // Simple conversion for arrow functions
    // This is a basic implementation - real-world usage may require more sophisticated conversion
    if (funcStr.includes('=>')) {
      // Extract the function body
      const match = funcStr.match(/\(([^)]*)\)\s*=>\s*(.+)/);
      if (match) {
        const params = match[1].trim() || 'x';
        let body = match[2].trim();
        
        // Remove curly braces if present
        if (body.startsWith('{') && body.endsWith('}')) {
          body = body.slice(1, -1).trim();
        }
        
        // Remove 'return' keyword if present
        body = body.replace(/^return\s+/, '');
        
        // Convert basic JS operators to Python
        body = body.replace(/===/g, '==');
        body = body.replace(/!==/g, '!=');
        
        // Generate Python lambda
        return `lambda ${params}: ${body}`;
      }
    }
    
    // Fallback: try to extract function body
    const bodyMatch = funcStr.match(/function[^{]*{([\s\S]*)}/) || 
                     funcStr.match(/{([\s\S]*)}/);
    if (bodyMatch) {
      let body = bodyMatch[1].trim();
      body = body.replace(/^return\s+/, '');
      body = body.replace(/===/g, '==');
      body = body.replace(/!==/g, '!=');
      return `lambda x: ${body}`;
    }
    
    // Simple fallback
    return `lambda x: x`;
  }
}
