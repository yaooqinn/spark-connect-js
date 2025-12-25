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

import { DataType } from '../types/data_types';
import { DataTypes } from '../types/DataTypes';

/**
 * Parse a string data type to a DataType object.
 * 
 * @param typeString - The data type as a string
 * @returns DataType
 */
export function parseDataType(typeString: string): DataType {
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
 */
export function serializeFunctionToPython(func: (...args: any[]) => any): string {
  const funcStr = func.toString();
  
  // Simple conversion for arrow functions
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
