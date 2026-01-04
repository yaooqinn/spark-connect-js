# Code Style Guide

Coding standards and conventions for spark.js development.

## Overview

spark.js follows TypeScript and Apache Spark ecosystem conventions. This guide ensures consistency and maintainability across the codebase.

## TypeScript Configuration

### Version and Target

- **TypeScript Version**: 5.x
- **Target**: ES2020
- **Module System**: CommonJS (for Node.js compatibility)
- **Strict Mode**: Enabled

Configuration in `tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true
  }
}
```

## Naming Conventions

### Classes, Interfaces, and Types

Use **PascalCase** for:
- Classes: `SparkSession`, `DataFrame`, `Column`
- Interfaces: `DataFrameReader`, `DataFrameWriter`
- Type aliases: `SaveMode`, `DataType`
- Enums: `SaveMode`, `StorageLevel`

```typescript
// Good
class SparkSession { }
interface DataFrameReader { }
type DataType = string | number;
enum SaveMode { Append, Overwrite }

// Bad
class sparkSession { }
interface dataFrameReader { }
```

**Note**: Do NOT use interface prefixes like `I`:

```typescript
// Good
interface DataFrameReader { }

// Bad
interface IDataFrameReader { }
```

### Methods, Variables, and Functions

Use **camelCase** for:
- Methods: `createDataFrame`, `withColumn`, `show`
- Variables: `sparkSession`, `dataFrame`, `columnName`
- Functions: `col`, `lit`, `when`

```typescript
// Good
function createDataFrame() { }
const dataFrame = spark.range(10);
const columnName = 'id';

// Bad
function CreateDataFrame() { }
const DataFrame = spark.range(10);
```

### Constants

Use **SCREAMING_SNAKE_CASE** for true constants:

```typescript
// Good
const DEFAULT_PORT = 15002;
const MAX_RETRIES = 3;

// For configuration values, camelCase is acceptable
const defaultPort = 15002;
```

## File Naming

**Exception to TypeScript conventions**: This project uses **PascalCase** for TypeScript files.

This aligns with Apache Spark ecosystem conventions (Java/Scala).

```
// Good
SparkSession.ts
DataFrame.ts
DataFrameReader.ts
DataTypes.ts

// Acceptable for utilities
helpers.ts
utils.ts
logger.ts

// Bad - DO NOT use kebab-case
spark-session.ts
data-frame.ts
```

**Important**: **Do NOT rename files to kebab-case** - maintain consistency with Spark conventions.

## Code Organization

### Imports

Order imports logically:

```typescript
// 1. Node.js built-ins
import * as fs from 'fs';
import * as path from 'path';

// 2. External dependencies
import { Message } from '@bufbuild/protobuf';
import * as grpc from '@grpc/grpc-js';

// 3. Internal modules (relative imports)
import { DataFrame } from './DataFrame';
import { Column } from './Column';
import { col, lit } from './functions';

// 4. Generated protobuf (absolute imports)
import { Plan } from '../../../gen/spark/connect/base_pb';
```

### Module Structure

Keep modules focused and single-purpose:

```typescript
// Good: Focused module
export class DataFrame {
  // All DataFrame operations
}

// Bad: Kitchen sink module
export class Everything {
  // DataFrame, SparkSession, Column, etc.
}
```

### Function Length

Keep functions focused (prefer <50 lines):

```typescript
// Good: Short, focused function
function filter(condition: string): DataFrame {
  const plan = buildFilterPlan(condition);
  return new DataFrame(this.client, plan);
}

// If logic grows, extract helpers
function complexOperation(): DataFrame {
  const step1 = prepareData();
  const step2 = transformData(step1);
  const step3 = finalizeData(step2);
  return step3;
}

function prepareData() { /* ... */ }
function transformData(data) { /* ... */ }
function finalizeData(data) { /* ... */ }
```

## Type System

### Avoid `any`

Minimize `any` usage. Prefer `unknown` with type narrowing:

```typescript
// Good
function process(value: unknown): string {
  if (typeof value === 'string') {
    return value.toUpperCase();
  }
  return String(value);
}

// Acceptable when necessary
function legacy(data: any): void {
  // Working with untyped external API
}

// Bad - unnecessary any
function getValue(obj: any): any {
  return obj.value;
}
```

### Use Type Guards

Create type guards for runtime type checking:

```typescript
// Good
function isString(value: unknown): value is string {
  return typeof value === 'string';
}

function process(value: unknown): string {
  if (isString(value)) {
    return value.toUpperCase();  // TypeScript knows it's string
  }
  return '';
}
```

### Express Intent with Utility Types

Use TypeScript utility types:

```typescript
// Good
type ReadonlyConfig = Readonly<Config>;
type PartialUpdate = Partial<User>;
type StringMap = Record<string, string>;

// Mark parameters as readonly
function process(items: readonly string[]): void {
  // items cannot be modified
}
```

### Discriminated Unions

Use for complex state or message types:

```typescript
// Good
type Result<T> =
  | { success: true; value: T }
  | { success: false; error: Error };

function handle(result: Result<number>): void {
  if (result.success) {
    console.log(result.value);  // TypeScript knows value exists
  } else {
    console.error(result.error);  // TypeScript knows error exists
  }
}
```

## Async and Error Handling

### Use async/await

Prefer `async/await` over raw Promises:

```typescript
// Good
async function fetchData(): Promise<Data> {
  const response = await fetch(url);
  const data = await response.json();
  return data;
}

// Bad
function fetchData(): Promise<Data> {
  return fetch(url)
    .then(response => response.json())
    .then(data => data);
}
```

### Structured Error Handling

Wrap awaits in try/catch with structured errors:

```typescript
// Good
async function process(): Promise<Result> {
  try {
    const data = await fetchData();
    return { success: true, data };
  } catch (error) {
    logger.error('Failed to process', error);
    return { success: false, error };
  }
}

// Guard edge cases early
function divide(a: number, b: number): number {
  if (b === 0) {
    throw new Error('Division by zero');
  }
  return a / b;
}
```

### Resource Cleanup

Ensure proper disposal of resources:

```typescript
// Good
async function useResource(): Promise<void> {
  const resource = await acquireResource();
  try {
    await processWithResource(resource);
  } finally {
    await resource.dispose();  // Always cleanup
  }
}
```

## Formatting

### Indentation

- **2 spaces** (not tabs)
- Configured in `.vscode/settings.json`

```typescript
// Good
function example() {
  if (condition) {
    doSomething();
  }
}

// Bad (4 spaces or tabs)
function example() {
    if (condition) {
        doSomething();
    }
}
```

### Line Length

- Prefer lines under 100 characters
- Break long lines logically

```typescript
// Good
const result = someObject
  .method1()
  .method2()
  .method3();

// Good
const config = {
  propertyOne: 'value',
  propertyTwo: 'another value',
  propertyThree: 'yet another value'
};
```

### Semicolons

Use semicolons (enforced by ESLint):

```typescript
// Good
const x = 5;
doSomething();

// Bad
const x = 5
doSomething()
```

## Comments and Documentation

### JSDoc for Public APIs

Add JSDoc comments to public classes, methods, and functions:

```typescript
/**
 * Creates a new DataFrame with an additional column.
 * 
 * @param colName - Name of the new column
 * @param col - Column expression for the new column
 * @returns A new DataFrame with the added column
 * 
 * @example
 * ```typescript
 * const df = spark.range(5);
 * const result = df.withColumn('doubled', col('id').multiply(lit(2)));
 * ```
 */
withColumn(colName: string, col: Column): DataFrame {
  // Implementation
}
```

### Include Useful Tags

Use appropriate JSDoc tags:

- `@param` - Parameter description
- `@returns` - Return value description
- `@throws` - Exceptions that may be thrown
- `@example` - Usage examples
- `@remarks` - Additional notes
- `@group` - Categorization for TypeDoc

```typescript
/**
 * Filters rows using a SQL expression.
 * 
 * @param condition - SQL WHERE clause condition
 * @returns Filtered DataFrame
 * @throws {Error} If condition is invalid SQL
 * 
 * @group Transformations
 * 
 * @example
 * ```typescript
 * df.filter('age > 18');
 * df.filter(col('age').gt(18));
 * ```
 */
filter(condition: string | Column): DataFrame {
  // Implementation
}
```

### Inline Comments

Write comments that explain **why**, not **what**:

```typescript
// Good - explains intent
// Use base64 encoding to safely transmit binary data over gRPC
const encoded = Buffer.from(data).toString('base64');

// Bad - obvious from code
// Convert data to base64
const encoded = Buffer.from(data).toString('base64');

// Good - explains non-obvious behavior
// Spark uses 1-based indexing for substring, unlike JavaScript
const result = substring(col('text'), 1, 5);
```

### Keep Comments Updated

Remove or update stale comments during refactors:

```typescript
// Bad - stale comment
// TODO: Remove this hack when Spark 3.5 is released
const workaround = doSomething();  // (Spark 4.0 is already released!)

// Good - relevant comment
// Workaround for Spark Connect bug SPARK-12345
const workaround = doSomething();
```

## License Headers

**ALL source files** must include the Apache License 2.0 header:

```typescript
/**
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
```

## ESLint Configuration

The project uses ESLint with TypeScript support.

**Key rules**:
- `@typescript-eslint/no-explicit-any`: OFF (any is allowed but discouraged)
- `@typescript-eslint/no-unused-expressions`: OFF

Run linter:
```bash
npm run lint
```

## Best Practices

### Immutability

Favor immutable data and pure functions:

```typescript
// Good - immutable
function add(arr: readonly number[], value: number): number[] {
  return [...arr, value];
}

// Bad - mutates input
function add(arr: number[], value: number): number[] {
  arr.push(value);
  return arr;
}
```

### Single Responsibility

Each class/function should have one responsibility:

```typescript
// Good
class DataFrame {
  filter(condition: string): DataFrame { /* ... */ }
  select(...cols: string[]): DataFrame { /* ... */ }
}

class DataFrameWriter {
  parquet(path: string): Promise<void> { /* ... */ }
  csv(path: string): Promise<void> { /* ... */ }
}

// Bad - too many responsibilities
class DataFrame {
  filter(condition: string): DataFrame { /* ... */ }
  write(): DataFrameWriter { /* ... */ }
  read(): DataFrameReader { /* ... */ }
  // Should be separate classes
}
```

### Dependency Injection

Inject dependencies for testability:

```typescript
// Good - dependencies injected
class SparkSession {
  constructor(private client: Client) { }
  
  range(end: number): DataFrame {
    return new DataFrame(this.client, plan);
  }
}

// Bad - hard-coded dependency
class SparkSession {
  range(end: number): DataFrame {
    const client = new Client('localhost:15002');  // Hard to test!
    return new DataFrame(client, plan);
  }
}
```

## Security

### Validate External Input

Use validators for external data:

```typescript
// Good
function processMessage(data: unknown): Message {
  if (!isValidMessage(data)) {
    throw new Error('Invalid message');
  }
  return parseMessage(data);
}
```

### Avoid Dynamic Code Execution

Never use `eval()` or `Function()` constructors:

```typescript
// Bad - security risk
eval(userInput);
const fn = new Function(userInput);

// Good - use proper parsing
const result = JSON.parse(userInput);
```

## Next Steps

- Review [Testing](testing.md) for testing standards
- See [Documentation](documentation.md) for doc writing guidelines
- Check [Architecture](architecture.md) for design patterns
- Read [Building](building.md) for build process
