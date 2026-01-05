# Code Style Guide

This document outlines the coding conventions and best practices for contributing to spark.js.

## General Principles

1. **Follow TypeScript best practices** - Leverage the type system for safety
2. **Write readable code** - Clarity over cleverness
3. **Document public APIs** - Use JSDoc for all exported functions/classes
4. **Test your changes** - Add unit tests for new functionality
5. **Keep it consistent** - Match the existing code style

## TypeScript Conventions

### Target & Module System

- **TypeScript Version**: 5.x
- **Target**: ES2020 (configured in tsconfig.json)
- **Module System**: CommonJS (for Node.js compatibility)
- **Strict Mode**: Enabled

### Type System Guidelines

#### Prefer Strong Types Over `any`

```typescript
// ❌ Avoid
function processData(data: any): any {
  return data.value;
}

// ✅ Prefer
function processData(data: { value: string }): string {
  return data.value;
}

// ✅ Or use unknown with type narrowing
function processData(data: unknown): string {
  if (typeof data === 'object' && data !== null && 'value' in data) {
    return String((data as { value: unknown }).value);
  }
  throw new Error('Invalid data');
}
```

**Note**: While `@typescript-eslint/no-explicit-any` is OFF in the ESLint config, minimize `any` usage. Use `unknown` with type guards when the type is uncertain.

#### Use Discriminated Unions

```typescript
// ✅ Good - Clear intent, type-safe
type Response = 
  | { type: 'success'; data: string }
  | { type: 'error'; error: Error };

function handleResponse(response: Response) {
  if (response.type === 'success') {
    console.log(response.data); // TypeScript knows data exists
  } else {
    console.error(response.error); // TypeScript knows error exists
  }
}
```

#### Leverage Utility Types

```typescript
// Use built-in utility types
type ReadonlyConfig = Readonly<Config>;
type PartialUser = Partial<User>;
type UserKeys = keyof User;
type UserValues = User[keyof User];
```

### Naming Conventions

#### Classes and Types

Use **PascalCase** for classes, interfaces, enums, and type aliases:

```typescript
// ✅ Correct
class SparkSession { }
interface DataFrameOptions { }
type ColumnExpression = Expression | string;
enum SaveMode { }
```

#### Methods and Variables

Use **camelCase** for methods, variables, and functions:

```typescript
// ✅ Correct
function createDataFrame() { }
const sparkSession = new SparkSession();
let columnName = 'id';
```

#### File Naming

**Exception to TypeScript conventions**: This project uses **PascalCase** for TypeScript files to align with Apache Spark ecosystem conventions (Java/Scala):

```
✅ Correct:
  SparkSession.ts
  DataFrame.ts
  DataFrameReader.ts

✅ Also acceptable for utilities:
  helpers.ts
  utils.ts
```

**Do NOT rename files to kebab-case** - maintain consistency with Spark conventions.

#### Constants

Use **UPPER_SNAKE_CASE** for true constants:

```typescript
const MAX_RETRIES = 3;
const DEFAULT_PORT = 15002;
```

### Interface vs Type

- **Prefer `interface`** for object shapes that may be extended
- **Prefer `type`** for unions, intersections, and complex types

```typescript
// ✅ Interface for extensible objects
interface DataFrame {
  columns: string[];
  count(): Promise<number>;
}

// ✅ Type for unions
type ColumnReference = string | Column;

// ✅ Type for complex combinations
type WriteOptions = Partial<SaveOptions> & { mode?: SaveMode };
```

### Async/Await

#### Always Use Async/Await

Prefer `async/await` over raw Promises:

```typescript
// ❌ Avoid
function getData(): Promise<Data> {
  return client.fetch()
    .then(response => response.data)
    .catch(error => {
      console.error(error);
      throw error;
    });
}

// ✅ Prefer
async function getData(): Promise<Data> {
  try {
    const response = await client.fetch();
    return response.data;
  } catch (error) {
    console.error(error);
    throw error;
  }
}
```

#### Error Handling

Always wrap awaits in try/catch blocks:

```typescript
// ✅ Good error handling
async function processDataFrame(df: DataFrame): Promise<void> {
  try {
    const count = await df.count();
    console.log(`Rows: ${count}`);
  } catch (error) {
    logger.error('Failed to process DataFrame', error);
    throw new SparkRuntimeException('Processing failed', error);
  }
}
```

## Code Organization

### Imports

Organize imports in this order:

1. External packages
2. Generated protobuf code
3. Internal modules (absolute paths from src)
4. Relative imports

```typescript
// ✅ Well-organized imports
import { logger } from 'log4js';
import { ExecutePlanRequest } from '../../../gen/spark/connect/base_pb';
import { Client } from './grpc/Client';
import { PlanBuilder } from './proto/PlanBuilder';
import { Column } from './Column';
```

### Function Organization

Keep functions focused and extract helpers:

```typescript
// ✅ Good - Single responsibility
async function validateAndExecute(plan: Plan): Promise<Result> {
  validate(plan);
  return await execute(plan);
}

function validate(plan: Plan): void {
  if (!plan.root) {
    throw new Error('Invalid plan: missing root');
  }
}

async function execute(plan: Plan): Promise<Result> {
  // execution logic
}
```

### Class Structure

Organize class members in this order:

1. Static properties
2. Static methods
3. Instance properties
4. Constructor
5. Public methods
6. Protected methods
7. Private methods

```typescript
class DataFrame {
  // Static
  private static readonly DEFAULT_LIMIT = 20;
  
  // Instance properties
  private readonly _relation: Relation;
  private _plan: Plan;
  
  // Constructor
  constructor(session: SparkSession, relation: Relation) {
    this._relation = relation;
  }
  
  // Public methods
  public select(...cols: string[]): DataFrame { }
  
  // Private methods
  private buildPlan(): Plan { }
}
```

## Documentation

### JSDoc Comments

Add JSDoc comments to all public APIs:

```typescript
/**
 * Creates a DataFrame from the given data and schema.
 * 
 * @param data - Array of objects representing rows
 * @param schema - Optional StructType defining the schema
 * @returns A new DataFrame
 * @throws {SparkRuntimeException} If data format is invalid
 * 
 * @example
 * ```typescript
 * const data = [{ name: 'Alice', age: 30 }];
 * const df = spark.createDataFrame(data);
 * ```
 */
createDataFrame(data: object[], schema?: StructType): DataFrame {
  // implementation
}
```

### Inline Comments

Add comments for complex logic or non-obvious decisions:

```typescript
// Convert Arrow schema to Spark schema for compatibility
const sparkSchema = ArrowUtils.fromArrowSchema(arrowSchema);

// Note: We use BigInt here because JavaScript Number cannot safely
// represent integers larger than 2^53 - 1
const rowCount = BigInt(response.metrics.numRows);
```

### Comment Style

```typescript
// ✅ Use single-line comments for brief explanations
// This validates the input before processing

/* ✅ Use multi-line comments for longer explanations */
/*
 * This function handles the complex conversion between Spark types
 * and Arrow types. The conversion must preserve nullability and
 * metadata while handling nested structures.
 */

/** ✅ Use JSDoc for documentation */
/**
 * Filters rows using the given condition.
 * @param condition - Boolean expression or SQL string
 */
```

## Code Quality

### Avoid Deep Nesting

```typescript
// ❌ Avoid deep nesting
function process(data: Data) {
  if (data) {
    if (data.valid) {
      if (data.value) {
        return transform(data.value);
      }
    }
  }
  return null;
}

// ✅ Guard clauses and early returns
function process(data: Data) {
  if (!data || !data.valid || !data.value) {
    return null;
  }
  return transform(data.value);
}
```

### Type Guards

Use type guards for external data:

```typescript
// ✅ Type guard
function isErrorResponse(response: unknown): response is ErrorResponse {
  return (
    typeof response === 'object' &&
    response !== null &&
    'error' in response &&
    typeof (response as any).error === 'string'
  );
}

// Usage
if (isErrorResponse(data)) {
  console.error(data.error); // TypeScript knows error exists
}
```

### Resource Cleanup

Ensure proper disposal of resources:

```typescript
// ✅ Proper resource management
async function executeQuery(query: string): Promise<Result> {
  const stream = client.executePlan(request);
  try {
    const result = await processStream(stream);
    return result;
  } finally {
    // Always clean up
    stream.cancel();
  }
}
```

## Formatting

### Indentation

- **2 spaces** (configured in .vscode/settings.json)
- No tabs

### Line Length

- Aim for **100 characters** maximum
- Break long lines logically

```typescript
// ✅ Good line breaks
const df = spark.read
  .option('header', true)
  .option('inferSchema', true)
  .csv('data.csv');

// ✅ Good parameter breaks
function complexFunction(
  firstParam: string,
  secondParam: number,
  thirdParam: ComplexType
): ReturnType {
  // implementation
}
```

### Quotes

- **Single quotes** for strings
- Template literals for string interpolation

```typescript
const name = 'spark.js';
const message = `Welcome to ${name}`;
```

## License Headers

**ALL source files** must include the Apache License 2.0 header:

```typescript
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
```

## ESLint Configuration

The project uses ESLint with TypeScript support:

- Configuration: `eslint.config.mjs`
- Generated code (`src/gen/**/*`) is ignored
- Rules:
  - `@typescript-eslint/no-explicit-any`: OFF (but use sparingly)
  - `@typescript-eslint/no-unused-expressions`: OFF

Run linting:
```bash
npm run lint
```

## Security Best Practices

1. **Validate external input** - Use type guards for protobuf responses
2. **Avoid dynamic code execution** - No `eval()` or `Function()` constructors
3. **Parameterize queries** - Use proper parameter handling with Spark SQL
4. **Keep dependencies updated** - Monitor security advisories

## Performance Considerations

1. **Lazy-load heavy dependencies** when possible
2. **Batch operations** instead of making individual calls
3. **Track resource lifetimes** to prevent memory leaks (especially gRPC streams)
4. **Dispose resources deterministically** using proper cleanup patterns

## Testing Guidelines

When writing code, also write tests:

1. **Unit tests** for individual functions/methods
2. **Integration tests** for cross-module behavior
3. **Edge case tests** including error conditions
4. **Use descriptive test names** that explain what's being tested

```typescript
describe('DataFrame', () => {
  it('should filter rows using a boolean column expression', async () => {
    const df = spark.range(1, 100);
    const filtered = df.filter(col('id').gt(50));
    const count = await filtered.count();
    expect(count).toBe(49);
  });
});
```

See [BUILD_AND_TEST.md](BUILD_AND_TEST.md) for more testing details.
