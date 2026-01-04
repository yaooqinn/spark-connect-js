# Documentation Writing Guide

Guidelines for writing and maintaining documentation in spark.js.

## Overview

spark.js uses multiple documentation formats:
- **JSDoc comments** in source code for API documentation
- **Markdown guides** in `guides/` for user and developer docs
- **TypeDoc** for generating API reference from JSDoc

## JSDoc Standards

### Public API Documentation

All public classes, methods, and functions must have JSDoc comments:

```typescript
/**
 * Creates a DataFrame representing the range of numbers.
 * 
 * @param start - Starting value (inclusive). If only one argument, this is the end value and start is 0.
 * @param end - Ending value (exclusive). Optional if only start is provided.
 * @param step - Step size. Default is 1.
 * @param numPartitions - Number of partitions. Optional.
 * @returns A DataFrame with a single column named 'id'
 * 
 * @example
 * ```typescript
 * // Create range from 0 to 9
 * const df = spark.range(10);
 * 
 * // Create range from 5 to 14
 * const df = spark.range(5, 15);
 * 
 * // Create range with step
 * const df = spark.range(0, 10, 2);  // 0, 2, 4, 6, 8
 * ```
 * 
 * @group Core Functions
 */
range(start: number, end?: number, step?: number, numPartitions?: number): DataFrame {
  // Implementation
}
```

### Required Tags

- **`@param`**: Describe each parameter
  - Format: `@param paramName - Description`
  - Include type constraints, defaults, and optional info
  
- **`@returns`**: Describe return value
  - What is returned and its type
  - For Promises, describe what resolves

- **`@throws`** (when applicable): Document exceptions
  - List conditions that cause errors
  - Error types thrown

### Optional Tags

- **`@example`**: Show usage examples
  - Use code blocks with TypeScript syntax
  - Include practical, realistic examples
  - Show common use cases

- **`@remarks`**: Additional notes
  - Implementation details
  - Performance considerations
  - Compatibility notes

- **`@group`**: Categorize for TypeDoc
  - Groups related functions in documentation
  - Common groups: Core, Transformations, Actions, Aggregations, etc.

- **`@deprecated`**: Mark deprecated APIs
  - Include alternative to use
  - Planned removal version

### Class Documentation

```typescript
/**
 * Main entry point for Spark Connect functionality.
 * 
 * A SparkSession represents a connection to a Spark Connect server
 * and provides methods for creating DataFrames, executing SQL, and
 * managing configuration.
 * 
 * @example
 * ```typescript
 * const spark = await SparkSession.builder()
 *   .remote('sc://localhost:15002')
 *   .appName('MyApp')
 *   .build();
 * 
 * const df = spark.range(100);
 * await df.show();
 * 
 * await spark.stop();
 * ```
 * 
 * @group Core
 */
export class SparkSession {
  // ...
}
```

### Method Documentation

```typescript
/**
 * Returns a new DataFrame with the specified column added or replaced.
 * 
 * If a column with the same name already exists, it will be replaced.
 * 
 * @param colName - Name of the column to add or replace
 * @param col - Column expression or value
 * @returns New DataFrame with the column added
 * 
 * @example
 * ```typescript
 * import { col, lit } from 'spark.js';
 * 
 * const df = spark.range(5);
 * 
 * // Add a constant column
 * const df1 = df.withColumn('constant', lit(42));
 * 
 * // Add a computed column
 * const df2 = df.withColumn('doubled', col('id').multiply(lit(2)));
 * 
 * // Replace existing column
 * const df3 = df.withColumn('id', col('id').plus(lit(1)));
 * ```
 * 
 * @group Transformations
 */
withColumn(colName: string, col: Column): DataFrame {
  // Implementation
}
```

## TypeDoc Configuration

TypeDoc generates API documentation from JSDoc comments.

### typedoc.json

```json
{
  "$schema": "https://typedoc.org/schema.json",
  "entryPoints": ["src/org/apache/spark"],
  "entryPointStrategy": "expand",
  "out": "guides/api",
  "readme": "guides/user/README.md",
  "name": "spark.js API Documentation",
  "categorizeByGroup": true,
  "navigationLinks": {
    "User Guides": "../user/README.md",
    "Developer Guides": "../dev/README.md",
    "GitHub": "https://github.com/yaooqinn/spark.js"
  }
}
```

### Generating Docs

```bash
npm run docs
```

Output goes to `guides/api/` (excluded from git).

### Categorization with @group

Use `@group` tags to organize documentation:

```typescript
/**
 * @group Core
 */
class SparkSession { }

/**
 * @group DataFrame Operations
 */
class DataFrame { }

/**
 * @group SQL Functions
 */
export function col(name: string): Column { }

/**
 * @group Aggregations
 */
export function sum(col: Column): Column { }
```

Groups appear in TypeDoc navigation.

## Markdown Documentation

### User Guides

Located in `guides/user/`, user guides explain how to use spark.js.

#### Structure

```markdown
# Guide Title

Brief introduction explaining what this guide covers.

## Prerequisites

What the user needs to know before reading.

## Main Content

### Subsection 1

Content with examples.

### Subsection 2

More content.

## Examples

Practical, complete examples.

## Next Steps

Links to related guides.
```

#### Example Template

````markdown
# Feature Name

Learn how to use [feature] in spark.js.

## Overview

Brief explanation of the feature and when to use it.

## Basic Usage

```typescript
import { SparkSession } from 'spark.js';

const spark = await SparkSession.builder()
  .remote('sc://localhost:15002')
  .build();

// Example usage
const df = spark.range(10);
await df.show();
```

## Advanced Usage

More complex examples.

## Common Patterns

### Pattern 1

```typescript
// Example code
```

### Pattern 2

```typescript
// Example code
```

## Best Practices

1. **Practice 1**: Explanation
2. **Practice 2**: Explanation

## Troubleshooting

**Problem**: Description

**Solution**: How to fix

## Next Steps

- [Related Guide 1](link)
- [Related Guide 2](link)
````

### Developer Guides

Located in `guides/dev/`, developer guides explain how to contribute.

#### Structure

```markdown
# Development Topic

Guide for developers working on spark.js.

## Prerequisites

Required knowledge and tools.

## Quick Reference

Common commands or quick facts.

## Detailed Explanation

In-depth content.

## Examples

Practical examples.

## Troubleshooting

Common issues and solutions.

## Next Steps

Related developer guides.
```

## Writing Style

### Be Clear and Concise

```markdown
<!-- Good -->
Use `spark.range(10)` to create a DataFrame with 10 rows.

<!-- Bad -->
The range function, when invoked with a single numeric parameter, will
generate a DataFrame object containing rows from zero up to but not
including the specified value.
```

### Use Active Voice

```markdown
<!-- Good -->
Run `npm install` to install dependencies.

<!-- Bad -->
Dependencies should be installed by running `npm install`.
```

### Provide Context

```markdown
<!-- Good -->
Tests require a Spark Connect server running on port 15002.
Start the server with `docker run...` before running tests.

<!-- Bad -->
Run tests with `npm test`.
```

### Show, Don't Just Tell

Always include code examples:

```markdown
<!-- Good -->
Filter a DataFrame using a condition:

```typescript
const filtered = df.filter('age > 18');
const result = await filtered.collect();
```

<!-- Bad -->
You can filter DataFrames using the filter method.
```

## Code Examples

### Complete and Runnable

Examples should be complete enough to run:

```typescript
// Good - complete example
import { SparkSession } from 'spark.js';
import { col, lit } from 'spark.js';

async function example() {
  const spark = await SparkSession.builder()
    .remote('sc://localhost:15002')
    .build();
    
  const df = spark.range(10);
  const result = df.withColumn('doubled', col('id').multiply(lit(2)));
  await result.show();
  
  await spark.stop();
}

// Bad - incomplete fragment
const result = df.withColumn('doubled', col('id').multiply(lit(2)));
```

### Include Expected Output

When helpful, show expected output:

```typescript
const df = spark.range(5);
await df.show();
// Output:
// +---+
// | id|
// +---+
// |  0|
// |  1|
// |  2|
// |  3|
// |  4|
// +---+
```

### Use TypeScript Syntax

All examples should be TypeScript:

````markdown
```typescript
const df = spark.range(10);
```
````

## Cross-Referencing

### Link Related Docs

```markdown
See [Basic Concepts](basic-concepts.md) for an introduction.

For more details, check the [API Reference](../api/).

Learn about [Testing](testing.md) in the developer guide.
```

### Use Relative Links

```markdown
<!-- From guides/user/ -->
[Quick Start](quick-start.md)
[API Docs](../api/)
[Dev Guide](../dev/README.md)

<!-- From guides/dev/ -->
[User Guide](../user/README.md)
[Testing](testing.md)
```

## Maintenance

### Keep Docs Updated

- Update docs when changing functionality
- Remove or update stale examples
- Check links periodically
- Update version numbers

### Version-Specific Info

Note version-specific features:

```markdown
The `DataFrame.writeTo()` method is available in Spark 4.0+.

**Note**: This feature requires Spark Connect 4.1 or higher.
```

## Review Checklist

Before submitting documentation:

- [ ] All public APIs have JSDoc comments
- [ ] JSDoc includes `@param` and `@returns`
- [ ] Code examples are complete and runnable
- [ ] Examples use TypeScript syntax
- [ ] Links work correctly
- [ ] Spelling and grammar checked
- [ ] Follows style guide
- [ ] Builds without warnings: `npm run docs`

## Next Steps

- See [Code Style](code-style.md) for coding standards
- Review [Architecture](architecture.md) for system design
- Check [Testing](testing.md) for test documentation
