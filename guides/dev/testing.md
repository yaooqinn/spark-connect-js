# Testing Guide

Complete guide to testing spark.js, including running tests and writing new ones.

## Overview

spark.js uses **Jest** with **ts-jest** for testing. Tests are written in TypeScript and run against a live Spark Connect server.

**Key Facts**:
- Test framework: Jest v29.x
- TypeScript transformer: ts-jest v29.x  
- Test timeout: 30 seconds (configurable)
- Server requirement: Spark Connect server must be running
- Test count: 22 test suites, 179 tests

## Prerequisites

### Required Tools

- **Node.js** 18.x or higher
- **npm** 8.x or higher
- **Docker** (for Spark Connect server)
- **Dependencies installed**: Run `npm install`

### Test Infrastructure

Tests require a running Spark Connect server. **Tests will hang indefinitely if the server is not running.**

## Running Tests

### Complete Test Setup Sequence

Follow this exact sequence to run tests successfully:

#### 1. Build the Docker Image

First time only (takes ~20-25 seconds):

```bash
docker build -t scs .github/docker
```

This builds a Docker image with Spark 4.1.0 and Spark Connect enabled.

**Image size**: ~700MB+
**Build time**: 20-25 seconds

#### 2. Start the Spark Connect Server

```bash
docker run --name sparkconnect -p 15002:15002 -d scs
```

This starts the server in detached mode on port 15002.

#### 3. Wait for Server to Start

**Critical**: Wait 15-20 seconds for the server to fully initialize.

Check server logs:

```bash
docker logs sparkconnect
```

Look for this line:
```
Spark Connect server started at: [::]:15002
```

#### 4. Run Tests

```bash
npm test
```

**Test execution time**: ~80-100 seconds

#### 5. Stop and Clean Up

After testing:

```bash
docker stop sparkconnect
docker rm sparkconnect
```

### Quick Test Commands

```bash
# Run all tests
npm test

# Run specific test file
npx jest SparkSession.test.ts

# Run tests matching pattern
npx jest --testNamePattern="should create DataFrame"

# Run tests with coverage
npx jest --coverage

# Run tests in watch mode
npx jest --watch
```

## Test Configuration

### Jest Configuration

**jest.config.js**:
```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testTimeout: 30000,  // 30 seconds
  collectCoverage: true,
  coverageDirectory: 'coverage',
  coveragePathIgnorePatterns: [
    '/node_modules/',
    '/dist/',
    '/src/gen/'  // Ignore generated protobuf code
  ],
  setupFilesAfterEnv: ['./jest.setup.js']
};
```

### Jest Setup

**jest.setup.js**:
```javascript
// Enable BigInt JSON serialization
BigInt.prototype.toJSON = function() {
  return this.toString();
};
```

This allows BigInt values to be serialized in test snapshots and logs.

## Test Structure

### Directory Layout

Tests mirror the source code structure:

```
tests/
├── helpers.ts                   # Shared test utilities
├── utils.test.ts               # Utility tests
└── org/apache/spark/sql/       # SQL API tests
    ├── SparkSession.test.ts
    ├── DataFrame.test.ts
    ├── DataFrameCollect.test.ts
    ├── functions/              # Function tests
    │   ├── aggregate.test.ts
    │   ├── string.test.ts
    │   └── ...
    └── types/                  # Type system tests
        └── DataTypes.test.ts
```

### Test Naming Convention

- Test files: `*.test.ts`
- Location: Mirror source structure in `tests/`
- Test suites: Use `describe()` blocks
- Test cases: Use `test()` or `it()`

## Writing Tests

### Basic Test Template

```typescript
import { SparkSession } from '../../src/org/apache/spark/sql/SparkSession';
import { sharedSpark } from '../helpers';

describe('FeatureName', () => {
  let spark: SparkSession;

  beforeAll(async () => {
    spark = sharedSpark;
  });

  afterAll(async () => {
    // Cleanup if needed
  });

  test('should do something', async () => {
    // Arrange
    const df = spark.range(0, 10);
    
    // Act
    const result = await df.count();
    
    // Assert
    expect(result).toBe(10);
  });
});
```

### Using Shared SparkSession

**Recommended**: Use the shared SparkSession from helpers:

```typescript
import { sharedSpark } from '../helpers';

describe('MyTests', () => {
  const spark = sharedSpark;
  
  test('example', async () => {
    const df = spark.range(5);
    await df.show();
  });
});
```

**helpers.ts** creates a single SparkSession connected to `sc://localhost:15002/`.

### Testing Patterns

#### Test DataFrame Operations

```typescript
test('should filter DataFrame', async () => {
  const df = spark.range(0, 10);
  const filtered = df.filter('id > 5');
  const count = await filtered.count();
  
  expect(count).toBe(4);  // 6, 7, 8, 9
});
```

#### Test Transformations

```typescript
import { col, lit } from '../../src/org/apache/spark/sql/functions';

test('should add column', async () => {
  const df = spark.range(0, 5);
  const result = df.withColumn('doubled', col('id').multiply(lit(2)));
  
  const rows = await result.collect();
  expect(rows[0].get('doubled')).toBe(0);
  expect(rows[1].get('doubled')).toBe(2);
});
```

#### Test Aggregations

```typescript
import { sum, avg } from '../../src/org/apache/spark/sql/functions';

test('should aggregate data', async () => {
  const df = spark.range(1, 6);  // 1, 2, 3, 4, 5
  const result = df.agg(
    sum('id').alias('total'),
    avg('id').alias('average')
  );
  
  const row = await result.first();
  expect(row.get('total')).toBe(15);
  expect(row.get('average')).toBe(3);
});
```

#### Test Error Conditions

```typescript
test('should throw error for invalid column', async () => {
  const df = spark.range(5);
  
  await expect(async () => {
    await df.select('non_existent_column').collect();
  }).rejects.toThrow();
});
```

#### Test Async Operations

Always use `async/await` and `await` actions:

```typescript
test('should collect results', async () => {
  const df = spark.range(3);
  const rows = await df.collect();  // Must await
  
  expect(rows).toHaveLength(3);
  expect(rows[0].get('id')).toBe(0);
});
```

### Test Data

Use `spark.range()` for simple numeric data:

```typescript
const df = spark.range(0, 100);  // 0 to 99
const df = spark.range(100);     // 0 to 99
```

For complex data, read from test files in `example/org/apache/spark/sql/example/data/`:

```typescript
const df = spark.read()
  .option('header', 'true')
  .csv('example/org/apache/spark/sql/example/data/people.csv');
```

## Best Practices

### Do's

✅ **Use the shared SparkSession** from `helpers.ts`
✅ **Always await actions** (count, collect, show, etc.)
✅ **Test edge cases** (empty DataFrames, null values, etc.)
✅ **Use descriptive test names** ("should filter rows when condition matches")
✅ **Clean up resources** if creating temporary files
✅ **Test error conditions** (invalid inputs, etc.)

### Don'ts

❌ **Don't create new SparkSessions** in every test (slow and wasteful)
❌ **Don't forget to await** async operations
❌ **Don't use brittle timing assertions** (use proper waits)
❌ **Don't modify shared test data** (use copies or spark.range())
❌ **Don't test generated protobuf code** (in `src/gen/`)

## Coverage

### Generating Coverage Reports

Coverage is collected automatically:

```bash
npm test
```

View coverage report:

```bash
# Open in browser
open coverage/lcov-report/index.html

# Or use a coverage tool
npx jest --coverage --coverageReporters=text
```

### Coverage Configuration

Coverage excludes:
- `node_modules/`
- `dist/`
- `src/gen/` (generated protobuf code)

Target: Aim for >80% coverage on new code.

## Debugging Tests

### Running Single Test

```bash
# Run specific file
npx jest SparkSession.test.ts

# Run specific test
npx jest -t "should create SparkSession"
```

### Verbose Output

```bash
npx jest --verbose
```

### Debug Mode

```bash
# Node inspector
node --inspect-brk node_modules/.bin/jest --runInBand
```

Then attach a debugger (VS Code, Chrome DevTools).

### Check Server Logs

If tests hang or fail to connect:

```bash
docker logs sparkconnect
```

### Increase Timeout

For slow operations, increase timeout in the test:

```typescript
test('slow operation', async () => {
  // Test code
}, 60000);  // 60 second timeout
```

## Troubleshooting

### Tests Hang Indefinitely

**Cause**: Spark Connect server not running or not ready

**Solutions**:
1. Check server is running: `docker ps | grep sparkconnect`
2. Check server logs: `docker logs sparkconnect`
3. Wait 15-20 seconds after starting server
4. Restart server:
   ```bash
   docker stop sparkconnect && docker rm sparkconnect
   docker run --name sparkconnect -p 15002:15002 -d scs
   ```

### Connection Refused Errors

**Cause**: Server not listening on port 15002

**Solutions**:
1. Verify port mapping: `docker port sparkconnect`
2. Check firewall settings
3. Ensure server started successfully

### Test Failures After Changes

1. Rebuild the project: `npm run build`
2. Restart the server
3. Clear Jest cache: `npx jest --clearCache`
4. Run tests again

### Memory Issues

For large test suites:

```bash
node --max-old-space-size=4096 node_modules/.bin/jest
```

## Continuous Integration

Tests run automatically in GitHub Actions:

**.github/workflows/ci.yml**:
```yaml
- name: Build Docker image
  run: docker build -t scs .github/docker

- name: Start Spark Connect server
  run: docker run --name sparkconnect -p 15002:15002 -d scs

- name: Wait for server
  run: sleep 20

- name: Run tests
  run: npm test

- name: Stop server
  run: docker stop sparkconnect
```

## Performance Tips

1. **Reuse SparkSession**: Use `sharedSpark` from helpers
2. **Use spark.range()**: Faster than creating complex test data
3. **Limit data size**: Use `.limit()` for large datasets in tests
4. **Run tests in parallel**: Jest runs tests in parallel by default
5. **Cache Docker image**: Build once, reuse many times

## Writing Good Tests

### Arrange-Act-Assert Pattern

```typescript
test('should transform data', async () => {
  // Arrange
  const df = spark.range(0, 10);
  
  // Act
  const result = df.filter('id > 5');
  const count = await result.count();
  
  // Assert
  expect(count).toBe(4);
});
```

### Test One Thing

```typescript
// Good: Tests one behavior
test('should filter rows by condition', async () => {
  const df = spark.range(10);
  const filtered = df.filter('id > 5');
  expect(await filtered.count()).toBe(4);
});

// Bad: Tests multiple things
test('should do everything', async () => {
  const df = spark.range(10);
  expect(await df.count()).toBe(10);
  expect(await df.filter('id > 5').count()).toBe(4);
  expect(await df.select('id').count()).toBe(10);
  // ... too many assertions
});
```

### Use Descriptive Names

```typescript
// Good
test('should return empty DataFrame when filtering with impossible condition', async () => {
  // ...
});

// Bad
test('test1', async () => {
  // ...
});
```

## Next Steps

- Review [Code Style](code-style.md) for coding standards
- Learn about [Debugging](debugging.md) techniques
- See [Building](building.md) for build process
- Check [Architecture](architecture.md) for system design
