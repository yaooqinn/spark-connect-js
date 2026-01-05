# Build and Test

This guide covers building, testing, and running spark.js during development.

## Prerequisites

Before you begin, ensure you have:
- Completed the [Getting Started](GETTING_STARTED.md) setup
- Installed all dependencies with `npm install`
- Docker running (for Spark Connect server)

## Building

### TypeScript Compilation

The project is primarily used as TypeScript source, but you can compile to JavaScript:

```bash
# Type check without emitting files
npx tsc --noEmit

# Build JavaScript to dist/ directory
npx tsc --build

# Clean build artifacts
npx tsc --build --clean
```

**Note**: There is no explicit `build` script in package.json. The repository is used as TypeScript source.

### Protocol Buffer Generation

If you modify `.proto` files in `protobuf/spark/connect/`:

```bash
# Regenerate TypeScript code from proto files
npx buf generate
```

Generated files are placed in `src/gen/` and **must be committed** to the repository.

**Important**: Never manually edit files in `src/gen/` - they are auto-generated.

#### Buf Configuration

- `buf.gen.yaml` - Code generation configuration
- `buf.work.yaml` - Workspace configuration
- Uses `@bufbuild/buf` v1.47.2

## Testing

### Test Framework

- **Framework**: Jest with ts-jest
- **Configuration**: `jest.config.js`
- **Timeout**: 30 seconds per test
- **Setup**: `jest.setup.js` (BigInt.toJSON support)

### Test Structure

Tests mirror the source structure:

```
tests/
├── helpers.ts                    # Shared test utilities
├── utils.test.ts
└── org/apache/spark/sql/
    ├── SparkSession.test.ts      # SparkSession tests
    ├── DataFrame.test.ts         # DataFrame API tests
    ├── DataFrameCollect.test.ts  # Collection tests
    ├── functions.test.ts         # SQL functions tests
    └── ...                       # 28 test suites total
```

### Running Tests

#### Complete Test Setup

**Tests require a running Spark Connect server.** Follow these steps:

```bash
# 1. Build Docker image (first time only, ~20-25 seconds)
docker build -t scs .github/docker

# 2. Start Spark Connect server (port 15002)
docker run --name sparkconnect -p 15002:15002 -d scs

# 3. Wait 15-20 seconds for server to start
docker logs sparkconnect
# Look for: "Spark Connect server started at: [::]:15002"

# 4. Run tests (~80-100 seconds)
npm test

# 5. Clean up when done
docker stop sparkconnect
docker rm sparkconnect
```

#### Running Specific Tests

```bash
# Run a single test file
npm test -- SparkSession.test.ts

# Run tests matching a pattern
npm test -- DataFrame

# Run tests in watch mode
npm test -- --watch

# Run with coverage
npm test -- --coverage
```

#### Test Coverage

Coverage is automatically collected during `npm test`:

```bash
# View coverage in terminal
npm test

# Coverage reports are generated in coverage/
# - coverage/lcov-report/index.html (HTML report)
# - coverage/lcov.info (LCOV format)
# - coverage/clover.xml (Clover format)
```

### Writing Tests

#### Test Helpers

Use `sharedSpark` from `tests/helpers.ts` for SparkSession:

```typescript
import { sharedSpark } from '../helpers';
import { functions } from '../../src/org/apache/spark/sql/functions';

describe('MyFeature', () => {
  it('should work correctly', async () => {
    const spark = await sharedSpark();
    const df = spark.range(1, 10);
    const count = await df.count();
    expect(count).toBe(9);
  });
});
```

#### Test Patterns

```typescript
describe('DataFrame operations', () => {
  let spark: SparkSession;
  
  beforeAll(async () => {
    spark = await sharedSpark();
  });
  
  it('should select columns', async () => {
    const df = spark.range(1, 100);
    const selected = df.select('id');
    const columns = selected.columns;
    expect(columns).toEqual(['id']);
  });
  
  it('should filter rows', async () => {
    const df = spark.range(1, 100);
    const filtered = df.filter(col('id').gt(50));
    const count = await filtered.count();
    expect(count).toBe(49);
  });
  
  it('should handle errors gracefully', async () => {
    const df = spark.range(1, 10);
    await expect(df.select('nonexistent')).rejects.toThrow();
  });
});
```

#### Testing Best Practices

1. **Use descriptive test names** - Explain what's being tested
2. **Test edge cases** - Include boundary conditions and error cases
3. **Keep tests focused** - One concept per test
4. **Use async/await** - All DataFrame operations are async
5. **Clean up resources** - Close connections when needed
6. **Avoid timing assumptions** - Use controlled waits

### Test Statistics

Current test suite:
- **28 test suites**
- **179+ test cases**
- Coverage: DataFrame, SparkSession, SQL functions, types, catalog, etc.

## Linting

### Running ESLint

```bash
# Lint all files
npm run lint

# Lint specific files
npx eslint src/org/apache/spark/sql/DataFrame.ts

# Auto-fix issues where possible
npx eslint --fix src/
```

### ESLint Configuration

- **Config file**: `eslint.config.mjs`
- **TypeScript support**: Enabled via `@typescript-eslint` plugin
- **Ignored paths**: `src/gen/**/*` (generated code)

### Linting Rules

Key ESLint rules:
- `@typescript-eslint/no-explicit-any`: OFF (but use sparingly)
- `@typescript-eslint/no-unused-expressions`: OFF

## Continuous Integration

### GitHub Actions Workflows

The project has two CI workflows that run on every push and pull request:

#### 1. Linter Workflow

File: `.github/workflows/linter.yml`

```yaml
Steps:
  1. Checkout code
  2. Setup Node.js 23
  3. npm install
  4. npm run lint
```

**Must pass for PRs to merge.**

#### 2. CI Workflow

File: `.github/workflows/ci.yml`

```yaml
Steps:
  1. Checkout code
  2. Setup Node.js 23 with npm cache
  3. Build Spark Connect Docker image
  4. Start Spark Connect server
  5. npm install
  6. npm test
  7. Stop server
```

**Must pass for PRs to merge.**

### Running CI Checks Locally

Before submitting a PR, run the same checks CI will run:

```bash
# Linting check
npm run lint

# Full test suite
docker build -t scs .github/docker
docker run --name sparkconnect -p 15002:15002 -d scs
# Wait 15-20 seconds
npm test
docker stop sparkconnect && docker rm sparkconnect
```

## Common Build & Test Issues

### Tests Hang Indefinitely

**Symptom**: Tests start but never complete, no output.

**Cause**: Spark Connect server not running or not ready.

**Solution**:
```bash
# Check if server is running
docker ps | grep sparkconnect

# Check server logs
docker logs sparkconnect

# Restart if needed
docker stop sparkconnect && docker rm sparkconnect
docker run --name sparkconnect -p 15002:15002 -d scs

# Wait 15-20 seconds before running tests
```

### Docker Build Takes Long

**Symptom**: `docker build` takes 20-30 seconds.

**Cause**: Large apache/spark:4.1.0 base image (~700MB+).

**Solution**: This is normal. Build once and reuse the image. The image is cached after first build.

### Port Already in Use

**Symptom**: Error: `port 15002 is already allocated`.

**Cause**: Previous Spark Connect container still running.

**Solution**:
```bash
# Stop and remove existing container
docker stop sparkconnect
docker rm sparkconnect

# Or use different port
docker run --name sparkconnect -p 15003:15002 -d scs
# Update connection in tests/helpers.ts
```

### npm Warnings About Deprecated Packages

**Symptom**: Warnings about `inflight`, `glob`, etc. during `npm install`.

**Cause**: Transitive dependencies from older packages.

**Solution**: These are normal and can be ignored. They don't affect functionality.

### TypeScript Compilation Errors

**Symptom**: `tsc` reports type errors.

**Cause**: Type mismatches or missing definitions.

**Solution**:
```bash
# Check specific error location
npx tsc --noEmit

# Ensure dependencies are up to date
npm install

# Check tsconfig.json settings
cat tsconfig.json
```

### Test Timeout

**Symptom**: Tests fail with `Exceeded timeout of 30000ms`.

**Cause**: Slow network, server not responding, or genuinely slow operation.

**Solution**:
1. Increase timeout in jest.config.js if needed
2. Check server is healthy: `docker logs sparkconnect`
3. Verify network connectivity to localhost:15002

## Performance Tips

### Docker Performance

- **Allocate sufficient memory**: At least 4GB for Docker
- **Use Docker volume caching** if running tests frequently
- **Reuse Docker image**: Don't rebuild unnecessarily

### Test Performance

- **Run specific tests** during development instead of full suite
- **Use watch mode** for iterative development
- **Parallel execution**: Jest runs tests in parallel by default

### Build Performance

- **Use incremental compilation**: TypeScript supports incremental builds
- **Cache node_modules**: CI caches npm packages
- **Skip type checking** during rapid iteration (use editor for feedback)

## Debugging

### Debugging Tests

```bash
# Run tests with verbose output
npm test -- --verbose

# Debug specific test
node --inspect-brk node_modules/.bin/jest SparkSession.test.ts

# Use console.log in tests (Jest captures it)
console.log('Debug:', someValue);
```

### Debugging Application Code

The project uses log4js for logging:

- **Configuration**: `log4js.json`
- **Log directory**: `logs/` (gitignored)
- **Appenders**: Both file and stdout

```typescript
import { logger } from './logger';

logger.debug('Debug message');
logger.info('Info message');
logger.error('Error message', error);
```

### VS Code Debugging

See [IDE_SETUP.md](IDE_SETUP.md) for VS Code debugging configuration.

## Next Steps

- Review [Submitting Changes](SUBMITTING_CHANGES.md) when ready to contribute
- Check [Code Style Guide](CODE_STYLE.md) for coding conventions
