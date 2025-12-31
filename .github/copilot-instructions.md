# Copilot Instructions for spark.js

## Repository Overview

This is an **experimental** Apache Spark Connect client for JavaScript/TypeScript. It allows JavaScript applications to interact with Apache Spark through the Spark Connect protocol using gRPC.

**Key Facts:**
- **Language:** TypeScript (compiled to JavaScript)
- **Runtime:** Node.js (always update/use latest LTS, e.g., Node 23.x)
- **Package Manager:** npm
- **Testing Framework:** Jest with ts-jest
- **Linter:** ESLint with TypeScript plugin
- **Build Tool:** TypeScript compiler (tsc)
- **Protocol Buffers:** Buf for code generation
- **Repository Size:** ~75 TypeScript source files, 8 proto-generated files

## Critical Build & Test Requirements

### Dependencies Installation
**ALWAYS run `npm install` before any other command.** This installs all dependencies required for building, testing, and linting.

```bash
npm install
```

### Testing Requirements

**Tests require a running Spark Connect server.** Tests will hang indefinitely if the server is not running.

#### Complete Test Setup Sequence:
1. Build the Docker image (takes ~20-25 seconds):
   ```bash
   docker build -t scs .github/docker
   ```

2. Start the Spark Connect server (port 15002):
   ```bash
   docker run --name sparkconnect -p 15002:15002 -d scs
   ```

3. Wait 15-20 seconds for the server to fully start (check logs):
   ```bash
   docker logs sparkconnect
   ```
   Look for: `Spark Connect server started at: [::]:15002`

4. Run tests (~80-100 seconds):
   ```bash
   npm test
   ```

5. Stop and clean up after testing:
   ```bash
   docker stop sparkconnect
   docker rm sparkconnect
   ```

**Important:** If tests hang or fail to connect, the server may not be ready. Wait longer or check `docker logs sparkconnect`.

### Linting

Run before committing code changes:
```bash
npm run lint
```

ESLint is configured in `eslint.config.mjs` with TypeScript support. The `src/gen/**/*` directory (generated protobuf code) is ignored.

### TypeScript Type Checking

Verify types without emitting files:
```bash
npx tsc --noEmit
```

Build compiled JavaScript (output to `dist/`):
```bash
npx tsc --build
```

**Note:** There is no explicit `build` script in package.json. The repository is primarily used as TypeScript source.

### Protocol Buffer Generation

If you modify `.proto` files in `protobuf/`, regenerate TypeScript code:
```bash
npx buf generate
```

Generated files are placed in `src/gen/` and should be committed. Uses `@bufbuild/buf` v1.47.2.

## Project Structure

### Root Directory Files
- `package.json` - Dependencies and scripts (test, lint)
- `tsconfig.json` - TypeScript compiler configuration (target: ES2020, module: commonjs)
- `eslint.config.mjs` - ESLint configuration (ignores src/gen/)
- `jest.config.js` - Jest test configuration (30s timeout, ts-jest transform)
- `jest.setup.js` - BigInt.toJSON serialization support for Jest
- `log4js.json` - Logging configuration (file + stdout appenders)
- `buf.gen.yaml` - Buf protobuf code generation config
- `buf.work.yaml` - Buf workspace configuration
- `.gitignore` - Excludes: .vscode/, coverage/, logs/, node_modules/, dist/

### Source Code Layout

```
src/
├── gen/                         # Generated protobuf TypeScript code (DO NOT EDIT)
│   └── spark/connect/           # Generated from protobuf/*.proto files
├── org/apache/spark/
│   ├── logger.ts                # Log4js logger configuration
│   ├── sql/
│   │   ├── SparkSession.ts      # Main entry point (~234 lines)
│   │   ├── DataFrame.ts         # DataFrame API (~550 lines)
│   │   ├── DataFrameReader.ts   # Data reading interface
│   │   ├── DataFrameWriter.ts   # Data writing interface
│   │   ├── SparkResult.ts       # Query result handling
│   │   ├── Row.ts               # Row data structure
│   │   ├── functions.ts         # Spark SQL functions (~776 lines)
│   │   ├── RuntimeConfig.ts     # Configuration management
│   │   ├── SaveMode.ts          # Write save modes
│   │   ├── errors.ts            # Error handling
│   │   ├── grpc/                # gRPC client implementation
│   │   │   ├── Client.ts        # gRPC client
│   │   │   └── client_builder.ts
│   │   ├── proto/               # Protobuf message builders
│   │   │   ├── PlanBuilder.ts
│   │   │   ├── RelationBuilder.ts
│   │   │   ├── CommandBuilder.ts
│   │   │   ├── ExpressionBuilder.ts
│   │   │   ├── AnalyzePlanRequestBuilder.ts
│   │   │   └── ...
│   │   ├── types/               # Data type definitions
│   │   │   ├── DataTypes.ts     # Type factory (~324 lines)
│   │   │   ├── StructType.ts
│   │   │   ├── StructField.ts
│   │   │   └── ... (30+ type files)
│   │   ├── arrow/               # Apache Arrow integration
│   │   │   └── ArrowUtils.ts
│   │   ├── catalog/             # Catalog API (metadata)
│   │   │   ├── Catalog.ts
│   │   │   ├── Database.ts
│   │   │   ├── Table.ts
│   │   │   └── Function.ts
│   │   └── util/
│   │       ├── CaseInsensitiveMap.ts
│   │       └── helpers.ts
│   └── storage/
│       └── StorageLevel.ts
└── utils.ts                     # Utility functions (PlanIdGenerator)

tests/
├── helpers.ts                   # Test utilities (sharedSpark instance)
├── utils.test.ts
└── org/apache/spark/sql/        # Test files mirroring src/ structure
    ├── SparkSession.test.ts
    ├── DataFrame.test.ts
    ├── DataFrameCollect.test.ts
    └── ... (22 test suites, 179 tests)

example/
└── org/apache/spark/sql/example/
    ├── Pi.ts, CSVExample.ts, ParquetExample.ts, etc.
    └── data/                    # Sample data files
```

### Protobuf Files
Located in `protobuf/spark/connect/`:
- `base.proto` - Core message types
- `commands.proto` - Command messages
- `relations.proto` - Relation messages
- `expressions.proto` - Expression messages
- `types.proto` - Type definitions
- `catalog.proto` - Catalog operations
- `common.proto` - Common definitions
- `example_plugins.proto` - Plugin examples

## Coding Conventions

### License Headers
**ALL source files** must include the Apache License 2.0 header (see existing files for template).

### TypeScript & JavaScript Target
- **TypeScript Version:** 5.x
- **Target:** ES2020 (configured in tsconfig.json)
- **Module System:** CommonJS (for Node.js compatibility)
- **Strict Mode:** Enabled - leverage TypeScript's type safety features

### Code Style
- **Indentation:** 2 spaces (configured in `.vscode/settings.json`)
- **Naming Conventions:**
  - PascalCase for classes, interfaces, enums, and type aliases
  - camelCase for methods, variables, and functions
  - Avoid interface prefixes like `I` - rely on descriptive names
- **File Naming:** Follow existing patterns (PascalCase for classes, camelCase preferred for utilities)
- **Imports:** Use relative paths; generated protobuf imports use absolute from src/gen/
- **Code Organization:** Keep functions focused and extract helpers when logic branches grow
- **Space vs. Tabs:** Use spaces only, no spaces for alignment

### Type System Guidelines
- **Minimize `any` usage:** While `@typescript-eslint/no-explicit-any` is OFF, prefer `unknown` with type narrowing when dealing with uncertain types
- **Use discriminated unions** for complex state or message types
- **Express intent** with TypeScript utility types (`Readonly`, `Partial`, `Record`, etc.)
- **Type guards:** Use type guards and validators for external data
- **Centralize shared types:** Avoid duplicating type definitions

### Async & Error Handling
- **Prefer `async/await`** over raw Promises for readability
- **Wrap awaits in try/catch** blocks with structured error handling
- **Guard edge cases early** to avoid deep nesting
- **Error reporting:** Use the project's logging utilities (log4js)
- **Resource cleanup:** Ensure proper disposal of resources (connections, streams)

### ESLint Rules
- `@typescript-eslint/no-explicit-any`: OFF (any is allowed, but use sparingly)
- `@typescript-eslint/no-unused-expressions`: OFF

### Security Practices
- **Validate external input:** Use schema validators or type guards for protobuf responses
- **Avoid dynamic code execution:** No `eval()` or `Function()` constructors
- **Parameterized queries:** When working with Spark SQL, use proper parameter handling
- **Dependency management:** Keep dependencies updated and monitor security advisories

### Performance & Reliability
- **Lazy-load heavy dependencies** when possible
- **Batch or debounce** high-frequency operations
- **Track resource lifetimes** to prevent memory leaks (especially gRPC streams)
- **Dispose resources deterministically** using proper cleanup patterns

### Documentation
- **JSDoc for public APIs:** Add JSDoc comments to classes, methods, and functions
- **Include `@param`, `@returns`, and `@throws`** in JSDoc as appropriate
- **Add `@remarks` or `@example`** for complex APIs
- **Keep comments up-to-date:** Remove stale comments during refactors
- **Document design decisions:** Add comments explaining non-obvious implementation choices

### Testing Patterns
- Use `sharedSpark` from `tests/helpers.ts` for SparkSession instance
- Connect to `sc://localhost:15002/` (see helpers.ts)
- Tests use Jest with 30-second timeout
- Coverage is collected automatically
- **Add or update unit tests** when modifying existing functionality
- **Integration tests** should be added for cross-module behavior
- **Avoid brittle timing assertions:** Use fake timers or controlled waits
- **Test edge cases** including error conditions and boundary values

## GitHub Actions CI/CD

### Two Workflows

1. **Linter** (`.github/workflows/linter.yml`)
   - Runs on: push to main, pull requests
   - Steps: Checkout → Setup Node 23 → npm install → npm run lint
   - **Must pass for PRs to merge**

2. **CI** (`.github/workflows/ci.yml`)
   - Runs on: push to main, pull requests
   - Steps:
     1. Checkout code
     2. Setup Node 23 with npm cache
     3. Build Spark Connect server Docker image
     4. Start Spark Connect server container (port 15002)
     5. npm install
     6. npm test
     7. Stop server
   - **Must pass for PRs to merge**

**Both workflows use concurrency control** to cancel in-progress runs when new commits are pushed.

## Common Development Tasks

### Making Code Changes
1. Run `npm install` (if not done)
2. Make changes to TypeScript files in `src/`
3. Run `npm run lint` to check style
4. Run `npx tsc --noEmit` to verify types
5. If tests are needed, start Docker server and run `npm test`
6. Commit changes (linting and tests will run in CI)

### Adding New Features
- Main API entry point: `SparkSession` class
- DataFrame operations: `DataFrame` class
- SQL functions: `src/org/apache/spark/sql/functions.ts`
- Type system: `src/org/apache/spark/sql/types/`
- Protocol builders: `src/org/apache/spark/sql/proto/`

### Modifying Protobuf
1. Edit `.proto` files in `protobuf/spark/connect/`
2. Run `npx buf generate`
3. Commit both `.proto` and generated `src/gen/` files

### Debugging Tests
- Check server logs: `docker logs sparkconnect`
- Increase Jest timeout if needed (currently 30s in jest.config.js)
- Use log4js for application logging (configured in log4js.json)
- Logs written to `logs/` directory (ignored by git)

## Known Issues & Workarounds

### Test Hangs
**Symptom:** Tests hang indefinitely without output.
**Cause:** Spark Connect server not running or not ready.
**Solution:** 
- Verify server is running: `docker ps | grep sparkconnect`
- Check server logs: `docker logs sparkconnect`
- Wait 15-20 seconds after starting server

### Docker Build Time
**Symptom:** Docker build takes 20-30 seconds.
**Cause:** Large apache/spark:4.1.0 base image (~700MB+).
**Solution:** Normal behavior. Build once and reuse the image.

### npm Warnings
**Symptom:** Warnings about deprecated packages (inflight, glob).
**Cause:** Transitive dependencies from older packages.
**Solution:** Ignore - these are from dependencies and don't affect functionality.

## Trust These Instructions

The information in this file has been validated by running all commands and verifying successful execution. When in doubt:
- **DO** trust these build sequences and command orders
- **DO** follow the exact Docker setup steps for tests
- **DO** run `npm install` before any other npm command
- **DON'T** skip waiting for the Spark Connect server to start
- **DON'T** attempt to run tests without the Docker server
- **DON'T** modify files in `src/gen/` - they are auto-generated

If you encounter errors not documented here, search the codebase for TODOs and check recent commit history for changes in build processes.
