# Building spark.js

Complete guide to building spark.js from source.

## Prerequisites

Before building, ensure you have:

- **Node.js** 18.x or higher (LTS recommended)
- **npm** 8.x or higher
- **Dependencies installed**: Run `npm install` first

## Quick Build

The fastest way to build:

```bash
npm run build
```

This runs `tsc --build` which compiles TypeScript to JavaScript in the `dist/` directory.

**Build time**: ~5-10 seconds on modern hardware

## Dependencies Installation

**ALWAYS run `npm install` before building.** This installs all dependencies required for building, testing, and linting.

```bash
npm install
```

**Installation time**: ~30-60 seconds (first time), ~5-10 seconds (subsequent)

### What Gets Installed

- **TypeScript compiler** (`typescript` v5.x): For compiling TypeScript to JavaScript
- **ESLint** (`eslint`, plugins): For code linting
- **Jest** (`jest`, `ts-jest`): For testing
- **Buf** (`@bufbuild/buf`): For protocol buffer code generation
- **Protobuf libraries** (`@bufbuild/protobuf`): For protobuf support
- **gRPC client** (`@grpc/grpc-js`): For Spark Connect communication
- **Apache Arrow** (`apache-arrow`): For efficient data transfer
- **Development tools**: Type definitions, build tools

## Build Process

### TypeScript Compilation

The main build process compiles TypeScript source to JavaScript:

```bash
# Using npm script
npm run build

# Or directly with tsc
npx tsc --build
```

**Configuration**: `tsconfig.json`

```json
{
  "compilerOptions": {
    "target": "ES2020",           // Output ES2020 JavaScript
    "module": "commonjs",         // CommonJS modules for Node.js
    "declaration": true,          // Generate .d.ts type definitions
    "outDir": "./dist",           // Output directory
    "rootDir": "./src",           // Source directory
    "strict": true,               // Enable all strict type checks
    "esModuleInterop": true,
    "skipLibCheck": true
  }
}
```

### Build Artifacts

After building, the `dist/` directory contains:

```
dist/
├── index.js           # Main entry point
├── index.d.ts         # TypeScript definitions
└── org/apache/spark/  # Compiled source tree
    ├── sql/
    │   ├── SparkSession.js
    │   ├── SparkSession.d.ts
    │   ├── DataFrame.js
    │   ├── DataFrame.d.ts
    │   └── ...
    └── ...
```

**Note**: The `dist/` directory is ignored by git (see `.gitignore`).

## Type Checking

### Type Check Without Building

Verify TypeScript types without emitting files:

```bash
npx tsc --noEmit
```

This is faster than a full build and useful for quick type checks during development.

**Typical time**: ~3-5 seconds

### Incremental Type Checking

TypeScript supports incremental compilation:

```bash
# First build (slower)
npx tsc --build

# Subsequent builds (faster - only changed files)
npx tsc --build
```

## Protocol Buffer Code Generation

If you modify `.proto` files in `protobuf/`, regenerate TypeScript code:

```bash
npx buf generate
```

**Build time**: ~2-3 seconds

### What This Does

1. Reads `buf.gen.yaml` configuration
2. Processes `.proto` files in `protobuf/spark/connect/`
3. Generates TypeScript code in `src/gen/`
4. Uses `@bufbuild/protoc-gen-es` plugin

### Generated Files

Generated files go to `src/gen/spark/connect/`:

```
src/gen/spark/connect/
├── base_pb.ts
├── commands_pb.ts
├── common_pb.ts
├── expressions_pb.ts
├── relations_pb.ts
├── types_pb.ts
└── ...
```

**Important**: 
- **DO NOT** manually edit files in `src/gen/`
- **DO** commit generated files to git
- **DO** run `buf generate` after modifying `.proto` files

### Buf Configuration

**buf.gen.yaml**:
```yaml
version: v2
plugins:
  - remote: buf.build/bufbuild/es:v2.2.3
    out: src/gen
    opt:
      - target=ts
```

**buf.work.yaml**:
```yaml
version: v2
directories:
  - protobuf
```

## Clean Build

To perform a clean build, remove artifacts first:

```bash
# Remove build artifacts
rm -rf dist/

# Rebuild
npm run build
```

For a complete clean including dependencies:

```bash
# Remove everything
rm -rf dist/ node_modules/

# Reinstall and rebuild
npm install
npm run build
```

## Build Troubleshooting

### Build Fails with Type Errors

**Solution**: Ensure dependencies are installed:
```bash
npm install
npx tsc --noEmit  # Check for type errors
```

### Generated Code is Missing

**Solution**: Run buf generate:
```bash
npx buf generate
```

### "Cannot find module" Errors

**Solution**: Clean and reinstall:
```bash
rm -rf node_modules package-lock.json
npm cache clean --force
npm install
```

### Slow Build Times

**Causes**:
- First build after clone (includes type checking for node_modules)
- Large codebase (~75 TypeScript files)

**Solutions**:
- Use `--incremental` flag (already default with `--build`)
- Enable `skipLibCheck` in tsconfig.json (already enabled)
- Use faster disk (SSD vs HDD)

## Build for Production

### Optimized Build

For production use, the TypeScript build is sufficient:

```bash
npm run build
```

The compiled JavaScript in `dist/` is ready for distribution.

### Publishing to npm

Before publishing:

```bash
# Run pre-publish script
npm run prepublishOnly
```

This runs `npm run build` automatically.

The `package.json` specifies what gets published:

```json
{
  "files": [
    "dist",
    "src",
    "LICENSE",
    "README.md"
  ],
  "main": "dist/index.js",
  "types": "dist/index.d.ts"
}
```

## Build Scripts Summary

| Command | Purpose | Time |
|---------|---------|------|
| `npm install` | Install dependencies | 30-60s (first), 5-10s (cached) |
| `npm run build` | Compile TypeScript | 5-10s |
| `npx tsc --noEmit` | Type check only | 3-5s |
| `npx buf generate` | Generate protobuf code | 2-3s |
| `npm run lint` | Run ESLint | 3-5s |
| `npm test` | Run tests (needs server) | 80-100s |
| `npm run docs` | Generate API docs | 10-15s |

## Continuous Integration

GitHub Actions automatically builds on every push/PR:

**.github/workflows/linter.yml**:
```yaml
- run: npm install
- run: npm run lint
```

**.github/workflows/ci.yml**:
```yaml
- run: npm install
- run: npm test  # Also validates build works
```

Both workflows must pass for PR to be merged.

## Development Workflow

### Typical Development Session

```bash
# Start of day
git pull
npm install  # Update dependencies if needed

# During development
npx tsc --noEmit  # Quick type check

# Before committing
npm run lint
npm run build
npm test  # If server is running

# Commit and push
git commit -am "My changes"
git push
```

### Watch Mode

For continuous development, use tsc in watch mode:

```bash
npx tsc --watch
```

This recompiles automatically when files change.

## Next Steps

- Learn about [Testing](testing.md) the build
- Review [Code Style](code-style.md) for code standards
- See [Protobuf](protobuf.md) for working with protocol buffers
- Check [Debugging](debugging.md) for debugging techniques
