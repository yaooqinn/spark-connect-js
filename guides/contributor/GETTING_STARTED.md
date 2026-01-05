# Getting Started

This guide will help you set up your development environment for contributing to spark.js.

## Prerequisites

### Required Software

- **Node.js**: Version 20 or higher
  - Recommended: Use [nvm](https://github.com/nvm-sh/nvm) to manage Node.js versions
  - CI/CD uses Node.js 23
- **npm**: Comes with Node.js (v10+)
- **Docker**: Required for running Spark Connect server during tests
  - [Docker Desktop](https://www.docker.com/products/docker-desktop) for macOS/Windows
  - [Docker Engine](https://docs.docker.com/engine/install/) for Linux
- **Git**: For version control

### Optional Tools

- **Visual Studio Code**: Recommended IDE (see [IDE Setup](IDE_SETUP.md))
- **TypeScript**: Globally installed for better CLI experience
  ```bash
  npm install -g typescript
  ```

## Initial Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub first, then:
git clone https://github.com/YOUR_USERNAME/spark.js.git
cd spark.js

# Add upstream remote
git remote add upstream https://github.com/yaooqinn/spark.js.git
```

### 2. Install Dependencies

```bash
npm install
```

This will install:
- TypeScript compiler and type definitions
- Jest testing framework
- ESLint for code linting
- Protocol buffer tools (Buf)
- All runtime dependencies

### 3. Verify Installation

```bash
# Check Node.js version
node --version  # Should be v20 or higher

# Check TypeScript
npx tsc --version

# Check that dependencies are installed
npm list --depth=0
```

## Spark Connect Server Setup

The test suite requires a running Spark Connect server. You have two options:

### Option 1: Docker (Recommended)

```bash
# Build the Docker image (first time only, takes ~20-30 seconds)
docker build -t scs .github/docker

# Start the server
docker run --name sparkconnect -p 15002:15002 -d scs

# Wait 15-20 seconds for server to be ready
# Check logs to confirm:
docker logs sparkconnect

# Look for this line:
# "Spark Connect server started at: [::]:15002"

# When done testing:
docker stop sparkconnect
docker rm sparkconnect
```

### Option 2: Local Spark Installation

If you have Apache Spark 4.0+ installed locally:

```bash
# Start Spark Connect server
$SPARK_HOME/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.0.0
```

## Verify Your Setup

### Run Type Checking

```bash
npx tsc --noEmit
```

Should complete without errors.

### Run Linting

```bash
npm run lint
```

Should pass with no errors.

### Run Tests

```bash
# Make sure Spark Connect server is running first!
npm test
```

Should see output like:
```
Test Suites: 28 passed, 28 total
Tests:       179 passed, 179 total
```

## Directory Structure

Understanding the project structure:

```
spark.js/
├── src/                          # Source code
│   ├── gen/                      # Generated protobuf code (DO NOT EDIT)
│   ├── org/apache/spark/
│   │   ├── sql/                  # Main API implementation
│   │   │   ├── SparkSession.ts   # Entry point
│   │   │   ├── DataFrame.ts      # DataFrame API
│   │   │   ├── functions.ts      # SQL functions
│   │   │   ├── types/            # Spark type system
│   │   │   ├── catalog/          # Catalog API
│   │   │   ├── grpc/             # gRPC client
│   │   │   └── proto/            # Protocol builders
│   │   └── storage/              # Storage levels
│   └── utils.ts                  # Utility functions
├── tests/                        # Test suites
│   ├── helpers.ts                # Test utilities
│   └── org/apache/spark/sql/     # Tests mirroring src/
├── example/                      # Example applications
├── guides/                       # Documentation
│   └── contributor/              # This guide
├── protobuf/                     # Protocol buffer definitions
│   └── spark/connect/            # Spark Connect proto files
├── .github/
│   ├── workflows/                # CI/CD workflows
│   │   ├── ci.yml                # Test workflow
│   │   ├── linter.yml            # Linting workflow
│   │   └── docs.yml              # Documentation workflow
│   ├── docker/                   # Spark Connect Docker setup
│   └── copilot-instructions.md   # AI assistant guidelines
├── package.json                  # Dependencies and scripts
├── tsconfig.json                 # TypeScript configuration
├── jest.config.js                # Jest test configuration
├── eslint.config.mjs             # ESLint configuration
├── buf.gen.yaml                  # Buf protobuf generation config
└── log4js.json                   # Logging configuration
```

## Next Steps

- Read the [Code Style Guide](CODE_STYLE.md) to understand coding conventions
- Learn about [Build and Test](BUILD_AND_TEST.md) workflows
- When ready, learn how to [submit changes](SUBMITTING_CHANGES.md)

## Common Issues

### Tests Hang Indefinitely

**Symptom**: Tests start but never complete.

**Cause**: Spark Connect server is not running or not ready.

**Solution**:
1. Verify server is running: `docker ps | grep sparkconnect`
2. Check logs: `docker logs sparkconnect`
3. Wait 15-20 seconds after starting the server

### Docker Build Fails

**Symptom**: Docker build command fails.

**Cause**: Docker not running or insufficient resources.

**Solution**:
1. Ensure Docker Desktop/Engine is running
2. Increase Docker memory allocation to at least 4GB
3. Check Docker logs for specific errors

### npm install Warnings

**Symptom**: Warnings about deprecated packages during `npm install`.

**Cause**: Transitive dependencies from older packages.

**Solution**: These warnings are normal and can be ignored. They don't affect functionality.
