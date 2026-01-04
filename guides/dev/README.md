# Developer Guides

Welcome to the spark.js developer documentation! These guides will help you contribute to the project.

## Getting Started

New to spark.js development? Start here:

1. **[Getting Started](getting-started.md)** - Set up your development environment
2. **[Building](building.md)** - Build the project from source
3. **[Testing](testing.md)** - Run and write tests

## Development Workflow

### Quick Reference Commands

```bash
# Install dependencies
npm install

# Run linter
npm run lint

# Run tests (requires Spark Connect server)
npm test

# Build the project
npm run build

# Generate API documentation
npm run docs

# Generate protobuf code
npx buf generate
```

### Core Guides

- **[Code Style](code-style.md)** - Coding standards and conventions
- **[Testing](testing.md)** - Testing infrastructure and best practices
- **[Documentation](documentation.md)** - Writing and maintaining documentation
- **[Debugging](debugging.md)** - Debugging techniques and tools

### Advanced Topics

- **[Architecture](architecture.md)** - System architecture and design
- **[Protobuf](protobuf.md)** - Working with Protocol Buffers
- **[Release](release.md)** - Release process and versioning

## Project Structure

```
spark.js/
├── src/                      # Source code
│   ├── gen/                  # Generated protobuf code (DO NOT EDIT)
│   └── org/apache/spark/     # Main source code
├── tests/                    # Test files
├── example/                  # Example applications
├── protobuf/                 # Protocol buffer definitions
├── guides/                   # Documentation
│   ├── user/                 # User guides
│   ├── dev/                  # Developer guides (you are here)
│   └── api/                  # Generated API docs
└── .github/                  # GitHub workflows and configs
```

## Development Environment

### Required Tools

- **Node.js**: 18.x or higher (LTS recommended)
- **npm**: 8.x or higher
- **Docker**: For running Spark Connect server
- **Git**: For version control

### Optional Tools

- **VS Code**: Recommended editor (includes TypeScript support)
- **ts-node**: For running TypeScript files directly

### First-Time Setup

```bash
# Clone the repository
git clone https://github.com/yaooqinn/spark.js.git
cd spark.js

# Install dependencies
npm install

# Build the project
npm run build

# Run tests (start Docker server first)
docker build -t scs .github/docker
docker run --name sparkconnect -p 15002:15002 -d scs
npm test
```

## Common Tasks

### Making Code Changes

1. Create a feature branch: `git checkout -b feature/my-feature`
2. Make your changes in `src/`
3. Run linter: `npm run lint`
4. Build: `npm run build`
5. Test: `npm test`
6. Commit and push

### Adding New Features

See [Architecture](architecture.md) for understanding the codebase structure:
- DataFrame operations go in `src/org/apache/spark/sql/DataFrame.ts`
- SQL functions go in `src/org/apache/spark/sql/functions.ts`
- Type definitions go in `src/org/apache/spark/sql/types/`

### Fixing Bugs

1. Write a failing test that reproduces the bug
2. Fix the code
3. Verify the test passes
4. Submit a pull request

## Resources

- **[GitHub Repository](https://github.com/yaooqinn/spark.js)**
- **[Issue Tracker](https://github.com/yaooqinn/spark.js/issues)**
- **[Pull Requests](https://github.com/yaooqinn/spark.js/pulls)**
- **[Apache Spark Documentation](https://spark.apache.org/docs/latest/)**
- **[Spark Connect Protocol](https://github.com/apache/spark/tree/master/connector/connect)**

## Getting Help

- Check existing [issues](https://github.com/yaooqinn/spark.js/issues)
- Read the [user guides](../user/README.md)
- Review [example code](../../example/)
- Ask questions by creating a new issue

## Next Steps

- Read [Getting Started](getting-started.md) to set up your environment
- Learn about [Code Style](code-style.md) conventions
- Understand the [Architecture](architecture.md)
- Check out [Testing](testing.md) practices
