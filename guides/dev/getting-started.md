# Getting Started

Set up your development environment to contribute to spark.js.

## Prerequisites

### Required Software

1. **Node.js** (v18.x or higher, LTS recommended)
   - Download from [nodejs.org](https://nodejs.org/)
   - Verify: `node --version`

2. **npm** (v8.x or higher, comes with Node.js)
   - Verify: `npm --version`

3. **Docker** (for running tests)
   - Download from [docker.com](https://www.docker.com/)
   - Verify: `docker --version`

4. **Git** (for version control)
   - Download from [git-scm.com](https://git-scm.com/)
   - Verify: `git --version`

### Optional Tools

- **VS Code**: Recommended editor with excellent TypeScript support
- **ts-node**: Run TypeScript files directly without compilation
  ```bash
  npm install -g ts-node
  ```

## Initial Setup

### 1. Fork and Clone

Fork the repository on GitHub, then clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/spark.js.git
cd spark.js
```

Add the upstream repository:

```bash
git remote add upstream https://github.com/yaooqinn/spark.js.git
```

### 2. Install Dependencies

**This step is critical** - run it before any other command:

```bash
npm install
```

This installs all dependencies needed for development, testing, and building.

### 3. Verify Installation

Check that everything is installed correctly:

```bash
# Check Node.js version
node --version  # Should be 18.x or higher

# Check npm version
npm --version   # Should be 8.x or higher

# Check TypeScript
npx tsc --version  # Should be 5.x

# List installed packages
npm list --depth=0
```

## Development Environment Configuration

### VS Code Setup

If using VS Code, the repository includes recommended settings in `.vscode/settings.json`:

```json
{
  "editor.tabSize": 2,
  "editor.insertSpaces": true,
  "typescript.tsdk": "node_modules/typescript/lib"
}
```

Install recommended extensions:
- ESLint
- TypeScript
- Prettier (if used)

### TypeScript Configuration

The project uses TypeScript 5.x with these settings (see `tsconfig.json`):

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

## First Build

### Build the Project

```bash
npm run build
```

This compiles TypeScript to JavaScript in the `dist/` directory.

**Note**: The build script uses `tsc --build`. The primary use of this project is as TypeScript source, not compiled JavaScript.

### Verify Type Checking

Check types without emitting files:

```bash
npx tsc --noEmit
```

This should complete without errors.

## Running Your First Test

### Set Up Spark Connect Server

Tests require a running Spark Connect server. Use Docker for easy setup:

1. **Build the Docker image** (~20-25 seconds):
   ```bash
   docker build -t scs .github/docker
   ```

2. **Start the server** (port 15002):
   ```bash
   docker run --name sparkconnect -p 15002:15002 -d scs
   ```

3. **Wait for server to start** (15-20 seconds):
   ```bash
   docker logs sparkconnect
   ```
   Look for: `Spark Connect server started at: [::]:15002`

### Run Tests

```bash
npm test
```

Tests will take ~80-100 seconds to complete.

### Stop the Server

After testing:

```bash
docker stop sparkconnect
docker rm sparkconnect
```

## Code Quality Checks

### Linting

Run ESLint to check code style:

```bash
npm run lint
```

Fix auto-fixable issues:

```bash
npm run lint -- --fix
```

### Type Checking

Verify TypeScript types:

```bash
npx tsc --noEmit
```

## Understanding the Codebase

### Project Structure

```
spark.js/
├── src/                          # Source code
│   ├── gen/                      # Generated protobuf (DO NOT EDIT)
│   └── org/apache/spark/         # Main source
│       ├── sql/                  # SQL API
│       │   ├── SparkSession.ts   # Entry point
│       │   ├── DataFrame.ts      # DataFrame operations
│       │   ├── functions.ts      # SQL functions
│       │   └── types/            # Type system
│       └── storage/              # Storage utilities
├── tests/                        # Test files
├── example/                      # Example applications
├── protobuf/                     # Protocol buffer definitions
└── guides/                       # Documentation
```

### Key Files

- **src/org/apache/spark/sql/SparkSession.ts**: Main entry point for the API
- **src/org/apache/spark/sql/DataFrame.ts**: DataFrame implementation
- **src/org/apache/spark/sql/functions.ts**: Built-in SQL functions
- **tests/helpers.ts**: Test utilities including shared SparkSession

See [Architecture](architecture.md) for detailed information.

## Making Your First Change

### 1. Create a Branch

```bash
git checkout -b feature/my-feature
```

### 2. Make Changes

Edit files in the `src/` directory. For example, add a new function:

```typescript
// src/org/apache/spark/sql/functions.ts

export function myNewFunction(col: Column): Column {
  // Implementation
}
```

### 3. Check Your Changes

```bash
# Lint
npm run lint

# Type check
npx tsc --noEmit

# Build
npm run build

# Test
npm test
```

### 4. Commit and Push

```bash
git add .
git commit -m "Add myNewFunction"
git push origin feature/my-feature
```

### 5. Create Pull Request

Go to GitHub and create a pull request from your branch.

## Development Workflow

### Daily Development

```bash
# Update from upstream
git fetch upstream
git merge upstream/main

# Create feature branch
git checkout -b feature/my-feature

# Make changes, test, commit

# Push and create PR
git push origin feature/my-feature
```

### Before Committing

Always run before committing:

```bash
npm run lint
npx tsc --noEmit
npm test  # If you have the Docker server running
```

## Common Issues

### npm install fails

Try:
```bash
rm -rf node_modules package-lock.json
npm cache clean --force
npm install
```

### Tests hang

- Ensure Spark Connect server is running: `docker ps`
- Check server logs: `docker logs sparkconnect`
- Wait longer for server to start (15-20 seconds)

### TypeScript errors

- Update TypeScript: `npm install -D typescript@latest`
- Clear build cache: `rm -rf dist/`

## Next Steps

- Read [Building](building.md) for detailed build instructions
- Learn about [Testing](testing.md) practices
- Review [Code Style](code-style.md) guidelines
- Explore [Architecture](architecture.md) to understand the design

## Getting Help

- Check [Troubleshooting](../user/troubleshooting.md)
- Search [GitHub Issues](https://github.com/yaooqinn/spark.js/issues)
- Create a new issue with your question
