# Contributor Guide

Welcome to the spark.js contributor guide! This documentation will help you get started with contributing to the Apache Spark Connect Client for JavaScript.

## 📚 Documentation Index

1. **[Getting Started](GETTING_STARTED.md)** - Set up your development environment
2. **[Code Style Guide](CODE_STYLE.md)** - Coding conventions and best practices
3. **[Build and Test](BUILD_AND_TEST.md)** - Building, testing, and running the project
4. **[Submitting Changes](SUBMITTING_CHANGES.md)** - How to submit pull requests

## Quick Start

### Prerequisites

- **Node.js**: v20 or higher (CI uses Node 23)
- **Docker**: For running Spark Connect server during testing
- **npm**: Package manager

### Setup

```bash
# 1. Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/spark.js.git
cd spark.js

# 2. Install dependencies
npm install

# 3. Start Spark Connect server for testing
docker build -t scs .github/docker
docker run --name sparkconnect -p 15002:15002 -d scs

# 4. Run tests
npm test

# 5. Run linting
npm run lint
```

### Making Your First Contribution

1. **Fork** the repository and create a feature branch
2. **Make changes** following the [Code Style Guide](CODE_STYLE.md)
3. **Add tests** for new functionality
4. **Run linting** and tests locally
5. **Submit a pull request** following the [Submitting Changes](SUBMITTING_CHANGES.md) guide

## Need Help?

- Check existing issues on [GitHub Issues](https://github.com/yaooqinn/spark.js/issues)
- Review the [API Documentation](https://yaooqinn.github.io/spark.js/)
- Look at [example code](../../example/org/apache/spark/sql/example/) for reference

## Code of Conduct

This project follows the Apache Software Foundation's Code of Conduct. Please be respectful and constructive in all interactions.
