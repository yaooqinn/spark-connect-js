# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-05

### Added

#### Core Features
- **SparkSession**: Main entry point for Spark operations with connection to Spark Connect server
- **DataFrame API**: Comprehensive DataFrame operations including:
  - Selection and projection (`select`, `selectExpr`, `drop`, `withColumn`, `withColumnRenamed`)
  - Filtering (`filter`, `where`)
  - Aggregation (`groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max`)
  - Joins (`join`, `crossJoin`)
  - Ordering (`orderBy`, `sort`)
  - Transformations (`distinct`, `limit`, `sample`, `union`, `intersect`, `except`)
  - Actions (`collect`, `show`, `count`, `head`, `first`, `take`)
- **SQL Support**: Execute SQL queries directly via `spark.sql()`
- **DataFrameReader**: Read data from various formats:
  - CSV, JSON, Parquet, ORC, JDBC
  - Support for schema inference and custom schemas
  - Configurable read options
- **DataFrameWriter**: Write data to various formats:
  - Support for different save modes (Overwrite, Append, ErrorIfExists, Ignore)
  - Partitioning and bucketing support
  - Custom write options
- **Catalog API**: Metadata operations for databases, tables, and functions
- **Type System**: Comprehensive type definitions for Spark data types
  - Primitive types (IntegerType, LongType, StringType, etc.)
  - Complex types (ArrayType, MapType, StructType)
  - Type inference and validation
- **Apache Arrow Integration**: Efficient data transfer using Apache Arrow format
- **Column Expressions**: Rich expression API with functions support
- **Statistical Functions**: DataFrame statistical operations (corr, cov, approxQuantile, etc.)
- **NA Functions**: Handle missing data (drop, fill, replace)
- **Storage Levels**: Control DataFrame persistence and caching

#### SQL Functions
- Over 200+ built-in SQL functions including:
  - String functions (concat, substring, trim, regexp_extract, etc.)
  - Date/time functions (current_date, date_add, datediff, etc.)
  - Math functions (abs, sqrt, exp, log, etc.)
  - Aggregate functions (sum, avg, count, collect_list, etc.)
  - Window functions (row_number, rank, lead, lag, etc.)
  - Collection functions (array_contains, explode, map_keys, etc.)

#### Developer Experience
- **TypeScript-first**: Full TypeScript support with comprehensive type definitions
- **ESLint Configuration**: Code quality enforcement
- **Jest Testing**: Comprehensive test suite with 179 tests across 22 test suites
- **Documentation**: Auto-generated API documentation using TypeDoc
- **Examples**: Sample code for common use cases (CSV, Parquet, Pi calculation, etc.)
- **CI/CD**: GitHub Actions workflows for linting, testing, and documentation

#### Infrastructure
- **gRPC Client**: Protocol Buffers-based communication with Spark Connect
- **Protocol Builders**: Type-safe builders for Spark Connect protocol messages
- **Error Handling**: Custom error types (AnalysisException, SparkRuntimeException, etc.)
- **Logging**: Log4js integration for debugging and monitoring

### Known Limitations
- ⚠️ **Experimental**: APIs may change in future releases
- Requires Apache Spark 4.0+ with Spark Connect enabled
- Node.js 20+ required
- Some advanced Spark features may not be fully implemented
- Limited UDF (User-Defined Function) support

### Development
- Built with TypeScript 5.x targeting ES2020
- Uses Node.js LTS (v20+)
- Protocol Buffers code generation via Buf
- Apache License 2.0

[0.1.0]: https://github.com/yaooqinn/spark.js/releases/tag/v0.1.0
