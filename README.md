# Apache Spark Connect Client for JavaScript

[![CI](https://github.com/yaooqinn/spark.js/actions/workflows/ci.yml/badge.svg)](https://github.com/yaooqinn/spark.js/actions/workflows/ci.yml)
[![Documentation](https://github.com/yaooqinn/spark.js/actions/workflows/docs.yml/badge.svg)](https://github.com/yaooqinn/spark.js/actions/workflows/docs.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An **experimental** client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) written in [TypeScript](https://www.typescriptlang.org/). This library allows JavaScript and TypeScript applications to interact with Apache Spark using the Spark Connect protocol over gRPC.

> ⚠️ **Experimental**: This library is in active development. APIs may change without notice.

## Documentation

📚 **[API Documentation](https://yaooqinn.github.io/spark.js/)** - Comprehensive API reference with examples

The API documentation is automatically generated from JSDoc comments and TypeScript definitions.

## Features

- 🚀 **TypeScript-first**: Full TypeScript support with comprehensive type definitions
- 🔗 **Spark Connect Protocol**: Uses the official Spark Connect gRPC protocol
- 📊 **DataFrame API**: Familiar DataFrame operations (select, filter, groupBy, join, etc.)
- 💾 **Multiple Data Formats**: Support for CSV, JSON, Parquet, ORC, and more
- 🔍 **SQL Support**: Execute SQL queries directly
- 📝 **Catalog API**: Metadata operations (databases, tables, functions)
- ⚡ **Apache Arrow**: Efficient data transfer using Apache Arrow
- 🎯 **Type-safe**: Built-in type system for Spark data types

## Prerequisites

- **Node.js**: v20 or higher
- **Apache Spark**: 4.0+ with Spark Connect enabled
  - For development/testing: Docker is used to run Spark Connect server

## Installation

Install from npm:

```bash
npm install @yaooqinn/spark.js
```

Or clone the repository for development:

```bash
git clone https://github.com/yaooqinn/spark.js.git
cd spark.js
npm install
```

## Quick Start

Here's a minimal example to get started:

```typescript
import { SparkSession } from '@yaooqinn/spark.js';

async function main() {
  // Create a SparkSession connected to a Spark Connect server
  const spark = await SparkSession.builder()
    .appName('MyApp')
    .remote('sc://localhost:15002')
    .getOrCreate();

  // Create a simple DataFrame
  const df = spark.range(1, 100)
    .selectExpr('id', 'id * 2 as doubled');
  
  // Show the first 10 rows
  await df.show(10);

  // Perform aggregation
  const count = await df.count();
  console.log(`Total rows: ${count}`);
}

main().catch(console.error);
```

## Usage

### Creating a SparkSession

The `SparkSession` is the entry point for all Spark operations:

```typescript
import { SparkSession } from '@yaooqinn/spark.js';

// Connect to a remote Spark Connect server
const spark = await SparkSession.builder()
  .appName('MyApplication')
  .remote('sc://localhost:15002')  // Spark Connect endpoint
  .getOrCreate();

// Get Spark version
const version = await spark.version();
console.log(`Spark version: ${version}`);
```

### Reading Data

Use the `DataFrameReader` to load data from various sources:

```typescript
// Read CSV file
const csvDF = spark.read
  .option('header', true)
  .option('delimiter', ';')
  .csv('path/to/people.csv');

// Read JSON file
const jsonDF = spark.read.json('path/to/data.json');

// Read Parquet file
const parquetDF = spark.read.parquet('path/to/data.parquet');

// Read with schema inference
const df = spark.read
  .option('inferSchema', true)
  .option('header', true)
  .csv('data.csv');
```

### DataFrame Operations

Perform transformations and actions on DataFrames:

```typescript
import { functions } from '@yaooqinn/spark.js';
const { col, lit } = functions;

// Select columns
const selected = df.select('name', 'age');

// Filter rows
const filtered = df.filter(col('age').gt(21));

// Add/modify columns
const transformed = df
  .withColumn('age_plus_one', col('age').plus(lit(1)))
  .withColumnRenamed('name', 'full_name');

// Group by and aggregate
const aggregated = df
  .groupBy('department')
  .agg({ salary: 'avg', age: 'max' });

// Join DataFrames
const joined = df1.join(df2, df1.col('id').equalTo(df2.col('user_id')), 'inner');

// Sort
const sorted = df.orderBy(col('age').desc());

// Limit
const limited = df.limit(100);

// Collect results
const rows = await df.collect();
rows.forEach(row => console.log(row.toJSON()));
```

### SQL Queries

Execute SQL queries directly:

```typescript
// Register DataFrame as temporary view
df.createOrReplaceTempView('people');

// Execute SQL
const resultDF = await spark.sql(`
  SELECT name, age, department
  FROM people
  WHERE age > 30
  ORDER BY age DESC
`);

await resultDF.show();
```

### Writing Data

Save DataFrames to various formats:

```typescript
// Write as Parquet (default mode: error if exists)
await df.write.parquet('output/data.parquet');

// Write as CSV with options
await df.write
  .option('header', true)
  .option('delimiter', '|')
  .mode('overwrite')
  .csv('output/data.csv');

// Write as JSON
await df.write
  .mode('append')
  .json('output/data.json');

// Partition by column
await df.write
  .partitionBy('year', 'month')
  .parquet('output/partitioned_data');

// V2 Writer API (advanced)
await df.writeTo('my_table')
  .using('parquet')
  .partitionBy('year', 'month')
  .tableProperty('compression', 'snappy')
  .create();
```

See [guides/DataFrameWriterV2.md](guides/DataFrameWriterV2.md) for more V2 Writer examples.

### Catalog Operations

Explore metadata using the Catalog API:

```typescript
// List databases
const databases = await spark.catalog.listDatabases();

// List tables in current database
const tables = await spark.catalog.listTables();

// List columns of a table
const columns = await spark.catalog.listColumns('my_table');

// Check if table exists
const exists = await spark.catalog.tableExists('my_table');

// Get current database
const currentDB = await spark.catalog.currentDatabase();
```

## API Overview

### Core Classes

- **`SparkSession`**: Main entry point for Spark functionality
- **`DataFrame`**: Distributed collection of data organized into named columns
- **`Column`**: Expression on a DataFrame column
- **`Row`**: Represents a row of data
- **`DataFrameReader`**: Interface for loading data
- **`DataFrameWriter`**: Interface for saving data
- **`DataFrameWriterV2`**: V2 writer with advanced options
- **`RuntimeConfig`**: Runtime configuration interface
- **`Catalog`**: Metadata and catalog operations

### Functions

Import SQL functions from the functions module:

```typescript
import { functions } from '@yaooqinn/spark.js';
const { col, lit, sum, avg, max, min, count, when, concat, upper } = functions;

const df = spark.read.csv('data.csv');

df.select(
  col('name'),
  upper(col('name')).as('upper_name'),
  when(col('age').gt(18), lit('adult')).otherwise(lit('minor')).as('category')
);
```

See [guides/STATISTICAL_FUNCTIONS.md](guides/STATISTICAL_FUNCTIONS.md) for statistical functions.

### Type System

Define schemas using the type system:

```typescript
import { DataTypes, StructType, StructField } from '@yaooqinn/spark.js';

const schema = new StructType([
  new StructField('name', DataTypes.StringType, false),
  new StructField('age', DataTypes.IntegerType, true),
  new StructField('salary', DataTypes.DoubleType, true)
]);

const df = spark.createDataFrame(data, schema);
```

## Configuration

### Spark Connect Connection

Configure the connection to Spark Connect server:

```typescript
const spark = await SparkSession.builder()
  .appName('MyApp')
  .remote('sc://host:port')  // Default: sc://localhost:15002
  .getOrCreate();
```

### Runtime Configuration

Set Spark configuration at runtime:

```typescript
// Set configuration
await spark.conf.set('spark.sql.shuffle.partitions', '200');

// Get configuration
const value = await spark.conf.get('spark.sql.shuffle.partitions');

// Get with default
const valueOrDefault = await spark.conf.get('my.config', 'default_value');
```

### Logging

Logging is configured in `log4js.json`. Logs are written to both console and `logs/` directory.


## Examples

The `example/` directory contains several runnable examples:

- **[Pi.ts](example/org/apache/spark/sql/example/Pi.ts)**: Monte Carlo Pi estimation
- **[CSVExample.ts](example/org/apache/spark/sql/example/CSVExample.ts)**: Reading and writing CSV files
- **[ParquetExample.ts](example/org/apache/spark/sql/example/ParquetExample.ts)**: Parquet file operations
- **[JsonExample.ts](example/org/apache/spark/sql/example/JsonExample.ts)**: JSON file operations
- **[JoinExample.ts](example/org/apache/spark/sql/example/JoinExample.ts)**: DataFrame join operations
- **[CatalogExample.ts](example/org/apache/spark/sql/example/CatalogExample.ts)**: Catalog API usage
- **[StatisticalFunctionsExample.ts](example/org/apache/spark/sql/example/StatisticalFunctionsExample.ts)**: Statistical functions

To run an example:

```bash
# Make sure Spark Connect server is running
npx ts-node example/org/apache/spark/sql/example/Pi.ts
```

## Contributing

Contributions are welcome! Please read the [Contributor Guide](guides/contributor/) for detailed information.


## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned features and improvements.

## License

This project is licensed under the [Apache License 2.0](LICENSE).

---

**Note**: This is an experimental project. For production use, please refer to the official Apache Spark documentation and consider using official Spark clients.
