# Quick Start Guide

Get started with spark.js in just a few minutes!

## Prerequisites

- Node.js 18.x or higher (recommended: latest LTS version)
- npm or yarn
- A running Spark Connect server

## Installation

Install spark.js via npm:

```bash
npm install spark.js
```

Or using yarn:

```bash
yarn add spark.js
```

## Basic Usage

Here's a simple example to get you started:

```typescript
import { SparkSession } from 'spark.js';

// Create a SparkSession
const spark = await SparkSession.builder()
  .remote('sc://localhost:15002')
  .appName('MyFirstSparkApp')
  .build();

// Create a DataFrame
const df = spark.range(0, 10);

// Show the data
await df.show();

// Perform operations
const result = df
  .filter('id > 5')
  .select('id')
  .collect();

console.log(result);

// Close the session
await spark.stop();
```

## Running Your First Example

1. **Start a Spark Connect Server**

   The easiest way is using Docker:

   ```bash
   docker run -p 15002:15002 apache/spark:4.1.0 \
     /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:4.1.0
   ```

2. **Create a TypeScript file** (e.g., `example.ts`):

   ```typescript
   import { SparkSession } from 'spark.js';

   async function main() {
     const spark = await SparkSession.builder()
       .remote('sc://localhost:15002')
       .appName('QuickStart')
       .build();

     // Create a simple DataFrame
     const df = spark.range(0, 5);
     await df.show();

     await spark.stop();
   }

   main().catch(console.error);
   ```

3. **Run the example**:

   ```bash
   npx ts-node example.ts
   ```

## Common Operations

### Reading Data

```typescript
// Read CSV
const csvDf = spark.read()
  .option('header', 'true')
  .csv('path/to/file.csv');

// Read Parquet
const parquetDf = spark.read().parquet('path/to/file.parquet');

// Read JSON
const jsonDf = spark.read().json('path/to/file.json');
```

### Transforming Data

```typescript
// Select columns
const selected = df.select('column1', 'column2');

// Filter rows
const filtered = df.filter('age > 18');

// Add a new column
import { col, lit } from 'spark.js';
const withColumn = df.withColumn('new_col', col('old_col').plus(lit(1)));

// Group and aggregate
const grouped = df.groupBy('category').count();
```

### Writing Data

```typescript
// Write as Parquet
await df.write().parquet('output/path');

// Write as CSV
await df.write()
  .option('header', 'true')
  .csv('output/path');

// Write as JSON
await df.write().json('output/path');
```

## Next Steps

- Learn about [Basic Concepts](basic-concepts.md) to understand DataFrame, Column, and more
- Explore [SQL Functions](sql-functions.md) for data transformation
- Read about [Data I/O](data-io.md) for advanced reading and writing operations
- Check out the [API Reference](../api/) for detailed documentation

## Troubleshooting

**Connection refused**: Make sure your Spark Connect server is running and accessible at the specified URL.

**Module not found**: Ensure spark.js is properly installed with `npm install spark.js`.

For more help, see the [Troubleshooting Guide](troubleshooting.md).
