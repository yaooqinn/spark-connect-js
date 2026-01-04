# Data I/O Guide

Learn how to read and write data in various formats using spark.js.

## Reading Data

### DataFrameReader

Access the reader through `spark.read()`:

```typescript
const reader = spark.read();
```

### Reading CSV Files

```typescript
// Basic CSV read
const df = spark.read().csv('path/to/file.csv');

// With options
const df = spark.read()
  .option('header', 'true')
  .option('inferSchema', 'true')
  .option('delimiter', ',')
  .csv('path/to/file.csv');

// Multiple files
const df = spark.read()
  .option('header', 'true')
  .csv('path/to/directory/*.csv');
```

**Common CSV Options:**
- `header`: Whether the first row is a header (`true`/`false`)
- `inferSchema`: Automatically infer column types (`true`/`false`)
- `delimiter`: Column delimiter (default: `,`)
- `quote`: Quote character (default: `"`)
- `escape`: Escape character
- `nullValue`: String that represents null
- `dateFormat`: Date format string
- `timestampFormat`: Timestamp format string

### Reading Parquet Files

Parquet is a columnar storage format, efficient for analytics:

```typescript
// Read Parquet
const df = spark.read().parquet('path/to/file.parquet');

// Read multiple files
const df = spark.read().parquet('path/to/directory/*.parquet');

// Read partitioned data
const df = spark.read().parquet('path/to/partitioned/data/');
```

### Reading JSON Files

```typescript
// Read JSON
const df = spark.read().json('path/to/file.json');

// With options
const df = spark.read()
  .option('multiLine', 'true')
  .json('path/to/file.json');
```

**JSON Options:**
- `multiLine`: Parse multi-line JSON objects (`true`/`false`)
- `allowComments`: Allow Java/C++ style comments
- `allowUnquotedFieldNames`: Allow unquoted JSON field names

### Reading Text Files

```typescript
// Read as text (one line per row)
const df = spark.read().text('path/to/file.txt');
```

### Reading with Schema

For better performance and type safety, specify a schema:

```typescript
import { DataTypes } from 'spark.js';

const schema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('name', DataTypes.StringType, true),
  DataTypes.createStructField('age', DataTypes.IntegerType, true),
  DataTypes.createStructField('salary', DataTypes.DoubleType, true)
]);

const df = spark.read()
  .schema(schema)
  .csv('path/to/file.csv');
```

### Reading from Different Sources

```typescript
// Local file system
const df = spark.read().csv('file:///path/to/local/file.csv');

// HDFS
const df = spark.read().csv('hdfs://namenode:8020/path/to/file.csv');

// S3
const df = spark.read().csv('s3a://bucket/path/to/file.csv');

// Azure Blob Storage
const df = spark.read().csv('wasbs://container@account.blob.core.windows.net/path');
```

## Writing Data

### DataFrameWriter

Access the writer through `df.write()`:

```typescript
const writer = df.write();
```

### Writing CSV Files

```typescript
// Basic CSV write
await df.write().csv('output/path');

// With options
await df.write()
  .option('header', 'true')
  .option('delimiter', ',')
  .csv('output/path');
```

### Writing Parquet Files

```typescript
// Write Parquet
await df.write().parquet('output/path');

// With compression
await df.write()
  .option('compression', 'snappy')
  .parquet('output/path');
```

**Parquet Compression Options:**
- `none`: No compression
- `snappy`: Fast compression (default)
- `gzip`: Better compression, slower
- `lzo`: LZO compression
- `zstd`: Zstandard compression

### Writing JSON Files

```typescript
await df.write().json('output/path');
```

### Save Modes

Control how data is saved when the output already exists:

```typescript
import { SaveMode } from 'spark.js';

// Error if exists (default)
await df.write()
  .mode(SaveMode.ErrorIfExists)
  .parquet('output/path');

// Append to existing data
await df.write()
  .mode(SaveMode.Append)
  .parquet('output/path');

// Overwrite existing data
await df.write()
  .mode(SaveMode.Overwrite)
  .parquet('output/path');

// Ignore if exists (no-op)
await df.write()
  .mode(SaveMode.Ignore)
  .parquet('output/path');
```

### Partitioned Writes

Write data partitioned by column values:

```typescript
// Partition by single column
await df.write()
  .partitionBy('year')
  .parquet('output/path');

// Partition by multiple columns
await df.write()
  .partitionBy('year', 'month', 'day')
  .parquet('output/path');
```

This creates a directory structure like:
```
output/path/
├── year=2024/
│   ├── month=01/
│   │   └── day=01/
│   │       └── part-00000.parquet
│   └── month=02/
│       └── day=01/
│           └── part-00000.parquet
```

### Bucketing

Organize data into a fixed number of buckets:

```typescript
await df.write()
  .bucketBy(10, 'user_id')
  .sortBy('timestamp')
  .saveAsTable('bucketed_table');
```

## Advanced I/O

### Reading with Filter Pushdown

For formats that support predicate pushdown (like Parquet), filters can be applied during read:

```typescript
const df = spark.read()
  .parquet('data/users.parquet')
  .filter('age > 18');  // May be pushed down to file read
```

### Writing to Tables

Write to Spark tables (requires a metastore):

```typescript
// Save as table
await df.write()
  .saveAsTable('my_table');

// Insert into table
await df.write()
  .mode(SaveMode.Append)
  .insertInto('existing_table');
```

### DataFrameWriterV2

For advanced write operations, see the [DataFrameWriterV2 Guide](DataFrameWriterV2.md).

## Format-Specific Features

### CSV Best Practices

```typescript
// Writing CSV with best practices
await df.write()
  .option('header', 'true')
  .option('compression', 'gzip')
  .option('quote', '"')
  .option('escape', '\\')
  .csv('output/path');
```

### Parquet Best Practices

```typescript
// Writing Parquet with optimization
await df.write()
  .option('compression', 'snappy')
  .option('parquet.block.size', '134217728')  // 128 MB
  .partitionBy('date')
  .parquet('output/path');
```

### Handling Large Files

For large files, consider:

1. **Partitioning**: Split data into manageable partitions
2. **Compression**: Reduce storage and I/O
3. **Column pruning**: Read only needed columns
4. **Filter pushdown**: Apply filters early

```typescript
// Efficient large file processing
const df = spark.read()
  .parquet('large/dataset')
  .select('id', 'name', 'value')  // Column pruning
  .filter('date >= "2024-01-01"');  // Filter pushdown
```

## Data Validation

### Schema Validation

```typescript
// Read with expected schema
const expectedSchema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('name', DataTypes.StringType, false)
]);

try {
  const df = spark.read()
    .schema(expectedSchema)
    .csv('data.csv');
} catch (error) {
  console.error('Schema mismatch:', error);
}
```

### Data Quality Checks

```typescript
// Check for nulls
const nullCount = await df.filter(col('important_col').isNull()).count();
if (nullCount > 0) {
  console.warn(`Found ${nullCount} null values`);
}

// Validate ranges
const invalidRows = await df.filter(col('age').lt(0).or(col('age').gt(150))).count();
```

## Performance Tips

1. **Use Parquet for analytics**: Better compression and faster reads
2. **Specify schemas**: Avoid expensive schema inference
3. **Partition wisely**: Balance partition size (100MB-1GB ideal)
4. **Use compression**: Save storage and reduce I/O
5. **Coalesce/repartition**: Control output file count

```typescript
// Control output files
await df
  .coalesce(1)  // Single output file
  .write()
  .parquet('output/path');

// Or repartition for parallel writes
await df
  .repartition(10)  // 10 output files
  .write()
  .parquet('output/path');
```

## Examples

### ETL Pipeline

```typescript
// Extract
const rawData = spark.read()
  .option('header', 'true')
  .csv('raw/data.csv');

// Transform
const cleaned = rawData
  .filter(col('value').isNotNull())
  .withColumn('processed_date', current_date());

// Load
await cleaned.write()
  .mode(SaveMode.Overwrite)
  .partitionBy('processed_date')
  .parquet('processed/data');
```

### Format Conversion

```typescript
// CSV to Parquet
const df = spark.read()
  .option('header', 'true')
  .option('inferSchema', 'true')
  .csv('data.csv');

await df.write()
  .mode(SaveMode.Overwrite)
  .parquet('data.parquet');
```

## Next Steps

- Learn about [Aggregations](aggregations.md)
- Explore [SQL Functions](sql-functions.md)
- See [DataFrameWriterV2](DataFrameWriterV2.md) for advanced writes
