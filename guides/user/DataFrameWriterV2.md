# DataFrameWriterV2 API Examples

The DataFrameWriterV2 API provides advanced write operations for V2 data sources with better semantics than V1.

## Basic Usage

### Create a new table

```typescript
await df.writeTo("my_table")
  .using("parquet")
  .partitionBy("year", "month")
  .tableProperty("compression", "snappy")
  .create();
```

### Replace existing table

```typescript
await df.writeTo("my_table")
  .using("delta")
  .partitionBy("date")
  .clusterBy("user_id")
  .replace();
```

### Append to existing table

```typescript
await df.writeTo("my_table")
  .option("mergeSchema", "true")
  .append();
```

### Conditional overwrite

```typescript
await df.writeTo("my_table")
  .overwrite("date = '2024-01-01'");
```

### Create or replace table

```typescript
await df.writeTo("my_table")
  .using("parquet")
  .createOrReplace();
```

### Overwrite partitions

```typescript
await df.writeTo("my_table")
  .overwritePartitions();
```

## Advanced Usage

### Iceberg table with clustering

```typescript
await df.writeTo("catalog.db.table")
  .using("iceberg")
  .partitionBy("date")
  .clusterBy("user_id", "event_type")
  .tableProperty("write.format.default", "parquet")
  .createOrReplace();
```

### Delta table with options

```typescript
await df.writeTo("my_delta_table")
  .using("delta")
  .option("mergeSchema", "true")
  .option("overwriteSchema", "false")
  .partitionBy("year", "month")
  .create();
```

### ORC table with table properties

```typescript
await df.writeTo("my_orc_table")
  .using("orc")
  .tableProperty("orc.compress", "SNAPPY")
  .tableProperty("orc.bloom.filter.columns", "id,name")
  .partitionBy("region")
  .create();
```

## API Reference

### Methods

- `using(provider: string)`: Specify data source provider (e.g., "parquet", "orc", "iceberg", "delta")
- `option(key: string, value: string)`: Add write option
- `options(opts: Record<string, string>)`: Add multiple options
- `tableProperty(key: string, value: string)`: Add table property
- `partitionBy(...cols: (string | Column)[])`: Partition by columns
- `clusterBy(...cols: string[])`: Cluster by columns (for data sources that support clustering)
- `create()`: Create new table
- `replace()`: Replace existing table
- `createOrReplace()`: Create or replace table
- `append()`: Append to existing table
- `overwrite(condition: Column | string)`: Overwrite matching rows
- `overwritePartitions()`: Overwrite partitions

## Supported Data Sources

The following data sources support V2 write operations:

- **Parquet**: Standard columnar format
- **ORC**: Optimized Row Columnar format
- **Delta**: Delta Lake tables (requires Delta Lake)
- **Iceberg**: Apache Iceberg tables (requires Iceberg)
- Other V2 data sources as supported by your Spark configuration

## Notes

- The DataFrameWriterV2 API is available starting from Spark Connect JS 1.0.0
- V2 write operations provide better semantics and performance compared to V1
- Not all data sources support all V2 operations - check your data source documentation
