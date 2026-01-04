# Troubleshooting Guide

Common issues and solutions when working with spark.js.

## Connection Issues

### Cannot Connect to Spark Connect Server

**Symptom**: Connection refused or timeout errors

```
Error: 14 UNAVAILABLE: No connection established
```

**Solutions**:

1. **Verify server is running**:
   ```bash
   # If using Docker
   docker ps | grep spark
   
   # Check server logs
   docker logs <container-name>
   ```

2. **Check the connection URL**:
   ```typescript
   // Correct format
   const spark = await SparkSession.builder()
     .remote('sc://localhost:15002')  // Default port is 15002
     .build();
   ```

3. **Check firewall settings**: Ensure port 15002 is not blocked

4. **Verify server is listening**:
   ```bash
   netstat -an | grep 15002
   # or
   lsof -i :15002
   ```

### Connection Timeout

**Symptom**: Operations hang or timeout

**Solutions**:

1. **Increase timeout**:
   ```typescript
   const spark = await SparkSession.builder()
     .remote('sc://localhost:15002')
     .config('spark.sql.connect.grpc.deadline', '600s')  // 10 minutes
     .build();
   ```

2. **Check network latency** to the Spark server

3. **Monitor server resources**: CPU, memory, disk I/O

## Installation Issues

### Module Not Found

**Symptom**: 
```
Error: Cannot find module 'spark.js'
```

**Solutions**:

1. **Reinstall the package**:
   ```bash
   npm install spark.js
   ```

2. **Clear cache and reinstall**:
   ```bash
   rm -rf node_modules package-lock.json
   npm cache clean --force
   npm install
   ```

3. **Check package.json** includes spark.js in dependencies

### TypeScript Errors

**Symptom**: Type errors when using spark.js

**Solutions**:

1. **Update TypeScript**:
   ```bash
   npm install -D typescript@latest
   ```

2. **Check tsconfig.json**:
   ```json
   {
     "compilerOptions": {
       "target": "ES2020",
       "module": "commonjs",
       "moduleResolution": "node",
       "esModuleInterop": true
     }
   }
   ```

3. **Install type definitions**:
   ```bash
   npm install -D @types/node
   ```

## Runtime Errors

### DataFrame Operation Fails

**Symptom**: Error when executing DataFrame operations

**Solutions**:

1. **Check column names**:
   ```typescript
   // Print schema to verify column names
   df.printSchema();
   
   // List columns
   console.log(df.columns());
   ```

2. **Validate data types**:
   ```typescript
   // Check data types
   console.log(df.dtypes());
   
   // Cast if needed
   df.withColumn('col', col('col').cast(DataTypes.IntegerType));
   ```

3. **Handle nulls**:
   ```typescript
   // Check for nulls
   const nullCount = await df.filter(col('column').isNull()).count();
   
   // Filter out nulls
   const cleaned = df.filter(col('column').isNotNull());
   ```

### Out of Memory Errors

**Symptom**: 
```
Error: OutOfMemoryError: Java heap space
```

**Solutions**:

1. **Increase executor memory** (server-side configuration)

2. **Reduce partition size**:
   ```typescript
   df.repartition(100);  // More, smaller partitions
   ```

3. **Use `limit()` for testing**:
   ```typescript
   const sample = df.limit(1000);  // Work with smaller dataset
   ```

4. **Process in batches**:
   ```typescript
   // Process partitions one at a time
   const partitions = 10;
   for (let i = 0; i < partitions; i++) {
     const partition = df.filter(`partition_id = ${i}`);
     await partition.write().parquet(`output/part_${i}`);
   }
   ```

### Type Casting Errors

**Symptom**: Invalid cast exceptions

**Solutions**:

1. **Use try_cast** for safe casting:
   ```typescript
   // Returns null on failure instead of error
   df.withColumn('safe_int', try_cast(col('str'), DataTypes.IntegerType));
   ```

2. **Validate before casting**:
   ```typescript
   // Filter invalid values first
   const valid = df.filter(col('number_str').rlike('^[0-9]+$'));
   const casted = valid.withColumn('number', col('number_str').cast(DataTypes.IntegerType));
   ```

## Performance Issues

### Slow Queries

**Symptom**: Operations take longer than expected

**Solutions**:

1. **Check query plan**:
   ```typescript
   df.explain();  // Show execution plan
   df.explain(true);  // Show detailed plan
   ```

2. **Cache frequently used DataFrames**:
   ```typescript
   df.cache();
   await df.count();  // Materialize cache
   ```

3. **Repartition for parallelism**:
   ```typescript
   df.repartition(200);  // More parallelism
   ```

4. **Use predicate pushdown**:
   ```typescript
   // Good: Filter early
   const result = spark.read()
     .parquet('data.parquet')
     .filter('date >= "2024-01-01"');
   
   // Bad: Filter late
   const all = spark.read().parquet('data.parquet');
   const result = all.filter('date >= "2024-01-01"');
   ```

5. **Broadcast small tables**:
   ```typescript
   import { broadcast } from 'spark.js';
   
   const result = largeDf.join(broadcast(smallDf), 'key');
   ```

### Data Skew

**Symptom**: Some tasks take much longer than others

**Solutions**:

1. **Check data distribution**:
   ```typescript
   df.groupBy('key').count().orderBy(col('count').desc()).show();
   ```

2. **Add salt for skewed joins**:
   ```typescript
   import { rand, lit } from 'spark.js';
   
   const salted = df.withColumn('salt', rand().multiply(lit(10)).cast('int'));
   ```

3. **Use adaptive query execution** (server-side):
   ```typescript
   const spark = await SparkSession.builder()
     .config('spark.sql.adaptive.enabled', 'true')
     .build();
   ```

## File I/O Issues

### File Not Found

**Symptom**: 
```
FileNotFoundException: path/to/file.csv
```

**Solutions**:

1. **Use absolute paths**:
   ```typescript
   const df = spark.read().csv('/absolute/path/to/file.csv');
   ```

2. **Check file permissions**:
   ```bash
   ls -la path/to/file.csv
   ```

3. **Verify file system prefix**:
   ```typescript
   // Local file
   spark.read().csv('file:///path/to/file.csv')
   
   // HDFS
   spark.read().csv('hdfs://namenode:8020/path/to/file.csv')
   
   // S3
   spark.read().csv('s3a://bucket/path/to/file.csv')
   ```

### Schema Mismatch

**Symptom**: Schema errors when reading files

**Solutions**:

1. **Specify schema explicitly**:
   ```typescript
   const schema = DataTypes.createStructType([...]);
   const df = spark.read().schema(schema).csv('file.csv');
   ```

2. **Use mode for corrupt records**:
   ```typescript
   const df = spark.read()
     .option('mode', 'PERMISSIVE')  // or DROPMALFORMED, FAILFAST
     .option('columnNameOfCorruptRecord', '_corrupt_record')
     .csv('file.csv');
   ```

3. **Check for encoding issues**:
   ```typescript
   const df = spark.read()
     .option('encoding', 'UTF-8')
     .csv('file.csv');
   ```

## Testing Issues

### Tests Hang

**Symptom**: Tests never complete

**Solutions**:

1. **Ensure Spark Connect server is running**:
   ```bash
   docker ps | grep spark
   ```

2. **Wait for server to start** (15-20 seconds after `docker run`)

3. **Check server logs**:
   ```bash
   docker logs sparkconnect
   ```
   Look for: "Spark Connect server started at: [::]:15002"

4. **Increase Jest timeout**:
   ```typescript
   jest.setTimeout(60000);  // 60 seconds
   ```

### Tests Fail Intermittently

**Solutions**:

1. **Avoid timing dependencies**: Use proper waits instead of fixed delays

2. **Clean up between tests**:
   ```typescript
   afterEach(async () => {
     await spark.stop();
   });
   ```

3. **Use dedicated SparkSession per test** or shared properly

## Common Mistakes

### Not Calling await on Actions

**Wrong**:
```typescript
df.show();  // Missing await - returns Promise
```

**Correct**:
```typescript
await df.show();  // Properly awaited
```

### Forgetting to Stop SparkSession

**Wrong**:
```typescript
const spark = await SparkSession.builder().build();
// ... use spark ...
// Session not closed - resource leak
```

**Correct**:
```typescript
const spark = await SparkSession.builder().build();
try {
  // ... use spark ...
} finally {
  await spark.stop();
}
```

### Mixing Transformations and Actions

**Inefficient**:
```typescript
// Count triggers execution
const count1 = await df.filter('age > 20').count();
// Another execution
const count2 = await df.filter('age > 30').count();
```

**Better**:
```typescript
// Cache the base DataFrame
df.cache();

const count1 = await df.filter('age > 20').count();
const count2 = await df.filter('age > 30').count();
```

## Getting Help

If you're still stuck:

1. **Check the examples**: See [example directory](../../example/org/apache/spark/sql/example/)

2. **Search issues**: [GitHub Issues](https://github.com/yaooqinn/spark.js/issues)

3. **Review API docs**: [API Reference](../api/)

4. **Enable debug logging**:
   ```typescript
   // Check log4js.json configuration
   // Logs are in logs/ directory
   ```

5. **Create a minimal reproduction**:
   ```typescript
   // Minimal example that shows the issue
   const spark = await SparkSession.builder()
     .remote('sc://localhost:15002')
     .build();
   
   const df = spark.range(10);
   await df.show();  // Does this work?
   
   await spark.stop();
   ```

6. **Report the issue**: Include:
   - spark.js version
   - Node.js version
   - Spark version
   - Minimal code to reproduce
   - Error message and stack trace
   - What you've already tried

## Diagnostic Commands

### Check Versions

```bash
# Node.js version
node --version

# npm version
npm --version

# spark.js version
npm list spark.js

# TypeScript version
npx tsc --version
```

### Verify Installation

```typescript
import { SparkSession } from 'spark.js';

async function verify() {
  console.log('Connecting to Spark...');
  const spark = await SparkSession.builder()
    .remote('sc://localhost:15002')
    .appName('VerifyInstallation')
    .build();
  
  console.log('✓ Connected successfully');
  
  const df = spark.range(5);
  await df.show();
  
  console.log('✓ Operations work correctly');
  
  await spark.stop();
  console.log('✓ Verification complete');
}

verify().catch(console.error);
```

## Next Steps

- Review [Quick Start Guide](quick-start.md) for basics
- Check [Installation Guide](installation.md) for setup
- Browse [Examples](../../example/org/apache/spark/sql/example/) for working code
