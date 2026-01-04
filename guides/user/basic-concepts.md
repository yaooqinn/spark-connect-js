# Basic Concepts

This guide introduces the core concepts you need to understand to work effectively with spark.js.

## SparkSession

`SparkSession` is the entry point for all Spark functionality. It represents the connection to your Spark Connect server.

### Creating a SparkSession

```typescript
import { SparkSession } from 'spark.js';

const spark = await SparkSession.builder()
  .remote('sc://localhost:15002')  // Spark Connect server URL
  .appName('MyApp')                 // Application name
  .build();
```

### Configuration Options

You can configure your SparkSession:

```typescript
const spark = await SparkSession.builder()
  .remote('sc://localhost:15002')
  .appName('MyApp')
  .config('spark.sql.shuffle.partitions', '10')
  .build();
```

### Closing a Session

Always close your session when done:

```typescript
await spark.stop();
```

## DataFrame

A `DataFrame` is a distributed collection of data organized into named columns. It's conceptually equivalent to a table in a relational database or a data frame in R/Python pandas.

### Creating DataFrames

**From a range:**

```typescript
const df = spark.range(0, 10);
// Creates a DataFrame with a single column 'id' from 0 to 9
```

**Reading from files:**

```typescript
const df = spark.read().csv('path/to/file.csv');
const df = spark.read().parquet('path/to/file.parquet');
const df = spark.read().json('path/to/file.json');
```

**From data:**

```typescript
const data = [
  { name: 'Alice', age: 25 },
  { name: 'Bob', age: 30 }
];
// Note: Direct data creation requires additional setup
```

### DataFrame Operations

DataFrames support two types of operations:

1. **Transformations**: Create a new DataFrame (lazy evaluation)
2. **Actions**: Trigger computation and return results

## Transformations

Transformations are lazy operations that define a new DataFrame without immediately computing results.

### Common Transformations

**Select columns:**

```typescript
const result = df.select('name', 'age');
```

**Filter rows:**

```typescript
const filtered = df.filter('age > 25');
// or using Column expressions
import { col } from 'spark.js';
const filtered = df.filter(col('age').gt(25));
```

**Add columns:**

```typescript
import { col, lit } from 'spark.js';
const withColumn = df.withColumn('age_plus_one', col('age').plus(lit(1)));
```

**Drop columns:**

```typescript
const dropped = df.drop('unwanted_column');
```

**Rename columns:**

```typescript
const renamed = df.withColumnRenamed('old_name', 'new_name');
```

**Sort:**

```typescript
const sorted = df.sort('age');
const sortedDesc = df.sort(col('age').desc());
```

**Distinct:**

```typescript
const unique = df.distinct();
```

### Chaining Transformations

Transformations can be chained:

```typescript
const result = df
  .filter('age > 20')
  .select('name', 'age')
  .sort('age')
  .limit(10);
```

## Actions

Actions trigger the actual computation and return results to the driver or write data.

### Common Actions

**Show data:**

```typescript
await df.show();           // Show first 20 rows
await df.show(5);          // Show first 5 rows
await df.show(10, false);  // Show 10 rows without truncation
```

**Collect data:**

```typescript
const rows = await df.collect();  // Returns all rows as an array
```

**Count rows:**

```typescript
const count = await df.count();
```

**Get first row:**

```typescript
const first = await df.first();
```

**Take rows:**

```typescript
const rows = await df.take(5);  // Get first 5 rows
```

**Write data:**

```typescript
await df.write().parquet('output/path');
```

## Column

`Column` represents a column in a DataFrame and supports various operations.

### Creating Column Expressions

```typescript
import { col, lit } from 'spark.js';

// Reference a column
const ageCol = col('age');

// Create a literal value
const one = lit(1);
```

### Column Operations

**Arithmetic:**

```typescript
col('age').plus(lit(1))      // age + 1
col('age').minus(lit(5))     // age - 5
col('age').multiply(lit(2))  // age * 2
col('age').divide(lit(2))    // age / 2
```

**Comparison:**

```typescript
col('age').gt(25)      // age > 25
col('age').lt(30)      // age < 30
col('age').equalTo(25) // age == 25
col('age').isNotNull() // age IS NOT NULL
```

**Logical:**

```typescript
col('age').gt(20).and(col('age').lt(30))  // age > 20 AND age < 30
col('status').equalTo('active').or(col('status').equalTo('pending'))
```

**String operations:**

```typescript
col('name').startsWith('A')
col('name').contains('test')
col('name').like('%pattern%')
```

**Aliasing:**

```typescript
col('age').plus(lit(1)).alias('age_next_year')
```

## Row

`Row` represents a row of data in a DataFrame.

### Accessing Row Data

```typescript
const rows = await df.collect();
for (const row of rows) {
  // Access by column name
  const name = row.get('name');
  const age = row.get('age');
  
  // Convert to object
  const obj = row.toJSON();
  console.log(obj);
}
```

## GroupedData

Created by `groupBy()`, allows aggregation operations.

```typescript
const grouped = df.groupBy('category');

// Aggregations
const counts = grouped.count();
const sums = grouped.sum('amount');
const averages = grouped.avg('score');
const maxValues = grouped.max('value');
```

Multiple aggregations:

```typescript
import { count, sum, avg } from 'spark.js';

const result = df.groupBy('category').agg(
  count('*').alias('count'),
  sum('amount').alias('total'),
  avg('score').alias('avg_score')
);
```

## Data Types

Spark has a rich type system. Common types include:

```typescript
import { DataTypes } from 'spark.js';

DataTypes.StringType      // String
DataTypes.IntegerType     // 32-bit integer
DataTypes.LongType        // 64-bit integer
DataTypes.DoubleType      // Double-precision float
DataTypes.BooleanType     // Boolean
DataTypes.DateType        // Date (no time)
DataTypes.TimestampType   // Timestamp with time
```

For complex types:

```typescript
// Array
const arrayType = DataTypes.createArrayType(DataTypes.IntegerType);

// Map
const mapType = DataTypes.createMapType(
  DataTypes.StringType, 
  DataTypes.IntegerType
);

// Struct
const structType = DataTypes.createStructType([
  DataTypes.createStructField('name', DataTypes.StringType, false),
  DataTypes.createStructField('age', DataTypes.IntegerType, true)
]);
```

Learn more in the [Type System Guide](type-system.md).

## Lazy Evaluation

Spark uses lazy evaluation for transformations:

1. **Transformations** are not executed immediately
2. Spark builds a **logical plan** (DAG)
3. **Actions** trigger execution
4. Spark **optimizes** the plan before execution

Example:

```typescript
// No execution yet
const df1 = df.filter('age > 20');
const df2 = df1.select('name');
const df3 = df2.distinct();

// Execution happens here
const result = await df3.collect();  // Action triggers all transformations
```

## Catalyst Optimizer

Spark's Catalyst optimizer automatically optimizes your queries:

- Predicate pushdown
- Column pruning
- Constant folding
- Join reordering

You write high-level code, Spark optimizes it for you!

## Next Steps

- Explore [SQL Functions](sql-functions.md) for data transformation
- Learn about [Aggregations](aggregations.md) in detail
- Understand the [Type System](type-system.md)
- Read about [Data I/O](data-io.md) operations
