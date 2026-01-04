# Type System Guide

Understanding Spark's type system in spark.js and how it maps to TypeScript.

## Overview

Spark has a rich type system that maps SQL and Spark types to TypeScript. Understanding these types helps you write type-safe applications and avoid runtime errors.

## Basic Types

### Numeric Types

```typescript
import { DataTypes } from 'spark.js';

// Integer types
DataTypes.ByteType       // 8-bit signed integer (-128 to 127)
DataTypes.ShortType      // 16-bit signed integer
DataTypes.IntegerType    // 32-bit signed integer
DataTypes.LongType       // 64-bit signed integer

// Floating point types
DataTypes.FloatType      // Single-precision (32-bit)
DataTypes.DoubleType     // Double-precision (64-bit)

// Decimal type for precise calculations
DataTypes.createDecimalType(10, 2)  // Precision 10, scale 2
```

### String and Boolean

```typescript
// String type
DataTypes.StringType     // Variable-length string

// Boolean type
DataTypes.BooleanType    // true or false
```

### Binary Type

```typescript
// Binary data
DataTypes.BinaryType     // Array of bytes
```

## Date and Time Types

```typescript
// Date without time (year, month, day)
DataTypes.DateType

// Timestamp with time (microsecond precision)
DataTypes.TimestampType

// Timestamp without timezone
DataTypes.TimestampNTZType  // Spark 3.4+
```

### Working with Dates

```typescript
import { to_date, to_timestamp, current_date } from 'spark.js';

// Convert string to date
df.withColumn('date', to_date(col('date_string'), 'yyyy-MM-dd'));

// Convert string to timestamp
df.withColumn('ts', to_timestamp(col('ts_string'), 'yyyy-MM-dd HH:mm:ss'));

// Current date
df.withColumn('today', current_date());
```

## Complex Types

### Array Type

Arrays hold multiple values of the same type:

```typescript
import { array, explode, array_contains } from 'spark.js';

// Create array type
const arrayType = DataTypes.createArrayType(DataTypes.IntegerType);

// Schema with array
const schema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('tags', arrayType, true)
]);

// Create array column
df.withColumn('numbers', array(lit(1), lit(2), lit(3)));

// Explode array to rows
df.select(col('id'), explode(col('tags')).alias('tag'));

// Check array contains
df.filter(array_contains(col('tags'), 'important'));
```

### Map Type

Maps store key-value pairs:

```typescript
// Create map type
const mapType = DataTypes.createMapType(
  DataTypes.StringType,   // Key type
  DataTypes.IntegerType   // Value type
);

// Schema with map
const schema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('attributes', mapType, true)
]);
```

### Struct Type

Structs are like nested records or objects:

```typescript
// Define a struct type
const addressType = DataTypes.createStructType([
  DataTypes.createStructField('street', DataTypes.StringType, true),
  DataTypes.createStructField('city', DataTypes.StringType, true),
  DataTypes.createStructField('zip', DataTypes.StringType, true)
]);

// Use in schema
const schema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('name', DataTypes.StringType, false),
  DataTypes.createStructField('address', addressType, true)
]);

// Access nested fields
df.select(
  col('id'),
  col('address.city'),
  col('address.zip')
);
```

## Creating Schemas

### StructType and StructField

```typescript
import { DataTypes } from 'spark.js';

// Define schema
const schema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.LongType, false),       // Not nullable
  DataTypes.createStructField('name', DataTypes.StringType, false),   // Not nullable
  DataTypes.createStructField('age', DataTypes.IntegerType, true),    // Nullable
  DataTypes.createStructField('salary', DataTypes.DoubleType, true)   // Nullable
]);

// Use schema when reading
const df = spark.read()
  .schema(schema)
  .csv('data.csv');
```

### Nested Schemas

```typescript
// Define nested structure
const addressSchema = DataTypes.createStructType([
  DataTypes.createStructField('street', DataTypes.StringType, true),
  DataTypes.createStructField('city', DataTypes.StringType, true),
  DataTypes.createStructField('state', DataTypes.StringType, true),
  DataTypes.createStructField('zip', DataTypes.StringType, true)
]);

const personSchema = DataTypes.createStructType([
  DataTypes.createStructField('id', DataTypes.IntegerType, false),
  DataTypes.createStructField('name', DataTypes.StringType, false),
  DataTypes.createStructField('address', addressSchema, true),
  DataTypes.createStructField('emails', 
    DataTypes.createArrayType(DataTypes.StringType), true)
]);
```

## Type Inference

Spark can automatically infer schema from data:

```typescript
// Infer schema from CSV
const df = spark.read()
  .option('header', 'true')
  .option('inferSchema', 'true')  // Enable inference
  .csv('data.csv');

// Print inferred schema
df.printSchema();
```

**Note**: Schema inference requires reading the entire dataset and can be expensive. For production, specify schemas explicitly.

## Schema Operations

### Viewing Schema

```typescript
// Print schema in tree format
df.printSchema();

// Get schema object
const schema = df.schema();

// Get column names
const columns = df.columns();

// Get data types
const dtypes = df.dtypes();
```

### Modifying Schema

```typescript
// Change column type
df.withColumn('age', col('age').cast(DataTypes.IntegerType));

// Add column with specific type
df.withColumn('score', lit(0).cast(DataTypes.DoubleType));

// Rename column
df.withColumnRenamed('old_name', 'new_name');
```

## Type Casting

### Explicit Casting

```typescript
import { col } from 'spark.js';

// Cast to different types
df.withColumn('age_str', col('age').cast(DataTypes.StringType));
df.withColumn('price_int', col('price').cast(DataTypes.IntegerType));
df.withColumn('value_double', col('value').cast(DataTypes.DoubleType));

// Cast to date/timestamp
df.withColumn('date', col('date_str').cast(DataTypes.DateType));
df.withColumn('ts', col('ts_str').cast(DataTypes.TimestampType));
```

### Safe Casting

Use `try_cast` to avoid errors on invalid casts (returns null on failure):

```typescript
// Returns null if cast fails
df.withColumn('safe_int', try_cast(col('string_col'), DataTypes.IntegerType));
```

## Nullable vs Non-Nullable

Fields can be marked as nullable or non-nullable:

```typescript
// Non-nullable (third parameter is false)
DataTypes.createStructField('id', DataTypes.IntegerType, false)

// Nullable (third parameter is true)
DataTypes.createStructField('description', DataTypes.StringType, true)
```

### Handling Nulls

```typescript
import { coalesce, when } from 'spark.js';

// Replace nulls with default value
df.withColumn('value', coalesce(col('value'), lit(0)));

// Conditional null handling
df.withColumn('cleaned',
  when(col('value').isNull(), lit('N/A'))
    .otherwise(col('value'))
);

// Drop rows with nulls
df.na.drop();

// Fill nulls
df.na.fill({ 'column': 0 });
```

## Type Compatibility

### TypeScript to Spark Mapping

| TypeScript Type | Spark Type | Notes |
|----------------|------------|-------|
| `number` | IntegerType, LongType, DoubleType | Depends on range and precision |
| `string` | StringType | UTF-8 encoded |
| `boolean` | BooleanType | |
| `Date` | TimestampType | JavaScript Date |
| `Array<T>` | ArrayType | Nested type |
| `Object` | StructType | Nested structure |
| `Map<K,V>` | MapType | Key-value pairs |
| `null` | NullType | Represents null value |

### Spark to TypeScript Mapping

When collecting data:

```typescript
const rows = await df.collect();
for (const row of rows) {
  const id: number = row.get('id');           // IntegerType → number
  const name: string = row.get('name');       // StringType → string
  const active: boolean = row.get('active');  // BooleanType → boolean
  const tags: any[] = row.get('tags');        // ArrayType → Array
}
```

## Decimal Types

For precise decimal arithmetic:

```typescript
// Create decimal type: precision, scale
const decimalType = DataTypes.createDecimalType(10, 2);  // 10 total digits, 2 after decimal

const schema = DataTypes.createStructType([
  DataTypes.createStructField('amount', decimalType, false)
]);

// Use for money calculations
df.withColumn('total', 
  col('price').cast(DataTypes.createDecimalType(10, 2))
);
```

## User-Defined Types

While Spark supports UDTs (User-Defined Types), they are complex in spark.js. Use struct types instead:

```typescript
// Instead of UDT, use StructType
const coordinateType = DataTypes.createStructType([
  DataTypes.createStructField('lat', DataTypes.DoubleType, false),
  DataTypes.createStructField('lon', DataTypes.DoubleType, false)
]);
```

## Best Practices

1. **Specify schemas explicitly**: Avoid expensive schema inference in production
2. **Use appropriate types**: Choose the smallest type that fits your data
3. **Mark non-nullable fields**: Helps catch data quality issues early
4. **Use Decimal for money**: Avoid floating-point precision issues
5. **Leverage complex types**: Use arrays, maps, and structs for hierarchical data
6. **Cast carefully**: Understand that invalid casts can cause errors

## Examples

### Complete Schema Definition

```typescript
const schema = DataTypes.createStructType([
  // Primary key
  DataTypes.createStructField('user_id', DataTypes.LongType, false),
  
  // Basic fields
  DataTypes.createStructField('username', DataTypes.StringType, false),
  DataTypes.createStructField('email', DataTypes.StringType, true),
  DataTypes.createStructField('age', DataTypes.IntegerType, true),
  
  // Decimal for money
  DataTypes.createStructField('balance', 
    DataTypes.createDecimalType(15, 2), true),
  
  // Dates
  DataTypes.createStructField('created_at', DataTypes.TimestampType, false),
  DataTypes.createStructField('last_login', DataTypes.DateType, true),
  
  // Array
  DataTypes.createStructField('interests', 
    DataTypes.createArrayType(DataTypes.StringType), true),
  
  // Nested struct
  DataTypes.createStructField('address',
    DataTypes.createStructType([
      DataTypes.createStructField('street', DataTypes.StringType, true),
      DataTypes.createStructField('city', DataTypes.StringType, true),
      DataTypes.createStructField('zip', DataTypes.StringType, true)
    ]),
    true
  )
]);
```

### Type-Safe Data Processing

```typescript
// Read with schema
const df = spark.read()
  .schema(schema)
  .parquet('users.parquet');

// Type conversions
const processed = df
  .withColumn('age_str', col('age').cast(DataTypes.StringType))
  .withColumn('created_date', col('created_at').cast(DataTypes.DateType))
  .withColumn('balance_int', col('balance').cast(DataTypes.LongType));
```

## Next Steps

- Explore [SQL Functions](sql-functions.md) for type-specific operations
- Learn about [Data I/O](data-io.md) and schema usage
- Check the [API Reference](../api/) for complete type documentation
