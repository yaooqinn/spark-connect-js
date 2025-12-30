# Apache Spark Connect Client for JavaScript

An <b><red>experimental</red></b> client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for [Apache Spark](https://spark.apache.org/) written in [TypeScript](https://www.typescriptlang.org/).


# Features

## User-Defined Functions (UDF)
This client now supports User-Defined Functions (UDFs) for extending Spark with custom JavaScript logic.

### Register UDFs
```typescript
// Register a UDF for use in SQL
await spark.udf.register("doubleValue", (x: number) => x * 2, "int");
const result = await spark.sql("SELECT doubleValue(5) as result");

// Register with DataType objects
import { DataTypes } from 'spark-connect-js';
await spark.udf.register("myUdf", (x: number) => x * 2, DataTypes.IntegerType);
```

### Inline UDFs in DataFrame API
```typescript
import { udf, col } from 'spark-connect-js';

// Create an inline UDF
const doubleUdf = udf((x: number) => x * 2, "int");

// Use in DataFrame operations
df.select(doubleUdf(col("value")));
```

### Register Java UDFs
```typescript
// With explicit return type
await spark.udf.registerJava("javaUdf", "com.example.MyUDF", DataTypes.IntegerType);

// Let server decide return type
await spark.udf.registerJava("javaUdf", "com.example.MyUDF");
```

# Roadmaps
- [ ] For minor changes or some features associated with certern classes, SEARCH 'TODO'
- [ ] Support Retry / Reattachable execution
- [ ] Support Checkpoint for DataFrame
- [ ] Support DataFrameNaFunctions
- [ ] Support DataFrame Join 
- [x] Support User-Defined Functions (UDF)
  - [x] UDF registration via `spark.udf.register()`
  - [x] Inline UDFs via `udf()` function
  - [x] Java UDF registration
  - [ ] UDAF (User-Defined Aggregate Functions)
  - [ ] UDTF (User-Defined Table Functions)
- [x] Support DataFrame Join 
- [ ] Support UserDefinedType
  - [ ] UserDefinedType declaration
  - [ ] UserDefinedType & Proto bidi-converions
  - [ ] UserDefinedType & Arrow bidi-converions
- [ ] Maybe Optimize the Logging or it's framework

