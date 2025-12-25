# Apache Spark Connect Client for JavaScript

An <b><red>experimental</red></b> client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for [Apache Spark](https://spark.apache.org/) written in [TypeScript](https://www.typescriptlang.org/).


# Roadmaps
- [ ] For minor changes or some features associated with certern classes, SEARCH 'TODO'
- [ ] Support Retry / Reattachable execution
- [ ] Support Checkpoint for DataFrame
- [ ] Support DataFrame Join 
- [ ] Support UserDefinedType
  - [ ] UserDefinedType declaration
  - [ ] UserDefinedType & Proto bidi-converions
  - [ ] UserDefinedType & Arrow bidi-converions
- [ ] Maybe Optimize the Logging or it's framework

# Features

## DataFrameNaFunctions

The library now supports comprehensive missing data handling via the `na` property on DataFrames:

### fillna() - Fill null/NaN values
```typescript
// Fill all null values with 0
df.na.fillna(0)

// Fill null values in specific columns
df.na.fillna("unknown", ["name", "address"])

// Also available as fill()
df.na.fill(0)
```

### dropna() - Drop rows with null/NaN values
```typescript
// Drop rows with any null values (default)
df.na.dropna()

// Drop rows only if all values are null
df.na.dropna('all')

// Drop rows with fewer than 2 non-null values
df.na.dropna(2)

// Drop rows with null values in specific columns
df.na.dropna('any', ['col1', 'col2'])

// Also available as drop()
df.na.drop()
```

### replace() - Replace values
```typescript
// Replace values across all compatible columns
df.na.replace({ 1: 100, 2: 200 })

// Replace string values
df.na.replace({ 'UNKNOWN': 'N/A', 'NULL': 'N/A' })

// Replace values in specific columns only
df.na.replace({ 1: 999 }, ['col1', 'col2'])
```

