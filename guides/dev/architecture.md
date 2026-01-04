# Architecture Overview

System architecture and design of spark.js.

## High-Level Architecture

spark.js is a client library that communicates with Apache Spark via the Spark Connect protocol using gRPC.

```
┌─────────────────────────────────────────────────┐
│                 Your Application                │
│              (JavaScript/TypeScript)            │
└────────────────────┬────────────────────────────┘
                     │
                     │ Import spark.js API
                     ↓
┌─────────────────────────────────────────────────┐
│                   spark.js                      │
│  ┌─────────────────────────────────────────┐   │
│  │         Public API Layer                │   │
│  │  (SparkSession, DataFrame, Column)      │   │
│  └──────────────────┬──────────────────────┘   │
│                     │                           │
│  ┌──────────────────▼──────────────────────┐   │
│  │       Protocol Builders                 │   │
│  │  (PlanBuilder, RelationBuilder, etc.)   │   │
│  └──────────────────┬──────────────────────┘   │
│                     │                           │
│  ┌──────────────────▼──────────────────────┐   │
│  │      Generated Protobuf Code            │   │
│  │  (Message classes from .proto files)    │   │
│  └──────────────────┬──────────────────────┘   │
│                     │                           │
│  ┌──────────────────▼──────────────────────┐   │
│  │          gRPC Client                    │   │
│  │  (@grpc/grpc-js, binary transport)      │   │
│  └──────────────────┬──────────────────────┘   │
└─────────────────────┼───────────────────────────┘
                      │
                      │ gRPC/Protobuf
                      │ Port 15002
                      ↓
┌─────────────────────────────────────────────────┐
│          Spark Connect Server                   │
│              (Apache Spark 4.x)                 │
└─────────────────────────────────────────────────┘
```

## Core Components

### 1. Public API Layer

Entry point for users. Provides high-level abstractions.

**Key Classes**:
- **SparkSession**: Connection to Spark, entry point
- **DataFrame**: Distributed dataset with operations
- **Column**: Column expressions
- **DataFrameReader**: Reading data
- **DataFrameWriter**: Writing data

**Location**: `src/org/apache/spark/sql/`

### 2. Protocol Builders

Translate API calls into protobuf messages.

**Key Builders**:
- **PlanBuilder**: Creates execution plans
- **RelationBuilder**: Creates relation messages
- **ExpressionBuilder**: Creates expression messages
- **CommandBuilder**: Creates command messages

**Location**: `src/org/apache/spark/sql/proto/`

### 3. Generated Protobuf Code

Auto-generated from `.proto` files using Buf.

**Messages**:
- Plan, Relation, Expression, Command
- Read, Filter, Project, Join, Aggregate
- Literal, UnresolvedAttribute, UnresolvedFunction

**Location**: `src/gen/spark/connect/`

### 4. gRPC Client

Handles network communication with Spark Connect server.

**Responsibilities**:
- Establish connection
- Send requests (protobuf binary)
- Receive responses
- Handle streaming results
- Error handling

**Location**: `src/org/apache/spark/sql/grpc/`

### 5. Supporting Systems

**Type System**: `src/org/apache/spark/sql/types/`
- DataTypes, StructType, StructField
- Type definitions matching Spark types

**Catalog**: `src/org/apache/spark/sql/catalog/`
- Database, Table, Function metadata
- Catalog operations

**Arrow Integration**: `src/org/apache/spark/sql/arrow/`
- Deserializing Arrow data from Spark
- Efficient data transfer

**Utilities**: `src/org/apache/spark/sql/util/`
- Helper functions
- Case-insensitive maps
- ID generation

## Data Flow

### Query Execution Flow

1. **User creates query**:
   ```typescript
   const result = spark.range(100).filter('id > 50');
   ```

2. **API builds logical plan**:
   - `spark.range(100)` → Read relation
   - `.filter('id > 50')` → Filter relation wrapping Read

3. **Protocol builders create protobuf**:
   - RelationBuilder creates Filter message
   - ExpressionBuilder creates expression for 'id > 50'
   - PlanBuilder wraps in Plan message

4. **gRPC client sends request**:
   - Serialize protobuf to binary
   - Send via gRPC to server
   - Port 15002, ExecutePlan RPC

5. **Spark executes query**:
   - Server receives plan
   - Catalyst optimizer optimizes
   - Spark executes on cluster
   - Returns results as Arrow batches

6. **Client processes response**:
   - Receive Arrow batches via gRPC stream
   - Deserialize Arrow to JavaScript objects
   - Return to user as Row objects

### Example: Creating a DataFrame

```typescript
// 1. User code
const df = spark.range(10);

// 2. SparkSession.range() implementation
range(end: number): DataFrame {
  // Build Range relation
  const relation = RelationBuilder.createRange(end);
  
  // Build Plan
  const plan = PlanBuilder.createPlan(relation);
  
  // Create DataFrame with plan
  return new DataFrame(this.client, plan);
}

// 3. RelationBuilder
static createRange(end: number): Relation {
  return new Relation({
    relType: {
      case: 'range',
      value: new Range({
        end: BigInt(end)
      })
    }
  });
}

// 4. DataFrame is lazy - no execution yet
// Execution happens on action like collect()
```

### Example: Collecting Results

```typescript
// 1. User calls action
const rows = await df.collect();

// 2. DataFrame.collect() implementation
async collect(): Promise<Row[]> {
  // Execute plan via gRPC
  const response = await this.client.executePlan(this.plan);
  
  // Convert Arrow batches to Rows
  return ArrowUtils.arrowToRows(response.arrowBatches);
}

// 3. Client.executePlan()
async executePlan(plan: Plan): Promise<Response> {
  // Serialize plan to binary
  const request = plan.toBinary();
  
  // Send via gRPC
  const stream = this.grpcClient.executePlan(request);
  
  // Collect streaming responses
  const batches = [];
  for await (const batch of stream) {
    batches.push(batch);
  }
  
  return { arrowBatches: batches };
}

// 4. ArrowUtils converts Arrow to JavaScript
static arrowToRows(batches: ArrowBatch[]): Row[] {
  const table = Table.from(batches);
  return table.toArray().map(row => new Row(row));
}
```

## Design Patterns

### Builder Pattern

Used for constructing complex protobuf messages:

```typescript
// Instead of this:
const relation = new Relation({
  relType: {
    case: 'filter',
    value: new Filter({
      input: new Relation({
        relType: {
          case: 'read',
          value: new Read({ /* ... */ })
        }
      }),
      condition: new Expression({ /* ... */ })
    })
  }
});

// Use builders:
const relation = RelationBuilder.createFilter(
  RelationBuilder.createRead(path),
  ExpressionBuilder.createGreaterThan(col('age'), lit(18))
);
```

### Fluent Interface

DataFrame operations return new DataFrames for chaining:

```typescript
const result = df
  .filter('age > 18')
  .select('name', 'age')
  .orderBy('age')
  .limit(10);
```

### Lazy Evaluation

Transformations build a plan without execution:

```typescript
const df1 = df.filter('age > 18');  // No execution
const df2 = df1.select('name');     // No execution
const rows = await df2.collect();   // NOW execution happens
```

### Immutability

DataFrames are immutable - operations return new instances:

```typescript
const df1 = spark.range(10);
const df2 = df1.filter('id > 5');
// df1 is unchanged, df2 is a new DataFrame
```

## Module Organization

### Source Code Structure

```
src/org/apache/spark/
├── sql/
│   ├── SparkSession.ts        # Main entry point
│   ├── DataFrame.ts           # DataFrame operations
│   ├── Column.ts              # Column expressions
│   ├── DataFrameReader.ts     # Read operations
│   ├── DataFrameWriter.ts     # Write operations
│   ├── Row.ts                 # Row data structure
│   ├── functions.ts           # SQL functions
│   │
│   ├── grpc/                  # gRPC client
│   │   ├── Client.ts
│   │   └── client_builder.ts
│   │
│   ├── proto/                 # Protocol builders
│   │   ├── PlanBuilder.ts
│   │   ├── RelationBuilder.ts
│   │   ├── ExpressionBuilder.ts
│   │   └── CommandBuilder.ts
│   │
│   ├── types/                 # Type system
│   │   ├── DataTypes.ts
│   │   ├── StructType.ts
│   │   └── [30+ type files]
│   │
│   ├── catalog/               # Catalog API
│   │   ├── Catalog.ts
│   │   ├── Database.ts
│   │   └── Table.ts
│   │
│   ├── arrow/                 # Arrow integration
│   │   └── ArrowUtils.ts
│   │
│   └── util/                  # Utilities
│       ├── helpers.ts
│       └── CaseInsensitiveMap.ts
│
└── storage/
    └── StorageLevel.ts
```

### Dependency Graph

```
SparkSession
    ↓
DataFrame ← uses → Column
    ↓                ↓
DataFrameReader   functions.ts
DataFrameWriter      ↓
    ↓              ExpressionBuilder
RelationBuilder       ↓
    ↓              Generated Protobuf
PlanBuilder           ↓
    ↓              gRPC Client
Generated Protobuf    ↓
    ↓              Spark Connect Server
gRPC Client
```

## Extension Points

### Adding New DataFrame Operations

1. Add method to `DataFrame.ts`
2. Create relation builder in `RelationBuilder.ts`
3. Use existing protobuf messages or add new ones
4. Add tests

### Adding New SQL Functions

1. Add function to `functions.ts`
2. Create expression builder in `ExpressionBuilder.ts`
3. Use existing protobuf expressions
4. Add tests and documentation

### Adding New Data Types

1. Add type class in `src/org/apache/spark/sql/types/`
2. Update `DataTypes.ts` factory
3. Handle in serialization/deserialization
4. Add tests

## Performance Considerations

### Lazy Evaluation

Transformations are lazy - multiple transformations build a single execution plan without intermediate results.

### Predicate Pushdown

Filters applied early in the plan can be pushed down to data sources (e.g., Parquet, databases).

### Column Pruning

Selecting specific columns reduces data transfer - only selected columns are sent from server.

### Caching

Use `df.cache()` to persist frequently-used DataFrames in Spark memory.

### Arrow Optimization

Apache Arrow provides efficient columnar data transfer between Spark and client.

## Security

### No User Code Execution

spark.js only sends plans to server - no arbitrary code execution on client.

### gRPC Security

Support for SSL/TLS and authentication tokens (configure on server side).

### Input Validation

Type system and schema validation prevent many errors at compile time.

## Error Handling

### Error Types

1. **Connection errors**: Server unreachable, network issues
2. **Query errors**: Invalid SQL, type mismatches
3. **Runtime errors**: Out of memory, execution failures

### Error Propagation

Errors from Spark are wrapped and thrown as JavaScript errors with context.

## Testing Architecture

### Unit Tests

Test individual components in isolation (builders, utilities).

### Integration Tests

Test against live Spark Connect server via Docker.

### Test Utilities

`tests/helpers.ts` provides shared SparkSession and utilities.

## Future Enhancements

### Potential Improvements

1. **UDF support**: User-defined functions in JavaScript
2. **Streaming**: Support for Spark Structured Streaming
3. **ML support**: Machine learning model operations
4. **Advanced optimizations**: Client-side optimization hints
5. **Connection pooling**: Reuse connections efficiently
6. **Better TypeScript types**: Generic types for DataFrame schemas

## Next Steps

- Review [Code Style](code-style.md) for coding conventions
- See [Protobuf](protobuf.md) for protocol details
- Check [Building](building.md) for build process
- Read [Testing](testing.md) for testing approach
