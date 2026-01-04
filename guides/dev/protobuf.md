# Protocol Buffers Development

Guide to working with Protocol Buffers (protobuf) in spark.js.

## Overview

spark.js uses Protocol Buffers to communicate with Spark Connect servers via gRPC. Proto files define the message formats and RPC services.

**Key Facts**:
- **Proto version**: proto3
- **Tool**: Buf (v1.47.2)
- **Generator**: @bufbuild/protoc-gen-es
- **Location**: `protobuf/spark/connect/`
- **Generated code**: `src/gen/spark/connect/`

## Prerequisites

- **Buf CLI**: Installed via npm (`@bufbuild/buf`)
- **Proto files**: From Apache Spark repository
- **Understanding of protobuf**: Basic knowledge recommended

## Proto Files

### Directory Structure

```
protobuf/spark/connect/
├── base.proto              # Core message types
├── catalog.proto           # Catalog operations
├── commands.proto          # Command messages
├── common.proto            # Common definitions
├── example_plugins.proto   # Plugin examples
├── expressions.proto       # Expression messages
├── relations.proto         # Relation messages
└── types.proto             # Type definitions
```

### Key Message Types

**base.proto**:
- `Plan` - Execution plan
- `Relation` - Data relation (table, join, etc.)
- `Expression` - Column expressions
- `Command` - Commands to execute

**relations.proto**:
- `Read` - Read data source
- `Project` - Select columns
- `Filter` - Filter rows
- `Join` - Join operations
- `Aggregate` - Aggregation operations

**expressions.proto**:
- `Literal` - Constant values
- `UnresolvedAttribute` - Column references
- `UnresolvedFunction` - Function calls
- `Alias` - Column aliases

## Buf Configuration

### buf.work.yaml

Defines the workspace:

```yaml
version: v2
directories:
  - protobuf
```

### buf.gen.yaml

Configures code generation:

```yaml
version: v2
plugins:
  - remote: buf.build/bufbuild/es:v2.2.3
    out: src/gen
    opt:
      - target=ts
```

This generates TypeScript code using the `@bufbuild/protoc-gen-es` plugin.

## Generating Code

### Basic Generation

Whenever proto files are modified:

```bash
npx buf generate
```

**Output**: TypeScript files in `src/gen/spark/connect/`

**Time**: ~2-3 seconds

### What Gets Generated

For each `.proto` file, Buf generates a `_pb.ts` file:

```
protobuf/spark/connect/base.proto
→ src/gen/spark/connect/base_pb.ts

protobuf/spark/connect/expressions.proto
→ src/gen/spark/connect/expressions_pb.ts
```

Generated files include:
- Message classes
- Enum definitions
- Type definitions
- Serialization/deserialization methods

### Generated Code Example

From `base.proto`:

```protobuf
message Plan {
  oneof op_type {
    Relation root = 1;
    Command command = 2;
  }
}
```

Generates TypeScript:

```typescript
export class Plan extends Message<Plan> {
  opType:
    | { case: "root"; value: Relation }
    | { case: "command"; value: Command }
    | { case: undefined; value?: undefined } = { case: undefined };

  // Methods for serialization, etc.
}
```

## Using Generated Code

### Importing Messages

```typescript
// Import from generated files
import { Plan } from '../../../gen/spark/connect/base_pb';
import { Relation } from '../../../gen/spark/connect/base_pb';
import { Expression } from '../../../gen/spark/connect/expressions_pb';
```

### Creating Messages

```typescript
import { Plan, Relation } from '../../../gen/spark/connect/base_pb';
import { Read } from '../../../gen/spark/connect/relations_pb';

// Create a new message
const plan = new Plan({
  opType: {
    case: 'root',
    value: new Relation({
      relType: {
        case: 'read',
        value: new Read({
          // Read configuration
        })
      }
    })
  }
});
```

### Working with Oneofs

Protobuf oneofs are represented as discriminated unions:

```typescript
// Setting oneof field
const plan = new Plan();
plan.opType = {
  case: 'root',
  value: new Relation()
};

// Checking oneof field
if (plan.opType.case === 'root') {
  const relation = plan.opType.value;
  // TypeScript knows this is a Relation
}

// Pattern matching
switch (plan.opType.case) {
  case 'root':
    handleRelation(plan.opType.value);
    break;
  case 'command':
    handleCommand(plan.opType.value);
    break;
  case undefined:
    handleEmpty();
    break;
}
```

### Working with Repeated Fields

```typescript
import { StructField } from '../../../gen/spark/connect/types_pb';

const field1 = new StructField({ name: 'id' });
const field2 = new StructField({ name: 'name' });

// Repeated fields are arrays
const fields = [field1, field2];
```

## Message Builders

spark.js uses builder classes to simplify message creation.

### Builder Pattern

Located in `src/org/apache/spark/sql/proto/`:

```typescript
// PlanBuilder.ts
export class PlanBuilder {
  static createPlan(relation: Relation): Plan {
    return new Plan({
      opType: {
        case: 'root',
        value: relation
      }
    });
  }
}

// RelationBuilder.ts
export class RelationBuilder {
  static createRead(path: string): Relation {
    return new Relation({
      relType: {
        case: 'read',
        value: new Read({
          // Read configuration
        })
      }
    });
  }
}
```

### Using Builders

```typescript
import { PlanBuilder } from './proto/PlanBuilder';
import { RelationBuilder } from './proto/RelationBuilder';

// Build complex messages easily
const relation = RelationBuilder.createFilter(
  RelationBuilder.createRead('data.csv'),
  ExpressionBuilder.createGreaterThan(
    ExpressionBuilder.createAttribute('age'),
    ExpressionBuilder.createLiteral(18)
  )
);

const plan = PlanBuilder.createPlan(relation);
```

## Modifying Proto Files

### When to Modify

**Usually**: You don't need to modify proto files. They come from Apache Spark.

**Rare cases**: If contributing new features to Spark Connect protocol.

### Process

1. **Edit** proto file in `protobuf/spark/connect/`

2. **Regenerate** code:
   ```bash
   npx buf generate
   ```

3. **Update builders** in `src/org/apache/spark/sql/proto/`

4. **Update tests** for new functionality

5. **Commit both** `.proto` and generated files:
   ```bash
   git add protobuf/spark/connect/myfile.proto
   git add src/gen/spark/connect/myfile_pb.ts
   git commit -m "Add new protobuf message"
   ```

## Best Practices

### Do's

✅ **Commit generated code** to git (enables development without Buf)
✅ **Use builder classes** for complex message creation
✅ **Type check with TypeScript** to catch protobuf errors early
✅ **Keep proto files synchronized** with Spark Connect protocol

### Don'ts

❌ **Don't manually edit** files in `src/gen/`
❌ **Don't modify proto files** unless contributing to Spark
❌ **Don't forget to regenerate** after pulling proto changes
❌ **Don't commit only proto files** without generated code

## Serialization

### Converting to Bytes

```typescript
import { Plan } from '../../../gen/spark/connect/base_pb';

const plan = new Plan(/* ... */);

// Serialize to binary
const bytes = plan.toBinary();

// Send over gRPC
await client.executePlan(bytes);
```

### Converting from Bytes

```typescript
import { Plan } from '../../../gen/spark/connect/base_pb';

// Receive from gRPC
const bytes = await client.receivePlan();

// Deserialize
const plan = Plan.fromBinary(bytes);
```

## Debugging

### Print Message Content

```typescript
import { Plan } from '../../../gen/spark/connect/base_pb';

const plan = new Plan(/* ... */);

// Convert to JSON for inspection
console.log(plan.toJson());

// Pretty print
console.log(JSON.stringify(plan.toJson(), null, 2));
```

### Validate Messages

```typescript
// TypeScript helps catch errors at compile time
const plan = new Plan({
  opType: {
    case: 'root',
    value: 'invalid'  // TypeScript error: not a Relation
  }
});

// Runtime validation happens during serialization
try {
  plan.toBinary();
} catch (error) {
  console.error('Invalid message:', error);
}
```

## Troubleshooting

### Generated Code is Missing

**Solution**: Run `npx buf generate`

### Generated Code is Out of Date

**Solution**: 
```bash
rm -rf src/gen/
npx buf generate
```

### Type Errors After Regeneration

**Cause**: Proto schema changed

**Solution**: Update builder classes and calling code to match new schema

### Buf Command Not Found

**Solution**: 
```bash
npm install
# Buf is in devDependencies
```

## Resources

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Buf Documentation](https://buf.build/docs)
- [Bufbuild for TypeScript](https://github.com/bufbuild/protobuf-es)
- [Spark Connect Protocol](https://github.com/apache/spark/tree/master/connector/connect)

## Next Steps

- Review [Architecture](architecture.md) to understand how protobuf fits into the system
- See [Building](building.md) for build process including protobuf generation
- Check [Code Style](code-style.md) for working with generated code
