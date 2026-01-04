# Debugging Guide

Techniques and tools for debugging spark.js applications and development.

## Overview

This guide covers debugging techniques for:
- Application code using spark.js
- Development and testing of spark.js itself
- Connection and server issues
- Performance problems

## Debugging Applications

### Node.js Debugger

Use Node.js built-in debugger:

```bash
# Run with inspector
node --inspect your-app.js

# Break on start
node --inspect-brk your-app.js
```

Then connect with:
- Chrome DevTools: Navigate to `chrome://inspect`
- VS Code: Attach to process

### VS Code Debugging

Create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "node",
      "request": "launch",
      "name": "Debug Application",
      "program": "${workspaceFolder}/your-app.ts",
      "preLaunchTask": "tsc: build",
      "outFiles": ["${workspaceFolder}/dist/**/*.js"],
      "skipFiles": ["<node_internals>/**"]
    }
  ]
}
```

Set breakpoints in VS Code and press F5 to start debugging.

### TypeScript Source Maps

Ensure source maps are enabled in `tsconfig.json`:

```json
{
  "compilerOptions": {
    "sourceMap": true
  }
}
```

This allows debugging TypeScript directly instead of compiled JavaScript.

## Debugging Tests

### Debug Single Test

```bash
# Run specific test with inspector
node --inspect-brk node_modules/.bin/jest --runInBand SparkSession.test.ts
```

### VS Code Test Debugging

Add to `.vscode/launch.json`:

```json
{
  "type": "node",
  "request": "launch",
  "name": "Jest Debug",
  "program": "${workspaceFolder}/node_modules/.bin/jest",
  "args": [
    "--runInBand",
    "--no-cache",
    "${fileBasename}"
  ],
  "console": "integratedTerminal",
  "internalConsoleOptions": "neverOpen"
}
```

Open a test file and press F5 to debug it.

### Debug Test Hangs

Tests hanging? Check these:

1. **Verify server is running**:
   ```bash
   docker ps | grep sparkconnect
   ```

2. **Check server logs**:
   ```bash
   docker logs sparkconnect
   ```

3. **Verify connection**:
   ```bash
   telnet localhost 15002
   # or
   nc -zv localhost 15002
   ```

4. **Increase timeout**:
   ```typescript
   test('slow test', async () => {
     // ...
   }, 60000);  // 60 second timeout
   ```

## Logging

### Application Logging

spark.js uses log4js for logging:

```typescript
import { getLogger } from './org/apache/spark/logger';

const logger = getLogger('MyModule');

logger.debug('Debug message');
logger.info('Info message');
logger.warn('Warning message');
logger.error('Error message', error);
```

### Log Configuration

Configure logging in `log4js.json`:

```json
{
  "appenders": {
    "out": {
      "type": "stdout"
    },
    "file": {
      "type": "file",
      "filename": "logs/spark.log"
    }
  },
  "categories": {
    "default": {
      "appenders": ["out", "file"],
      "level": "info"
    }
  }
}
```

### Enable Debug Logging

Change level to `debug` for verbose output:

```json
{
  "categories": {
    "default": {
      "level": "debug"
    }
  }
}
```

### View Logs

```bash
# Tail log file
tail -f logs/spark.log

# Search logs
grep ERROR logs/spark.log

# View last 100 lines
tail -n 100 logs/spark.log
```

## gRPC Debugging

### Enable gRPC Logging

Set environment variables:

```bash
export GRPC_VERBOSITY=DEBUG
export GRPC_TRACE=all

node your-app.js
```

This shows detailed gRPC communication.

### Inspect gRPC Messages

Add logging to Client.ts:

```typescript
async executePlan(plan: Plan): Promise<Response> {
  // Log outgoing request
  logger.debug('Sending plan:', JSON.stringify(plan.toJson(), null, 2));
  
  const response = await this.client.executePlan(plan.toBinary());
  
  // Log incoming response
  logger.debug('Received response:', response);
  
  return response;
}
```

### Network Traffic Inspection

Use `tcpdump` or Wireshark to inspect network traffic:

```bash
# Capture traffic on port 15002
sudo tcpdump -i any -n port 15002 -w spark.pcap

# Analyze with Wireshark
wireshark spark.pcap
```

## DataFrame Debugging

### Print Execution Plan

See how Spark will execute your query:

```typescript
const df = spark.range(100)
  .filter('id > 50')
  .select('id');

// Show logical plan
df.explain();

// Show detailed plan
df.explain(true);
```

### Print Schema

Verify DataFrame structure:

```typescript
// Print schema
df.printSchema();

// Get column names
console.log('Columns:', df.columns());

// Get data types
console.log('Types:', df.dtypes());
```

### Inspect Data

```typescript
// Show first few rows
await df.show();

// Show more rows
await df.show(50);

// Show without truncation
await df.show(20, false);

// Collect and inspect
const rows = await df.take(5);
console.log('First 5 rows:', rows);
```

### Count Rows at Each Stage

```typescript
const df1 = spark.range(1000);
console.log('Initial count:', await df1.count());

const df2 = df1.filter('id > 500');
console.log('After filter:', await df2.count());

const df3 = df2.distinct();
console.log('After distinct:', await df3.count());
```

## Performance Debugging

### Measure Execution Time

```typescript
async function timeOperation(name: string, fn: () => Promise<any>) {
  const start = Date.now();
  await fn();
  const duration = Date.now() - start;
  console.log(`${name} took ${duration}ms`);
}

await timeOperation('Count', async () => {
  await df.count();
});

await timeOperation('Collect', async () => {
  await df.collect();
});
```

### Profile with Node.js

```bash
# Generate CPU profile
node --prof your-app.js

# Process profile
node --prof-process isolate-*.log > profile.txt

# View profile
less profile.txt
```

### Memory Profiling

```bash
# Enable heap snapshots
node --expose-gc --max-old-space-size=4096 your-app.js
```

Use Chrome DevTools to analyze heap snapshots.

### Check Query Plan Efficiency

```typescript
// Compare query plans
const inefficient = df
  .select('*')
  .filter('age > 18')
  .select('name');

const efficient = df
  .filter('age > 18')
  .select('name');

inefficient.explain();
efficient.explain();
// Efficient plan should show filter before select (predicate pushdown)
```

## Docker/Server Debugging

### Server Logs

```bash
# Follow logs in real-time
docker logs -f sparkconnect

# Last 100 lines
docker logs --tail 100 sparkconnect

# Logs since 1 hour ago
docker logs --since 1h sparkconnect
```

### Server Status

```bash
# Check if container is running
docker ps | grep sparkconnect

# Check container details
docker inspect sparkconnect

# Check resource usage
docker stats sparkconnect
```

### Access Server Shell

```bash
# Execute command in container
docker exec sparkconnect ps aux

# Interactive shell
docker exec -it sparkconnect /bin/bash
```

### Restart Server

```bash
docker restart sparkconnect
```

### Server Not Starting

1. Check logs: `docker logs sparkconnect`
2. Check port availability: `lsof -i :15002`
3. Check Docker resources: `docker system df`
4. Rebuild image: `docker build --no-cache -t scs .github/docker`

## Common Issues

### Connection Refused

**Debug steps**:

1. Verify server is running
2. Check port is accessible
3. Verify URL is correct
4. Check firewall settings

```bash
# Test connection
telnet localhost 15002

# Check port
netstat -an | grep 15002

# Check firewall
sudo iptables -L -n | grep 15002
```

### Timeout Errors

**Debug steps**:

1. Increase timeout in code
2. Check network latency
3. Check server load
4. Review query complexity

```typescript
// Increase timeout
const spark = await SparkSession.builder()
  .config('spark.sql.connect.grpc.deadline', '600s')
  .build();
```

### Type Errors

**Debug steps**:

1. Check TypeScript version: `npx tsc --version`
2. Verify types: `npx tsc --noEmit`
3. Clear cache: `rm -rf dist/ node_modules/`
4. Reinstall: `npm install`

### Data Inconsistencies

**Debug steps**:

1. Check schema: `df.printSchema()`
2. Inspect data: `await df.show()`
3. Check for nulls: `await df.filter(col('column').isNull()).count()`
4. Verify types: `console.log(df.dtypes())`

## Debugging Tools

### Built-in Tools

- **Node.js Inspector**: `--inspect`, `--inspect-brk`
- **Console logging**: `console.log()`, `console.error()`
- **Debugger statement**: `debugger;`

### External Tools

- **Chrome DevTools**: For Node.js debugging
- **VS Code**: Integrated debugging
- **Wireshark**: Network analysis
- **tcpdump**: Packet capture

### Spark-Specific

- **df.explain()**: Show query plan
- **df.printSchema()**: Show schema
- **Server logs**: Spark execution logs

## Best Practices

1. **Use structured logging**: Include context in log messages
2. **Log errors with context**: Include relevant data
3. **Use debug levels**: Don't log sensitive data at info level
4. **Clean up debug code**: Remove debug logs before committing
5. **Use proper error handling**: Catch and log errors appropriately

## Example Debug Session

```typescript
import { SparkSession } from 'spark.js';
import { col } from 'spark.js';
import { getLogger } from './org/apache/spark/logger';

const logger = getLogger('DebugExample');

async function debugExample() {
  logger.info('Starting debug session');
  
  try {
    // Connect
    logger.debug('Connecting to Spark...');
    const spark = await SparkSession.builder()
      .remote('sc://localhost:15002')
      .build();
    logger.info('Connected successfully');
    
    // Create DataFrame
    logger.debug('Creating DataFrame...');
    const df = spark.range(100);
    logger.info('DataFrame created');
    
    // Check schema
    logger.debug('Schema:');
    df.printSchema();
    
    // Transform
    logger.debug('Applying transformation...');
    const filtered = df.filter(col('id').gt(50));
    
    // Explain plan
    logger.debug('Query plan:');
    filtered.explain();
    
    // Execute
    logger.debug('Executing query...');
    const count = await filtered.count();
    logger.info(`Result: ${count} rows`);
    
    // Cleanup
    await spark.stop();
    logger.info('Session closed');
    
  } catch (error) {
    logger.error('Error occurred:', error);
    throw error;
  }
}

debugExample().catch(console.error);
```

## Next Steps

- Review [Testing](testing.md) for test debugging
- See [Building](building.md) for build issues
- Check [Troubleshooting](../user/troubleshooting.md) for common problems
