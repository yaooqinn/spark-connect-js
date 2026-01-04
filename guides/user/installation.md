# Installation Guide

This guide covers various installation methods for spark.js.

## Requirements

### System Requirements

- **Node.js**: Version 18.x or higher (LTS version recommended)
- **npm**: Version 8.x or higher (comes with Node.js)
- **Operating System**: Linux, macOS, or Windows

### Spark Requirements

- **Spark Connect Server**: Apache Spark 4.0.0 or higher with Spark Connect enabled
- **Network Access**: Ability to connect to the Spark Connect server via gRPC

## Installation Methods

### Using npm

The standard way to install spark.js:

```bash
npm install spark.js
```

### Using yarn

If you prefer yarn:

```bash
yarn add spark.js
```

### Using pnpm

For pnpm users:

```bash
pnpm add spark.js
```

## Installing a Specific Version

To install a specific version:

```bash
npm install spark.js@0.1.0
```

## Development Installation

If you want to contribute or use the latest development version:

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yaooqinn/spark.js.git
   cd spark.js
   ```

2. **Install dependencies**:

   ```bash
   npm install
   ```

3. **Build the project**:

   ```bash
   npm run build
   ```

4. **Link locally** (optional):

   ```bash
   npm link
   ```

   Then in your project:

   ```bash
   npm link spark.js
   ```

## Setting Up Spark Connect Server

spark.js requires a Spark Connect server to function. Here are several ways to set one up:

### Option 1: Docker (Recommended for Development)

The easiest way to get started:

```bash
docker run -d -p 15002:15002 --name spark-connect \
  apache/spark:4.1.0 \
  /opt/spark/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.12:4.1.0
```

To stop the server:

```bash
docker stop spark-connect
docker rm spark-connect
```

### Option 2: Local Spark Installation

If you have Spark installed locally:

1. **Download Apache Spark** (4.0.0 or higher) from [spark.apache.org](https://spark.apache.org/downloads.html)

2. **Extract the archive**:

   ```bash
   tar -xzf spark-4.1.0-bin-hadoop3.tgz
   cd spark-4.1.0-bin-hadoop3
   ```

3. **Start the Spark Connect server**:

   ```bash
   ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:4.1.0
   ```

The server will start on port 15002 by default.

### Option 3: Cloud Providers

Many cloud providers offer managed Spark services with Connect support:

- **Databricks**: Supports Spark Connect natively
- **AWS EMR**: Can be configured with Spark Connect
- **Google Cloud Dataproc**: Supports Spark Connect
- **Azure HDInsight**: Supports Spark Connect

Refer to your cloud provider's documentation for setup instructions.

## Verifying Installation

Create a test file `test.ts`:

```typescript
import { SparkSession } from 'spark.js';

async function test() {
  try {
    const spark = await SparkSession.builder()
      .remote('sc://localhost:15002')
      .appName('InstallTest')
      .build();

    console.log('✓ Successfully connected to Spark Connect server');
    
    const df = spark.range(0, 5);
    await df.show();
    
    await spark.stop();
    console.log('✓ Installation verified successfully');
  } catch (error) {
    console.error('✗ Installation verification failed:', error);
  }
}

test();
```

Run it:

```bash
npx ts-node test.ts
```

If you see the output without errors, spark.js is properly installed!

## TypeScript Configuration

If you're using TypeScript, ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "moduleResolution": "node",
    "esModuleInterop": true,
    "strict": true
  }
}
```

## Troubleshooting

### Installation Issues

**Problem**: `npm install` fails with permission errors

**Solution**: Try using `sudo npm install -g npm` to update npm, or use a Node version manager like nvm.

**Problem**: Module not found errors

**Solution**: Clear npm cache and reinstall:
```bash
npm cache clean --force
rm -rf node_modules package-lock.json
npm install
```

### Connection Issues

**Problem**: Cannot connect to Spark Connect server

**Solution**: 
- Verify the server is running: `docker ps` or check server logs
- Ensure the port (15002) is not blocked by firewall
- Check the connection URL is correct

For more help, see the [Troubleshooting Guide](troubleshooting.md).

## Next Steps

- Follow the [Quick Start Guide](quick-start.md) to write your first application
- Learn about [Basic Concepts](basic-concepts.md)
- Explore [Examples](../../example/org/apache/spark/sql/example/) in the repository
