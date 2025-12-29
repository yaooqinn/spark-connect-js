# Apache Spark Connect Client for JavaScript

An <b><red>experimental</red></b> client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for [Apache Spark](https://spark.apache.org/) written in [TypeScript](https://www.typescriptlang.org/).

**🌐 Browser-Only Support**: This library has been refactored to run in web browsers. It uses `@grpc/grpc-web` instead of Node.js-specific gRPC libraries.

## ⚠️ Important: gRPC-Web Proxy Required

Browsers cannot directly communicate with gRPC servers. You **must** set up a gRPC-Web proxy (e.g., [Envoy](https://www.envoyproxy.io/)) to use this library in browsers.

See [Browser Support Documentation](docs/BROWSER_SUPPORT.md) for detailed setup instructions.

## Quick Start

### 1. Set up gRPC-Web Proxy (Required)

Create `envoy-config.yaml` (see [docs/envoy-config.yaml](docs/envoy-config.yaml)) and run:

```bash
# Install Envoy (if not already installed)
# On macOS: brew install envoy
# On Linux: see https://www.envoyproxy.io/docs/envoy/latest/start/install

# Start Envoy proxy
envoy -c docs/envoy-config.yaml
```

This starts a proxy on port 8080 that forwards to your Spark Connect server on port 15002.

### 2. Installation

```bash
npm install spark-connect
```

### 3. Browser Usage

Build the browser bundle:

```bash
npm run build
```

Include in your HTML:

```html
<script src="dist/spark-connect.js"></script>
<script>
  const spark = SparkConnect.SparkSession.builder()
    .remote('sc://localhost:8080/') // Proxy address, not direct Spark server
    .getOrCreate();
    
  spark.sql('SELECT 1 as num').show();
</script>
```

Or use with a module bundler:

```javascript
import { SparkSession } from 'spark-connect';

const spark = SparkSession.builder()
  .remote('sc://localhost:8080/;user_id=myuser')
  .getOrCreate();

// Run SQL queries
const df = spark.sql('SELECT * FROM my_table');
await df.show();

// Use DataFrame API
const df2 = spark.range(10).filter('id > 5');
await df2.collect();
```

### 4. Try the Browser Example

Open [docs/browser-example.html](docs/browser-example.html) in your browser for an interactive example.

## Key Changes from Previous Versions

### Dependencies Removed
- ❌ `@grpc/grpc-js` (Node.js only) → ✅ `@grpc/grpc-web` (browser compatible)
- ❌ `log4js` (Node.js filesystem logging) → ✅ Console logger (browser compatible)
- ❌ `tmp`, `fs`, `os`, `path` (Node.js APIs) → ✅ Removed

### Migration Notes

If you were using the Node.js version:
1. **Setup a gRPC-Web proxy** (required for browser access)
2. **Update connection strings** to point to the proxy (e.g., port 8080 instead of 15002)
3. **Remove file system operations** from client code
4. See [Browser Support Documentation](docs/BROWSER_SUPPORT.md) for complete migration guide

## Documentation

- [Browser Support & Proxy Setup](docs/BROWSER_SUPPORT.md) - **Start here for setup instructions**
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [gRPC-Web Documentation](https://grpc.io/docs/platforms/web/)

# Roadmaps
- [ ] For minor changes or some features associated with certain classes, SEARCH 'TODO'
- [ ] Support Retry / Reattachable execution
- [ ] Support Checkpoint for DataFrame
- [ ] Support DataFrameNaFunctions
- [x] Support DataFrame Join 
- [x] Browser-only support with gRPC-Web
- [ ] Browser-based testing (Karma/Playwright)
- [ ] UserDefinedType support
  - [ ] UserDefinedType declaration
  - [ ] UserDefinedType & Proto bidi-conversions
  - [ ] UserDefinedType & Arrow bidi-conversions

