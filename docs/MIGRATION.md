# Migration Guide: Node.js to Browser-Only

This document explains the changes made to transition `spark-connect-js` from a Node.js-only library to a browser-only library using gRPC-Web.

## Overview

The library has been completely refactored to remove all Node.js-specific dependencies and work exclusively in browser environments. This required significant changes to the gRPC transport layer, logging, and file handling.

## Breaking Changes

### 1. gRPC Transport Layer

**Before (Node.js):**
```javascript
// Used @grpc/grpc-js - Node.js native gRPC
import * as grpc from '@grpc/grpc-js';
```

**After (Browser):**
```javascript
// Uses grpc-web - browser-compatible gRPC
import * as grpcWeb from 'grpc-web';
```

**Impact:**
- Requires a gRPC-Web proxy (Envoy or grpcwebproxy) between browser and Spark server
- Connection strings point to proxy address (e.g., `localhost:8080`) not direct server address
- SSL/TLS configuration works differently (handled at proxy level)

### 2. Logging

**Before (Node.js):**
```javascript
// Used log4js with file system logging
import log4js from 'log4js';
// Logs written to files in logs/ directory
```

**After (Browser):**
```javascript
// Uses console logger
class ConsoleLogger implements Logger {
  debug(...) { console.debug('[spark-connect]', ...); }
  info(...) { console.info('[spark-connect]', ...); }
  // ...
}
```

**Impact:**
- All logs go to browser console instead of files
- No log configuration file (log4js.json is no longer used)
- Log levels controlled by browser console filters

### 3. File System Operations

**Before (Node.js):**
```javascript
// Could use fs, path, tmp modules
import { rm } from 'fs';
import { dir } from 'tmp';

// Cleanup temporary files
rm(tempDir, { recursive: true }, callback);
```

**After (Browser):**
```javascript
// No file system access
// File cleanup must be handled server-side or manually
console.log('Files created at:', tempDir);
```

**Impact:**
- Examples no longer clean up temporary files automatically
- Data must be accessed via server-side paths
- No local file uploads/downloads through the client library

### 4. User Identity

**Before (Node.js):**
```javascript
// Could access process.env and os.userInfo()
import os from 'os';
const user = process.env.USER || os.userInfo().username;
```

**After (Browser):**
```javascript
// Uses default or explicitly provided user
const user = 'testuser'; // or from connection string
```

**Impact:**
- Must explicitly provide user_id in connection string
- No automatic detection from environment

### 5. Binary Data Handling

**Before (Node.js):**
```javascript
// Used Node.js Buffer
return Buffer.from(toBinary(desc, msg));
```

**After (Browser):**
```javascript
// Uses Uint8Array (browser standard)
return toBinary(desc, msg); // Returns Uint8Array
```

**Impact:**
- Minor - mostly internal change
- May affect custom serialization code if any

## New Requirements

### gRPC-Web Proxy Setup

**Required for all browser usage:**

1. Install Envoy or grpcwebproxy
2. Configure proxy to forward to Spark Connect server
3. Enable CORS headers
4. Point browser connections to proxy address

See [BROWSER_SUPPORT.md](BROWSER_SUPPORT.md) for detailed setup instructions.

### Connection String Changes

**Before:**
```javascript
// Direct connection to Spark server
SparkSession.builder()
  .remote('sc://localhost:15002/')
  .getOrCreate();
```

**After:**
```javascript
// Connection to gRPC-Web proxy
SparkSession.builder()
  .remote('sc://localhost:8080/') // Proxy port, not server port
  .getOrCreate();
```

## Removed Dependencies

The following packages have been completely removed:

- `@grpc/grpc-js` → replaced with `grpc-web`
- `log4js` → replaced with console logger
- `tmp` → removed (no temp file support)
- `@types/tmp` → removed

## Added Dependencies

- `grpc-web` - Browser-compatible gRPC client
- `webpack` - For creating browser bundles
- `webpack-cli` - Webpack command-line interface

## Features No Longer Available

### 1. Direct gRPC Connection
- Cannot connect directly from browser to Spark server
- Must use gRPC-Web proxy

### 2. File System Access
- Cannot read/write local files
- Cannot create temporary directories
- File paths must be server-side paths

### 3. Process Environment
- No access to environment variables
- No process information

### 4. Node.js Built-in Modules
- No `fs`, `path`, `os`, `crypto`, `stream`, `buffer`
- Webpack configured to exclude these modules

## Features That Still Work

### 1. Core Spark Operations
✅ SQL queries
✅ DataFrame operations
✅ Data transformations
✅ Aggregations
✅ Joins
✅ Window functions

### 2. Data Sources
✅ Reading data (Parquet, CSV, JSON, ORC, etc.)
✅ Writing data
✅ Table operations

### 3. Catalog Operations
✅ Database management
✅ Table metadata
✅ Function listing

## Testing Changes

### Before (Node.js):
```javascript
// Could use tmp module for test directories
import { dir } from 'tmp';
const tempDir = await createTempDir();
```

### After (Browser):
```javascript
// Use mock paths or skip file-based tests
const tempDir = `/tmp/test-${Date.now()}`;
// Note: Cleanup not available in browser
```

## Build Process

### New Build Scripts

```bash
# Build production bundle for browser
npm run build

# Build development bundle (with source maps)
npm run build:dev
```

### Output

- Creates `dist/spark-connect.js` - Browser-compatible UMD bundle
- Can be included via `<script>` tag or module bundler
- Exposed as global `SparkConnect` variable

## Compatibility Matrix

| Feature | Node.js Version | Browser Version |
|---------|----------------|-----------------|
| gRPC Connection | ✅ Direct | ⚠️ Via Proxy |
| SQL Queries | ✅ | ✅ |
| DataFrame API | ✅ | ✅ |
| File System | ✅ | ❌ |
| Logging to Files | ✅ | ❌ |
| Console Logging | ✅ | ✅ |
| Environment Variables | ✅ | ❌ |
| Streaming | ✅ Server Streaming | ✅ Server Streaming |
| Binary Data | ✅ Buffer | ✅ Uint8Array |

## Upgrade Path

### For Node.js Users

If you were using the Node.js version and need to continue using it:

1. Stay on the previous version
2. Consider creating a Node.js adapter layer
3. Or use Apache Arrow Flight SQL instead

This library is now **browser-only** and is not recommended for Node.js environments.

### For New Browser Users

1. Install the library: `npm install spark-connect`
2. Set up gRPC-Web proxy (see [BROWSER_SUPPORT.md](BROWSER_SUPPORT.md))
3. Build your bundle or include prebuilt dist file
4. Connect using proxy address
5. See [browser-example.html](browser-example.html) for working example

## Common Migration Issues

### Issue 1: "Cannot find module 'fs'"
**Cause:** Code trying to use Node.js file system
**Solution:** Remove fs usage, handle files server-side

### Issue 2: "Failed to fetch" or CORS errors
**Cause:** Proxy not configured or not running
**Solution:** Check proxy is running and CORS headers are correct

### Issue 3: Connection refused
**Cause:** Connecting to wrong address
**Solution:** Use proxy address (8080), not Spark server address (15002)

### Issue 4: Metadata/logging errors
**Cause:** grpc-js Metadata API usage
**Solution:** Use plain objects for metadata instead of grpc.Metadata class

## Additional Resources

- [BROWSER_SUPPORT.md](BROWSER_SUPPORT.md) - Complete browser setup guide
- [envoy-config.yaml](envoy-config.yaml) - Ready-to-use Envoy configuration
- [browser-example.html](browser-example.html) - Interactive browser example
- [gRPC-Web Documentation](https://grpc.io/docs/platforms/web/)
- [Envoy Proxy Docs](https://www.envoyproxy.io/)

## Support

For issues related to:
- **Browser compatibility**: Check BROWSER_SUPPORT.md
- **Proxy setup**: See envoy-config.yaml and Envoy docs
- **API usage**: See README.md and examples
- **General questions**: Open an issue on GitHub
