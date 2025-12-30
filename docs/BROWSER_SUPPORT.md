# Browser Support and gRPC-Web Proxy Setup

## Overview

As of this version, `spark-connect-js` has been refactored to support **browser-only** environments. This change replaces Node.js-specific dependencies with browser-compatible alternatives.

## Key Changes

### Dependencies
- **Removed**: `@grpc/grpc-js`, `log4js`, `tmp` (Node.js-only packages)
- **Added**: `@grpc/grpc-web` (browser-compatible gRPC client)
- **Logger**: Replaced with browser-compatible console logger

### Node.js to Browser Migration
The library no longer uses:
- File system (`fs`, `path`) APIs
- Operating system (`os`) APIs
- Process-specific (`process.env`, `process.cwd()`) APIs
- Node.js `Buffer` (replaced with `Uint8Array`)

## Browser Usage

### Installation

```bash
npm install spark-connect
```

### Building for Browser

Build the browser bundle:

```bash
npm run build
```

This creates `dist/spark-connect.js` which can be included in your HTML:

```html
<script src="dist/spark-connect.js"></script>
<script>
  const spark = SparkConnect.SparkSession.builder()
    .remote('sc://localhost:8080/;user_id=myuser;user_name=MyUser')
    .getOrCreate();
    
  spark.sql('SELECT 1 as num').show();
</script>
```

### Using with Module Bundlers

If you're using Webpack, Rollup, or another bundler:

```javascript
import { SparkSession } from 'spark-connect';

const spark = SparkSession.builder()
  .remote('sc://localhost:8080/')
  .getOrCreate();
```

## gRPC-Web Proxy Setup

**Important**: Browsers cannot directly communicate with gRPC servers. You must use a gRPC-Web proxy like Envoy or grpc-web-proxy.

### Option 1: Envoy Proxy (Recommended)

#### 1. Install Envoy

Follow the [official Envoy installation guide](https://www.envoyproxy.io/docs/envoy/latest/start/install).

#### 2. Create Envoy Configuration

Create `envoy.yaml`:

```yaml
admin:
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }

static_resources:
  listeners:
  - name: listener_0
    address:
      socket_address: { address: 0.0.0.0, port_value: 8080 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: auto
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route:
                  cluster: spark_connect_service
                  timeout: 0s
                  max_stream_duration:
                    grpc_timeout_header_max: 0s
              cors:
                allow_origin_string_match:
                  - prefix: "*"
                allow_methods: GET, PUT, DELETE, POST, OPTIONS
                allow_headers: keep-alive,user-agent,cache-control,content-type,content-transfer-encoding,custom-header-1,x-accept-content-transfer-encoding,x-accept-response-streaming,x-user-agent,x-grpc-web,grpc-timeout
                max_age: "1728000"
                expose_headers: custom-header-1,grpc-status,grpc-message
          http_filters:
          - name: envoy.filters.http.grpc_web
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.grpc_web.v3.GrpcWeb
          - name: envoy.filters.http.cors
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.cors.v3.Cors
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

  clusters:
  - name: spark_connect_service
    connect_timeout: 0.25s
    type: logical_dns
    http2_protocol_options: {}
    lb_policy: round_robin
    load_assignment:
      cluster_name: spark_connect_service
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: localhost  # Your Spark Connect server address
                port_value: 15002   # Your Spark Connect server port
```

#### 3. Run Envoy

```bash
envoy -c envoy.yaml
```

#### 4. Connect from Browser

Your browser application should now connect to `http://localhost:8080` (Envoy proxy), which forwards requests to `localhost:15002` (Spark Connect server):

```javascript
const spark = SparkSession.builder()
  .remote('sc://localhost:8080/;user_id=myuser')
  .getOrCreate();
```

### Option 2: grpc-web-proxy

An alternative lightweight proxy:

```bash
# Install
go install github.com/improbable-eng/grpc-web/go/grpcwebproxy@latest

# Run
grpcwebproxy \
  --backend_addr=localhost:15002 \
  --run_tls_server=false \
  --allow_all_origins
```

## Migration from Node.js Version

### Code Changes Required

1. **Logger Usage**: No changes needed - the logger interface remains the same, but now uses console output.

2. **File System Operations**: Examples and tests that used `fs`, `tmp`, or `os` modules need manual updates:
   - Remove direct file operations
   - Use server-side paths for data access
   - Handle cleanup server-side

3. **User Identity**: The default user is now `"testuser"` instead of reading from `process.env.USER` or `os.userInfo()`.

### Connection String

No changes required for connection strings:

```javascript
// Before and After - same syntax
SparkSession.builder()
  .remote('sc://localhost:15002/;user_id=myuser;user_name=MyUser')
  .getOrCreate();
```

### Testing

Browser-based testing is not yet fully configured. Current tests still require:
- A running Spark Connect server (or gRPC-Web proxy)
- Jest test runner

Future versions may include browser-based test runners like Karma or Playwright.

## Limitations

1. **No Direct File Access**: Browsers cannot access the local file system. All file operations must go through the Spark server.

2. **CORS Requirements**: The gRPC-Web proxy must be configured with appropriate CORS headers for browser access.

3. **Streaming**: gRPC-Web supports server streaming but not client streaming or bidirectional streaming.

4. **Binary Size**: Browser bundles may be larger than Node.js builds due to included polyfills.

## Troubleshooting

### CORS Errors

If you see CORS errors in the browser console:
- Ensure your Envoy/proxy configuration includes proper CORS settings
- Check that `allow_origin_string_match` includes your origin
- Verify `allow_headers` includes all required gRPC-Web headers

### Connection Refused

- Verify the proxy is running on the correct port
- Check that the proxy can reach the Spark Connect server
- Ensure the browser is using the proxy address (e.g., `localhost:8080`), not the direct server address

### Build Errors

If you encounter build errors:
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- Check that webpack and ts-loader are installed
- Verify TypeScript version compatibility

## Resources

- [gRPC-Web Documentation](https://grpc.io/docs/platforms/web/)
- [Envoy Proxy gRPC-Web Filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_web_filter)
- [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
