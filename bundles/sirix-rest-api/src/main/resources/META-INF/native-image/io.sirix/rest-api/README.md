# GraalVM Native Image Configuration for SirixDB REST API

## Current Status: Work in Progress

This directory contains GraalVM native-image configuration for building SirixDB REST API as a native executable.

### What's Implemented

- **Gradle Plugin**: `org.graalvm.buildtools.native` v0.10.4 configured in `build.gradle`
- **Reflection Config**: Basic reflection configuration for Vert.x, Netty, Sirix core classes
- **Resource Config**: Native libraries (LZ4, JNA) and configuration files
- **JNI Config**: JNA and native library support
- **FFI Support**: Configuration for Sirix's FFI-based memory allocator and LZ4 compressor

### Known Blocker

**GraalVM 25 + Netty Incompatibility**

GraalVM's built-in Netty substitutions (`io.netty.util.NetUtilSubstitutions`) store `InetAddress` objects at build time, but GraalVM 25 requires `java.net.*` classes to be run-time initialized for JNI support.

Error: `An object of type 'java.net.Inet4Address' was found in the image heap`

This is a known incompatibility between:
- GraalVM's Netty optimization substitutions (BUILD_TIME)
- JDK's network class requirements (RUN_TIME for JNI)

### Workarounds Explored

1. **Vert.x 5 Upgrade** (Tested):
   - Vert.x 5.0 uses Netty 4.2 with better native-image support
   - However, Vert.x 5 has significant API breaking changes:
     - `io.vertx.core.Launcher` removed (CLI decoupled)
     - `await` coroutines function moved
     - `executeBlocking` API changed
     - Various method signatures changed
   - Requires dedicated migration effort before native-image can be tested

2. **Vert.x 4.5.x Upgrade** (Tested):
   - Uses Netty 4.1.124+ with fixes for InetAddress issues
   - Still has some API changes (`executeBlocking` overload ambiguity, `PlanStage` removed)
   - Smaller migration than Vert.x 5 but still requires code changes

3. **Custom GraalVM Feature**: Create a feature to disable problematic substitutions

4. **Wait for GraalVM Fix**: Oracle may fix NetUtilSubstitutions in a future release

5. **Use native-image-agent**: Generate accurate config through runtime tracing

### Building (Once Blocker is Resolved)

```bash
# Build native image
./gradlew :sirix-rest-api:nativeCompile

# Run native binary
./bundles/sirix-rest-api/build/native/nativeCompile/sirix-rest-api
```

### Configuration Files

- `native-image.properties` - Build arguments and initialization settings
- `reflect-config.json` - Reflection metadata for runtime class access
- `resource-config.json` - Bundled resources (native libs, config files)
- `jni-config.json` - JNI method registrations
- `serialization-config.json` - Serializable classes
- `proxy-config.json` - Dynamic proxy interfaces

### Expected Benefits (When Working)

| Metric | JVM | Native Image |
|--------|-----|--------------|
| Startup | 30-60s | < 500ms |
| Memory (idle) | 4-6GB | 200-300MB |
| Image size | ~100MB JAR | < 100MB binary |
