# GraalVM Native Image Configuration for SirixDB REST API

## Current Status: Work in Progress

This directory contains GraalVM native-image configuration for building SirixDB REST API as a native executable.

### What's Implemented

- **Gradle Plugin**: `org.graalvm.buildtools.native` v0.10.4 configured in `build.gradle`
- **Reflection Config**: Basic reflection configuration for Vert.x, Netty, Sirix core classes
- **Resource Config**: Native libraries (LZ4, JNA) and configuration files
- **JNI Config**: JNA and native library support
- **FFI Support**: Configuration for Sirix's FFI-based memory allocator and LZ4 compressor
- **Simplified Netty Initialization**: All Netty classes initialized at run-time to avoid InetAddress issues

### Known Blocker

**GraalVM 25 + Netty Incompatibility**

GraalVM's built-in Netty substitutions (`io.netty.util.NetUtilSubstitutions`) store `InetAddress` objects at build time, but GraalVM 25 requires `java.net.*` classes to be run-time initialized for JNI support.

Error: `An object of type 'java.net.Inet4Address' was found in the image heap`

This is a known incompatibility between:
- GraalVM's Netty optimization substitutions (BUILD_TIME)
- JDK's network class requirements (RUN_TIME for JNI)

### Current Approach

We've simplified the Netty initialization to use a single run-time directive:
```
--initialize-at-run-time=io.netty
```

This defers all Netty initialization to run-time, which should avoid the InetAddress image heap issue. If this doesn't work, alternative approaches include:

1. **Vert.x 5 Upgrade**: Vert.x 5.0 uses Netty 4.2 with better native-image support
2. **Custom GraalVM Feature**: Create a feature to disable problematic substitutions
3. **Wait for GraalVM Fix**: Oracle may fix NetUtilSubstitutions in a future release
4. **Use native-image-agent**: Generate accurate config through runtime tracing

### Building

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
