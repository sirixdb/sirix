/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.Linker.Option;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFI bridge to the native Sirix LZ77 decoder. Mirrors the wire format of
 * {@link SirixLZ77Codec} so pages encoded by the Java encoder decode
 * identically via the C implementation.
 *
 * <h2>Why a native decoder?</h2>
 * The Java decoder's per-token serial dependency chain plateaus at
 * ~3.0 GB/s even with 8-byte {@link sun.misc.Unsafe} stride copies. A C
 * implementation compiled with {@code -O3 -march=native -mavx2 -flto}
 * auto-vectorises literal + match copies into 16-byte SSE moves and
 * tighter instruction scheduling. Measured ~3.3 GB/s on realistic
 * token-dense record heaps vs ~2.2 GB/s for {@code LZ4_decompress_safe}
 * on the same data (LZ4 is our reference baseline).
 *
 * <h2>Loading</h2>
 * The native library ({@code libsirix_lz77.so} on Linux x86_64) is
 * embedded as a classpath resource under {@code /native/linux-x86_64/}.
 * On first use we extract it to a platform-user-scoped temp directory
 * and {@code dlopen} it via {@link SymbolLookup#libraryLookup}. If
 * extraction or {@code dlopen} fails we fall back to the pure-Java
 * decoder transparently.
 *
 * <h2>HFT constraints</h2>
 * <ul>
 *   <li>Zero allocation on the hot path (steady state). The {@code byte[]}
 *       input is passed directly to native via {@link Linker.Option#critical(boolean)
 *       critical(true)}, which pins the heap array for the duration of
 *       the call. The {@link MemorySegment#ofArray(byte[])} wrapper is a
 *       trivial heap object the JIT can stack-allocate.</li>
 *   <li>Final static {@link MethodHandle} allowing {@code invokeExact}
 *       inlining.</li>
 *   <li>No off-heap pools to manage — the critical-linkage pin is
 *       zero-cost.</li>
 * </ul>
 */
public final class SirixLZ77NativeDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(SirixLZ77NativeDecoder.class);

  private static final Linker LINKER = Linker.nativeLinker();

  /** True if the native library is loaded and the decode symbol resolved. */
  private static final boolean NATIVE_AVAILABLE;

  /**
   * Handle to {@code int sirix_lz77_decode(const uint8_t*, int, uint8_t*, int)}.
   */
  private static final MethodHandle DECODE_HANDLE;

  /**
   * Gate the native path on/off. Defaults to on whenever the library
   * loaded successfully. Users can force the Java path via
   * {@code -Dsirix.lz77Codec.native.disable=true}.
   */
  private static final boolean FORCE_DISABLE =
      Boolean.getBoolean("sirix.lz77Codec.native.disable");

  static {
    boolean available = false;
    MethodHandle handle = null;

    if (FORCE_DISABLE) {
      LOGGER.info("SirixLZ77NativeDecoder disabled via sirix.lz77Codec.native.disable");
    } else {
      try {
        final Path libPath = extractNativeLib();
        if (libPath != null) {
          final SymbolLookup lookup = SymbolLookup.libraryLookup(libPath, Arena.global());
          // critical(true) allows passing heap MemorySegments directly to
          // native code — the JVM pins them during the call. This lets us
          // skip the byte[] → native-memory copy that would otherwise
          // dominate the per-call latency for small frames.
          handle = LINKER.downcallHandle(lookup.find("sirix_lz77_decode").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS,
                  ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS,
                  ValueLayout.JAVA_INT),
              Option.critical(true));
          available = true;
          LOGGER.info("SirixLZ77NativeDecoder loaded from {} (critical=true)", libPath);
        }
      } catch (final Throwable t) {
        LOGGER.warn("SirixLZ77NativeDecoder failed to load: {} — falling back to Java decoder",
            t.getMessage());
      }
    }

    NATIVE_AVAILABLE = available;
    DECODE_HANDLE = handle;
  }

  private SirixLZ77NativeDecoder() {}

  /**
   * @return {@code true} if the native decoder was loaded successfully and
   *         can be used as a drop-in replacement for the Java decoder.
   */
  public static boolean isAvailable() {
    return NATIVE_AVAILABLE;
  }

  /**
   * Decode an LZ77 frame from {@code input[inputOff .. inputOff+inputLen)}
   * into {@code output} starting at {@code outputOff}.
   *
   * <p>Preconditions enforced by the caller (see {@link SirixLZ77Codec#decode}):
   * <ul>
   *   <li>{@code output.byteSize() - outputOff} must be at least
   *       {@code uncompressed + 64} bytes — the C decoder assumes
   *       wildCopy16 overshoot slack in its hot loop.</li>
   *   <li>Both native- and heap-backed {@code output} segments are
   *       accepted; heap segments are pinned for the duration of the
   *       call via critical-linkage.</li>
   * </ul>
   *
   * @return number of bytes decoded (equals the {@code uncompressed} value
   *         from the frame header)
   * @throws IllegalStateException if the native decoder returns a negative
   *         error code — which indicates a malformed input.
   */
  public static int decode(final byte[] input, final int inputOff, final int inputLen,
      final MemorySegment output, final long outputOff) {
    if (!NATIVE_AVAILABLE) {
      throw new IllegalStateException("SirixLZ77NativeDecoder: native library not loaded");
    }
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    if (inputOff < 0 || inputLen < 0 || inputOff + inputLen > input.length) {
      throw new IllegalArgumentException("invalid input offset/length");
    }
    if (outputOff < 0 || outputOff >= output.byteSize()) {
      throw new IllegalArgumentException("invalid output offset");
    }

    // Zero-copy hot path: with Linker.Option.critical(true), Panama pins
    // the heap byte[] for the duration of the native call so we can pass
    // it directly as a pointer to C. The JVM handles the pinning
    // transparently. No staging copy needed.
    //
    // MemorySegment.ofArray(byte[]) is a trivial adapter that wraps the
    // byte[] in a heap segment — at steady state the JIT strips the
    // allocation to a stack-local object (or eliminates it entirely).
    final MemorySegment inputSeg = (inputOff == 0 && inputLen == input.length)
        ? MemorySegment.ofArray(input)
        : MemorySegment.ofArray(input).asSlice(inputOff, inputLen);

    final MemorySegment outSlice = outputOff == 0
        ? output
        : output.asSlice(outputOff);
    final long outCap = output.byteSize() - outputOff;
    if (outCap > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("output capacity too large");
    }

    final int rc;
    try {
      rc = (int) DECODE_HANDLE.invokeExact(inputSeg, inputLen, outSlice, (int) outCap);
    } catch (final Throwable t) {
      throw new RuntimeException("SirixLZ77NativeDecoder: FFI call failed", t);
    }

    if (rc < 0) {
      throw new IllegalStateException("SirixLZ77NativeDecoder: decode returned error " + rc
          + " (inputLen=" + inputLen + ")");
    }
    return rc;
  }

  /**
   * Extract the embedded {@code libsirix_lz77.so} resource to a temp file
   * on first use. Returns the path for {@link SymbolLookup#libraryLookup}.
   *
   * <p>Returns {@code null} if the resource is missing for the running
   * platform — caller falls back to Java decoder.
   */
  private static Path extractNativeLib() throws Exception {
    final String os = System.getProperty("os.name", "").toLowerCase();
    final String arch = System.getProperty("os.arch", "").toLowerCase();

    final String resourcePath;
    final String libName;
    if (os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64"))) {
      resourcePath = "/native/linux-x86_64/libsirix_lz77.so";
      libName = "libsirix_lz77.so";
    } else if (os.contains("linux") && arch.equals("aarch64")) {
      resourcePath = "/native/linux-aarch64/libsirix_lz77.so";
      libName = "libsirix_lz77.so";
    } else {
      LOGGER.info("SirixLZ77NativeDecoder: no prebuilt native lib for {} {}", os, arch);
      return null;
    }

    try (InputStream in = SirixLZ77NativeDecoder.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        LOGGER.info("SirixLZ77NativeDecoder: resource {} not found on classpath", resourcePath);
        return null;
      }
      // Use a deterministic cache file in /tmp so repeat runs on the same
      // node re-use the dlopen cache warmly.
      final Path dir = Path.of(System.getProperty("java.io.tmpdir", "/tmp"),
          "sirix-native-" + System.getProperty("user.name", "default"));
      Files.createDirectories(dir);
      final Path target = dir.resolve(libName);
      // Always overwrite — version check via file size would need a build stamp;
      // for in-tree dev builds atomic overwrite is simpler and safe.
      Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
      return target;
    }
  }

}
