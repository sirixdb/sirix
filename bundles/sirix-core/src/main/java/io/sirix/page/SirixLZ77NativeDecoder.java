/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.io.bytepipe.NativeCodecLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.Linker.Option;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFI bridge to the native Sirix LZ77 decoder. Mirrors the wire format of {@link SirixLZ77Codec} so
 * pages encoded by the Java encoder decode identically via the C implementation.
 *
 * <h2>Why a native decoder?</h2> The Java decoder's per-token serial dependency chain plateaus at
 * ~3.0 GB/s even with 8-byte {@link sun.misc.Unsafe} stride copies. A C implementation compiled
 * with {@code -O3 -march=native -mavx2 -flto} auto-vectorises literal + match copies into 16-byte
 * SSE moves and tighter instruction scheduling. Measured ~3.3 GB/s on realistic token-dense record
 * heaps vs ~2.2 GB/s for {@code LZ4_decompress_safe} on the same data (LZ4 is our reference
 * baseline).
 *
 * <h2>Loading</h2> {@link NativeCodecLibrary} loads the bundled library once per process. If loading
 * fails we fall back to the pure-Java decoder transparently. Staging and platform requirements are
 * documented in {@code docs/operations.md}.
 *
 * <h2>HFT constraints</h2>
 * <ul>
 * <li>Zero allocation on the hot path (steady state). The {@code byte[]} input is passed directly
 * to native via {@link Linker.Option#critical(boolean) critical(true)}, which pins the heap array
 * for the duration of the call. The {@link MemorySegment#ofArray(byte[])} wrapper is a trivial heap
 * object the JIT can stack-allocate.</li>
 * <li>Final static {@link MethodHandle} allowing {@code invokeExact} inlining.</li>
 * <li>No off-heap pools to manage — the critical-linkage pin is zero-cost.</li>
 * </ul>
 */
public final class SirixLZ77NativeDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(SirixLZ77NativeDecoder.class);

  /** True if the native library is loaded and the decode symbol resolved. */
  private static final boolean NATIVE_AVAILABLE;

  /** Address resolved in this process; never captured in a native image's build-time heap. */
  private static final MemorySegment DECODE_SYMBOL;

  /**
   * The signature has no library, arena, property or process state; the symbol is passed per call.
   * Native Image initializes this holder at run time by default, because the 25.0.x LTS line rejects
   * a build-time downcall handle with a {@code linkToNative} compilation error. An optimized build on
   * a GraalVM 25 innovation release 25.1.3 or later may preinitialize this pure adapter; the library
   * and its symbol remain process-local runtime state. See {@code docs/NATIVE_IMAGE.md}.
   */
  private static final class DecodeCall {
    private static final MethodHandle HANDLE =
        Linker.nativeLinker()
              .downcallHandle(FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                  ValueLayout.ADDRESS, ValueLayout.JAVA_INT), Option.critical(true));
  }

  /**
   * Gate the native path on/off. Defaults to on whenever the library loaded successfully. Users can
   * force the Java path via {@code -Dsirix.lz77Codec.native.disable=true}.
   */
  private static final boolean FORCE_DISABLE = Boolean.getBoolean("sirix.lz77Codec.native.disable");

  static {
    boolean available = false;
    MemorySegment symbol = null;

    if (FORCE_DISABLE) {
      LOGGER.info("SirixLZ77NativeDecoder disabled via sirix.lz77Codec.native.disable");
    } else {
      try {
        final SymbolLookup lookup = NativeCodecLibrary.lookup();
        if (lookup != null) {
          // critical(true) allows passing heap MemorySegments directly to
          // native code — the JVM pins them during the call. This lets us
          // skip the byte[] → native-memory copy that would otherwise
          // dominate the per-call latency for small frames.
          symbol = lookup.find("sirix_lz77_decode").orElseThrow();
          // Initialize inside the guarded load so unsupported linkage retains the Java fallback.
          DecodeCall.HANDLE.type();
          available = true;
          LOGGER.info("SirixLZ77NativeDecoder loaded (critical=true)");
        }
      } catch (final Throwable t) {
        LOGGER.warn("SirixLZ77NativeDecoder failed to load: {} — falling back to Java decoder", t.getMessage());
      }
    }

    NATIVE_AVAILABLE = available;
    DECODE_SYMBOL = symbol;
  }

  private SirixLZ77NativeDecoder() {}

  /**
   * @return {@code true} if the native decoder was loaded successfully and can be used as a drop-in
   *         replacement for the Java decoder.
   */
  public static boolean isAvailable() {
    return NATIVE_AVAILABLE;
  }

  /**
   * Decode an LZ77 frame from {@code input[inputOff .. inputOff+inputLen)} into {@code output}
   * starting at {@code outputOff}.
   *
   * <p>
   * Preconditions enforced by the caller (see {@link SirixLZ77Codec#decode}):
   * <ul>
   * <li>{@code output.byteSize() - outputOff} must be at least {@code uncompressed + 64} bytes — the
   * C decoder assumes wildCopy16 overshoot slack in its hot loop.</li>
   * <li>Both native- and heap-backed {@code output} segments are accepted; heap segments are pinned
   * for the duration of the call via critical-linkage.</li>
   * </ul>
   *
   * @return number of bytes decoded (equals the {@code uncompressed} value from the frame header)
   * @throws IllegalStateException if the native decoder returns a negative error code — which
   *         indicates a malformed input.
   */
  public static int decode(final byte[] input, final int inputOff, final int inputLen, final MemorySegment output,
      final long outputOff) {
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
      rc = (int) DecodeCall.HANDLE.invokeExact(DECODE_SYMBOL, inputSeg, inputLen, outSlice, (int) outCap);
    } catch (final Throwable t) {
      throw new RuntimeException("SirixLZ77NativeDecoder: FFI call failed", t);
    }

    if (rc < 0) {
      throw new IllegalStateException(
          "SirixLZ77NativeDecoder: decode returned error " + rc + " (inputLen=" + inputLen + ")");
    }
    return rc;
  }

}
