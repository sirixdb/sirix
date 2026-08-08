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
import java.security.MessageDigest;

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
      final byte[] lib = in.readAllBytes();
      // Content-addressed, so two builds of the library never contend for one name and a staged
      // file never has to be rewritten in place. See publishNativeLib for why in-place is fatal.
      final String stagedName = libName.substring(0, libName.length() - 3) + '-' + digestOf(lib) + ".so";
      final String dirName = "sirix-native-" + System.getProperty("user.name", "default");
      // More than one candidate because falling back to the Java decoder costs ~1.8× on every
      // region decode, and the usual reason for landing there is mundane: a read-only or
      // noexec /tmp. Losing that much throughput to an unwritable directory, silently, is not an
      // acceptable default — so try the obvious alternatives before giving up, and say so when we
      // do (at WARN, not DEBUG: this is a performance cliff, not a detail).
      // Deliberately only these two: the temp directory, then the user's home. Writing into the
      // process's working directory would be a surprising side effect of opening a database.
      final String[] roots = {
          System.getProperty("java.io.tmpdir"),
          System.getProperty("user.home")
      };
      Exception lastFailure = null;
      for (final String root : roots) {
        if (root == null || root.isEmpty()) {
          continue;
        }
        try {
          final Path dir = Path.of(root, dirName);
          Files.createDirectories(dir);
          return publishNativeLib(dir.resolve(stagedName), lib);
        } catch (final Exception e) {
          lastFailure = e;
        }
      }
      LOGGER.warn("SirixLZ77NativeDecoder: could not stage {} into any of {} — falling back to the "
              + "pure-Java decoder, which decompresses roughly 1.8x slower. Point java.io.tmpdir at "
              + "a writable directory to recover it.",
          libName, String.join(", ", roots), lastFailure);
      return null;
    }
  }

  /**
   * Make {@code target} exist with exactly {@code lib}'s bytes, without ever writing into a file
   * another process may already have mapped.
   *
   * <p>The staging directory is shared by every JVM running as this user, and the previous version
   * of this method wrote the library straight into it with {@code Files.write}, which truncates
   * and rewrites in place. A second JVM starting while a first had the library {@code dlopen}ed
   * therefore pulled the mapped pages out from under it, and the first JVM's next downcall jumped
   * into unmapped memory — a hard {@code SIGSEGV} at a near-null pc, not a catchable exception,
   * because {@link Option#critical(boolean) critical} linkage omits the thread-state transition
   * that would let the VM report it. It reproduced whenever more than one test JVM ran at once,
   * which is the ordinary case for a parallel Gradle build.
   *
   * <p>Two things make this safe. The name is content-addressed, so an existing file of the right
   * size already holds the right bytes and is simply reused. And a file that has to be created is
   * written under a unique temporary name and moved into place with
   * {@link StandardCopyOption#ATOMIC_MOVE}: a rename swaps the directory entry, so a process
   * holding the previous inode keeps a valid mapping until it lets go. Losing the race is not an
   * error — the winner staged identical bytes, so the loser drops its copy and uses theirs.
   */
  private static Path publishNativeLib(final Path target, final byte[] lib) throws Exception {
    if (Files.isReadable(target) && Files.size(target) == lib.length) {
      return target;
    }
    final Path staging = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
    try {
      Files.write(staging, lib);
      Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
      return target;
    } catch (final Exception e) {
      Files.deleteIfExists(staging);
      // Another JVM published the same content first — its bytes are ours, by construction.
      if (Files.isReadable(target) && Files.size(target) == lib.length) {
        return target;
      }
      throw e;
    }
  }

  /** Short, stable content digest for the staged file name. Runs once per JVM. */
  private static String digestOf(final byte[] lib) throws Exception {
    final byte[] hash = MessageDigest.getInstance("SHA-256").digest(lib);
    final StringBuilder out = new StringBuilder(16);
    for (int i = 0; i < 8; i++) {
      out.append(Character.forDigit((hash[i] >> 4) & 0xF, 16));
      out.append(Character.forDigit(hash[i] & 0xF, 16));
    }
    return out.toString();
  }

}
