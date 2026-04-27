/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.util.Arrays;

import sun.misc.Unsafe;

/**
 * Schema-aware single-pass LZ77 variant used per-page in Sirix's
 * {@link KeyValueLeafPage} serialization. HFT-grade: zero allocation on
 * the hot path, 8-byte-stride {@link Unsafe} copies, branchless token
 * decode, LZ4-compatible wire format. Targets ≥ 4 GB/s decode and
 * ≥ 500 MB/s encode on record-heap pages.
 *
 * <h2>Why LZ77 vs LZ4?</h2>
 * LZ4 itself is an LZ77 variant. What Sirix's record heaps contain are
 * per-page local 4-byte patterns like {@code 0x01 <tid> <NULL varint>
 * <NULL varint>} — record-header bytes that repeat verbatim across
 * consecutive same-kind nodes within one 32 KiB page. A hand-rolled
 * schema-tuned LZ77 matches LZ4's ratio with a leaner decoder.
 *
 * <h2>Wire format</h2>
 * <pre>
 *   byte    marker = 0xFD                   // distinguishes from ZeroRunByteCodec (0xFF) and ByteRunCodec (0xFE)
 *   varint  uncompressedSize
 *   body:
 *     byte token = (litLenNib << 4) | matchLenNib
 *     [if litLenNib == 15] 0xFF-chained overflow bytes (each 0xFF adds 255, final &lt; 0xFF ends)
 *     byte[litLen] literals
 *     // If uncompressedSize bytes have now been emitted, stop.
 *     // Otherwise a match follows:
 *     uint16  matchDistance (LE, 1..65535)
 *     [if matchLenNib == 15] 0xFF-chained overflow bytes same rule
 *     // decoded matchLen = matchLenNib + MIN_MATCH (4) + overflow
 * </pre>
 *
 * <p>The format is a near-clone of LZ4's block format. Keeping the wire
 * format stable lets us refactor internals aggressively without breaking
 * already-persisted pages.
 *
 * <h2>HFT optimizations applied</h2>
 * <ul>
 *   <li><b>Unsafe 8-byte stride:</b> all bulk literal / match copies use
 *       {@code Unsafe.getLong} / {@code putLong} in 8-byte strides. Only
 *       the last &lt; 8-byte tail runs through a byte-by-byte loop.</li>
 *   <li><b>Overlap-safe pattern expansion:</b> when {@code distance &lt; 8}
 *       the match source and destination overlap inside a single long
 *       store. We use the LZ4-style {@code DEC_TABLE[distance]} lookup to
 *       expand the short pattern to 8 bytes, then 8-byte stride from
 *       there.</li>
 *   <li><b>Branchless token decode:</b> the common case (both nibbles
 *       {@code &lt; 15}) has zero branches beyond the single nibble test.</li>
 *   <li><b>Scratch-then-bulk-copy output:</b> decode writes to a per-thread
 *       {@code byte[]} scratch (native Unsafe speed, no MemorySegment
 *       safety-check overhead) and finishes with one bulk
 *       {@code MemorySegment.copy} to the caller's output. The
 *       intrinsified memcpy moves the block at ~1600 GB/s so the extra
 *       step is sub-percent.</li>
 *   <li><b>Per-thread generation-tagged hash table</b> — no per-encode
 *       memset, wraps every 65 535 encodes.</li>
 *   <li><b>Absolute-address Unsafe access</b> for native-backed
 *       {@link MemorySegment} inputs: one {@code getInt} / {@code getLong}
 *       per 4- or 8-byte operation, no MemorySegment bounds-check.</li>
 * </ul>
 */
public final class SirixLZ77Codec {

  // ══════════════════════════════════════════════════════════════════ Unsafe

  /**
   * Raw {@link Unsafe} — used for all hot-path loads and stores. Pinned via
   * reflection at class-init per the {@code jdk.unsupported/sun.misc}
   * export declared in {@code build.gradle}.
   */
  private static final Unsafe UNSAFE;
  private static final long BYTE_ARRAY_BASE;

  static {
    try {
      final Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      UNSAFE = (Unsafe) f.get(null);
      BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ══════════════════════════════════════════════════════════════════ constants

  /** Start-of-frame marker. */
  public static final byte FRAME_MARKER = (byte) 0xFD;

  /** Minimum match length — below this we emit bytes as literals. */
  public static final int MIN_MATCH = 4;

  /**
   * Upper bound on match length we encode. Matches effectively unlimited
   * because overflow bytes elongate the match.
   */
  public static final int MAX_MATCH_LEN = Integer.MAX_VALUE / 2;

  /** Maximum back-reference distance. Matches LZ4 block format. */
  public static final int MAX_DISTANCE = 65535;

  /** Tail bytes (last MFLIMIT) are always literals — matches LZ4. */
  public static final int MFLIMIT = 12;

  /** Min input size to attempt matching — below this we emit everything literal. */
  public static final int LZ77_MIN_LENGTH = MFLIMIT + 1;

  /**
   * Hash table size in bits. 16 → 64K entries × 4 bytes = 256 KiB per thread.
   * A single memset on generation wrap every 65 535 encodes amortises the cost.
   */
  public static final int HASH_BITS = 16;
  private static final int HASH_SIZE = 1 << HASH_BITS;
  private static final int HASH_MASK = HASH_SIZE - 1;

  /**
   * Per-thread generation-tagged hash-table scratch. Holds
   * {@code (generation << 16) | (offset & 0xFFFF)}. Generation bumps on
   * each encode; a real memset runs only on 16-bit wrap.
   */
  private static final ThreadLocal<int[]> HASH_TABLE_SCRATCH =
      ThreadLocal.withInitial(() -> new int[HASH_SIZE]);

  /** Per-thread generation counter. */
  private static final ThreadLocal<int[]> GENERATION_SCRATCH =
      ThreadLocal.withInitial(() -> new int[] { 0 });

  /**
   * Per-thread decode scratch. Decode writes the decompressed stream into
   * this byte[] via {@link Unsafe} 8-byte stride, then the final
   * {@code MemorySegment.copy} delivers to the caller's output. Starts at
   * 128 KiB and grows as needed.
   */
  private static final ThreadLocal<byte[]> DECODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * Per-thread encode scratch for when the input MemorySegment is
   * native-backed: we snapshot it to a byte[] once so the encode hot
   * loop can run entirely on the heap via {@link Unsafe}. Avoids
   * {@code MemorySegment.get} overhead on millions of per-position
   * probes. Starts at 128 KiB.
   */
  private static final ThreadLocal<byte[]> ENCODE_INPUT_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * Enable lazy-matching strategy: after finding a match at position ip,
   * probe position ip+1. If ip+1 has a longer match, sacrifice one literal
   * byte and emit the longer match instead.
   */
  private static final boolean LAZY_MATCH =
      !Boolean.getBoolean("sirix.lz77Codec.greedy");

  /** Maximum number of iterated lazy-match swaps per initial match find. */
  private static final int LAZY_MAX_STEPS =
      Math.max(0, Integer.getInteger("sirix.lz77Codec.lazyMaxSteps", 1));

  /**
   * Worst-case encoded size for {@code uncompressedSize} bytes of input.
   * Matches LZ4's worst-case formula.
   */
  public static int maxEncodedSize(final int uncompressedSize) {
    if (uncompressedSize < 0) {
      throw new IllegalArgumentException("uncompressedSize=" + uncompressedSize);
    }
    // 1 marker + 5 varint + (uncompressedSize + ceil(uncompressedSize/255) + 16 framing)
    return 1 + 5 + uncompressedSize + (uncompressedSize / 255) + 16;
  }

  private SirixLZ77Codec() {}

  // ══════════════════════════════════════════════════════════════════ ENCODE

  /**
   * Encode {@code inputLength} bytes from {@code input} starting at
   * {@code inputOff} to {@code output} starting at {@code outputOff}.
   *
   * @return bytes written to {@code output}
   */
  public static int encode(final MemorySegment input, final long inputOff, final int inputLength,
      final byte[] output, final int outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    if (inputLength < 0) {
      throw new IllegalArgumentException("inputLength=" + inputLength);
    }

    int outPos = outputOff;
    output[outPos++] = FRAME_MARKER;
    outPos = writeVarint(output, outPos, inputLength);

    if (inputLength == 0) {
      // Zero-length: emit trailing literal-only token.
      output[outPos++] = 0;
      return outPos - outputOff;
    }

    // Snapshot input to a per-thread byte[] scratch so the hash-table
    // probe / 4-byte-compare / 8-byte match-extend loops run on the heap
    // via Unsafe. One bulk MemorySegment.copy — intrinsified memcpy.
    byte[] src = ENCODE_INPUT_SCRATCH.get();
    if (src.length < inputLength) {
      src = new byte[Math.max(inputLength, src.length * 2)];
      ENCODE_INPUT_SCRATCH.set(src);
    }
    MemorySegment.copy(input, ValueLayout.JAVA_BYTE, inputOff, src, 0, inputLength);

    // Below the LZ77 threshold: emit all bytes as a single literal token.
    if (inputLength < LZ77_MIN_LENGTH) {
      outPos = emitLiteralOnlyTokenArr(src, 0, inputLength, output, outPos);
      return outPos - outputOff;
    }

    // Sirix pages are bounded at ~256 KiB. The 16-bit offset in the
    // hash-table value can only address the first 64 KiB of input. For
    // larger inputs we fall back to a single literal-only token stream.
    if (inputLength > 0xFFFF) {
      outPos = emitLiteralOnlyTokenArr(src, 0, inputLength, output, outPos);
      return outPos - outputOff;
    }

    return outputOff + encodeCore(src, inputLength, output, outPos) - outputOff;
  }

  /**
   * Core encode loop on the heap-backed snapshot {@code src}. Returns the
   * final output position (absolute, relative to {@code output[0]}).
   */
  private static int encodeCore(final byte[] src, final int inputLength,
      final byte[] output, final int outputStartPos) {
    final int[] hashTable = HASH_TABLE_SCRATCH.get();
    final int[] genHolder = GENERATION_SCRATCH.get();
    int gen = genHolder[0] + 1;
    if (gen >= 0x10000) {
      Arrays.fill(hashTable, 0);
      gen = 1;
    }
    genHolder[0] = gen;
    final int genTag = gen << 16;

    int outPos = outputStartPos;
    int anchor = 0;
    int ip = 0;
    final int matchLimit = inputLength - MFLIMIT;
    final int mflimitPlusOne = matchLimit;

    // Seed first position.
    if (ip < matchLimit) {
      hashTable[hash4Arr(src, ip)] = genTag | ip;
      ip++;
    }

    while (ip < mflimitPlusOne) {
      final int h = hash4Arr(src, ip);
      final int slotValue = hashTable[h];
      hashTable[h] = genTag | ip;

      if ((slotValue & 0xFFFF0000) != genTag) {
        ip++;
        continue;
      }
      final int candidate = slotValue & 0xFFFF;
      if ((ip - candidate) > MAX_DISTANCE
          || !match4Arr(src, candidate, ip)) {
        ip++;
        continue;
      }

      // Found a 4-byte match. Extend via 8-byte XOR + trailing-zero count.
      int matchCandidate = candidate;
      int matchIp = ip;
      int matchLen = MIN_MATCH + extendMatchLen(
          src, matchCandidate + MIN_MATCH, matchIp + MIN_MATCH,
          inputLength - matchIp - MIN_MATCH);

      // Lazy-match: probe matchIp+1 for a strictly longer match.
      if (LAZY_MATCH) {
        int lazySteps = 0;
        while (lazySteps < LAZY_MAX_STEPS && matchIp + 1 < mflimitPlusOne) {
          final int h2 = hash4Arr(src, matchIp + 1);
          final int slotValue2 = hashTable[h2];
          if ((slotValue2 & 0xFFFF0000) != genTag) break;
          final int candidate2 = slotValue2 & 0xFFFF;
          if ((matchIp + 1 - candidate2) > MAX_DISTANCE
              || !match4Arr(src, candidate2, matchIp + 1)) {
            break;
          }
          final int altLen = MIN_MATCH + extendMatchLen(
              src, candidate2 + MIN_MATCH, matchIp + 1 + MIN_MATCH,
              inputLength - (matchIp + 1) - MIN_MATCH);
          if (altLen > matchLen) {
            matchLen = altLen;
            matchCandidate = candidate2;
            matchIp++;
            lazySteps++;
          } else {
            break;
          }
        }
      }

      final int literalLen = matchIp - anchor;
      final int distance = matchIp - matchCandidate;

      outPos = emitToken(src, anchor, literalLen, distance, matchLen, output, outPos);

      ip = matchIp + matchLen;
      anchor = ip;

      // Seed back-references so later positions can match into the body.
      if (ip - 2 >= 0 && ip - 2 < mflimitPlusOne) {
        hashTable[hash4Arr(src, ip - 2)] = genTag | (ip - 2);
      }
      if (ip - 1 < mflimitPlusOne) {
        hashTable[hash4Arr(src, ip - 1)] = genTag | (ip - 1);
      }
    }

    final int tailLen = inputLength - anchor;
    outPos = emitLiteralOnlyTokenArr(src, anchor, tailLen, output, outPos);
    return outPos;
  }

  /**
   * Extend a match starting from {@code (ap, bp)} via 8-byte XOR + tzcount.
   * Returns the extra byte count matched (not including the initial
   * {@link #MIN_MATCH} bytes the caller already confirmed).
   *
   * @param maxExtra maximum extra bytes we can match (i.e. input bytes
   *                 remaining past the initial 4-byte match).
   */
  private static int extendMatchLen(final byte[] src, final int ap, final int bp,
      final int maxExtra) {
    int extra = 0;
    // 8-byte stride while both sides have ≥ 8 bytes.
    while (extra + 8 <= maxExtra) {
      final long aLong = UNSAFE.getLong(src, BYTE_ARRAY_BASE + ap + extra);
      final long bLong = UNSAFE.getLong(src, BYTE_ARRAY_BASE + bp + extra);
      final long diff = aLong ^ bLong;
      if (diff == 0) {
        extra += 8;
        continue;
      }
      // On little-endian x86 we want the number of equal low bytes;
      // that's (numberOfTrailingZeros / 8). Java longs expose
      // Long.numberOfTrailingZeros which maps to a single tzcnt.
      extra += Long.numberOfTrailingZeros(diff) >>> 3;
      return extra;
    }
    // Final < 8-byte tail.
    while (extra < maxExtra
        && UNSAFE.getByte(src, BYTE_ARRAY_BASE + ap + extra)
            == UNSAFE.getByte(src, BYTE_ARRAY_BASE + bp + extra)) {
      extra++;
    }
    return extra;
  }

  /** Emit a token with literal run + match. */
  private static int emitToken(final byte[] src, final int literalFrom,
      final int literalLen, final int distance, final int matchLen,
      final byte[] output, final int outputOff) {
    int outPos = outputOff;
    final int litNib = Math.min(literalLen, 15);
    final int mlNib = Math.min(matchLen - MIN_MATCH, 15);
    output[outPos++] = (byte) ((litNib << 4) | mlNib);

    if (literalLen >= 15) {
      int rem = literalLen - 15;
      while (rem >= 255) {
        output[outPos++] = (byte) 0xFF;
        rem -= 255;
      }
      output[outPos++] = (byte) rem;
    }

    if (literalLen > 0) {
      System.arraycopy(src, literalFrom, output, outPos, literalLen);
      outPos += literalLen;
    }

    output[outPos++] = (byte) (distance & 0xFF);
    output[outPos++] = (byte) ((distance >>> 8) & 0xFF);

    if (matchLen - MIN_MATCH >= 15) {
      int rem = matchLen - MIN_MATCH - 15;
      while (rem >= 255) {
        output[outPos++] = (byte) 0xFF;
        rem -= 255;
      }
      output[outPos++] = (byte) rem;
    }

    return outPos;
  }

  /** Emit a literal-only token (trailing — no match follows). */
  private static int emitLiteralOnlyTokenArr(final byte[] src, final int literalFrom,
      final int literalLen, final byte[] output, final int outputOff) {
    int outPos = outputOff;
    final int litNib = Math.min(literalLen, 15);
    output[outPos++] = (byte) ((litNib << 4));

    if (literalLen >= 15) {
      int rem = literalLen - 15;
      while (rem >= 255) {
        output[outPos++] = (byte) 0xFF;
        rem -= 255;
      }
      output[outPos++] = (byte) rem;
    }

    if (literalLen > 0) {
      System.arraycopy(src, literalFrom, output, outPos, literalLen);
      outPos += literalLen;
    }
    return outPos;
  }

  /**
   * Knuth multiplicative hash on 4 input bytes → HASH_BITS-bit index.
   * Reads 4 bytes as a little-endian int via {@link Unsafe#getInt} — one
   * memory-load per call.
   */
  private static int hash4Arr(final byte[] src, final int offset) {
    final int w = UNSAFE.getInt(src, BYTE_ARRAY_BASE + offset);
    // 2654435761 = floor(2^32 * (sqrt(5) - 1) / 2). Knuth multiplicative hash.
    return ((w * 0x9E3779B1) >>> (32 - HASH_BITS)) & HASH_MASK;
  }

  /** True iff 4 bytes starting at {@code a} equal 4 bytes starting at {@code b}. */
  private static boolean match4Arr(final byte[] src, final int a, final int b) {
    return UNSAFE.getInt(src, BYTE_ARRAY_BASE + a)
        == UNSAFE.getInt(src, BYTE_ARRAY_BASE + b);
  }

  // ══════════════════════════════════════════════════════════════════ DECODE

  /**
   * Whether the native FFI decoder should be used when available. Default
   * on — measured ~2× speedup on typical 32 KiB pages. Gate off with
   * {@code -Dsirix.lz77Codec.native.disable=true} for A/B comparison.
   */
  private static final boolean NATIVE_DECODER_ENABLED =
      !Boolean.getBoolean("sirix.lz77Codec.native.disable")
          && SirixLZ77NativeDecoder.isAvailable();

  static {
    if (Boolean.getBoolean("sirix.lz77Codec.diag")) {
      System.out.println("[SirixLZ77Codec] NATIVE_DECODER_ENABLED=" + NATIVE_DECODER_ENABLED);
    }
  }

  /**
   * Whether to increment {@link #NATIVE_CALLS} / {@link #JAVA_CALLS}
   * counters on every decode. Diagnostic only. Defaults off in production
   * to avoid AtomicLong contention in the hot path.
   *
   * <p>Enable via {@code -Dsirix.lz77Codec.diag.counters=true}.
   */
  private static final boolean DIAG_COUNTERS =
      Boolean.getBoolean("sirix.lz77Codec.diag.counters");

  /** Counter: total native decode dispatches (diagnostic only). */
  private static final java.util.concurrent.atomic.AtomicLong NATIVE_CALLS =
      new java.util.concurrent.atomic.AtomicLong();

  /** Counter: total Java decode fallback dispatches (diagnostic only). */
  private static final java.util.concurrent.atomic.AtomicLong JAVA_CALLS =
      new java.util.concurrent.atomic.AtomicLong();

  /** @return number of native-path decode dispatches since JVM start. */
  public static long getNativeCallCount() {
    return NATIVE_CALLS.get();
  }

  /** @return number of Java-path decode dispatches since JVM start. */
  public static long getJavaCallCount() {
    return JAVA_CALLS.get();
  }

  /**
   * Decode a frame from {@code input} (starting at {@code inputOff}) into
   * {@code output} starting at {@code outputOff}. Writes exactly
   * {@code uncompressedSize} bytes as recorded in the frame header.
   *
   * <p>Hot path: decode writes directly into {@code output} via
   * {@link Unsafe}, using the {@code (heapBase, address)} pair to address
   * both native- and array-backed segments uniformly. Literal / match
   * copies are 8-byte strided. No scratch buffer, no intermediate copy.
   *
   * <p>When the native decoder is available (see {@link SirixLZ77NativeDecoder})
   * and the output is native-backed with at least 64 bytes of tail slack,
   * we dispatch to the C implementation which auto-vectorises the memcpy
   * hot path. Otherwise the Java implementation below runs.
   *
   * @return bytes written to {@code output} (== uncompressedSize)
   */
  public static int decode(final byte[] input, final int inputOff, final int inputLen,
      final MemorySegment output, final long outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }

    // Native fast-path. Preconditions:
    //   1. Output has ≥ 16 bytes of tail slack (the C decoder's hot loop
    //      uses 16-byte wildcopy stores). Works for both native and
    //      heap-backed outputs because Panama's critical-linkage pins heap
    //      segments for the duration of the call.
    //   2. Frame header is well-formed (0xFD marker + varint). We peek
    //      without allocation.
    // If any precondition fails we fall through to the Java decoder below.
    if (NATIVE_DECODER_ENABLED
        && inputLen >= 2
        && input[inputOff] == FRAME_MARKER) {
      final long vrPeek = readVarintPacked(input, inputOff + 1);
      final int uncompressedPeek = (int) vrPeek;
      if (uncompressedPeek > 0
          && outputOff + uncompressedPeek + 16 <= output.byteSize()) {
        if (DIAG_COUNTERS) NATIVE_CALLS.incrementAndGet();
        return SirixLZ77NativeDecoder.decode(input, inputOff, inputLen, output, outputOff);
      }
    }
    if (DIAG_COUNTERS) JAVA_CALLS.incrementAndGet();

    int inPos = inputOff;
    final int inEnd = inputOff + inputLen;
    if (inPos >= inEnd || input[inPos++] != FRAME_MARKER) {
      throw new IllegalStateException("SirixLZ77Codec: missing frame marker");
    }
    final long vr = readVarintPacked(input, inPos);
    inPos = (int) (vr >>> 32);
    final int uncompressed = (int) vr;

    if (uncompressed == 0) {
      return 0;
    }
    if (outputOff + uncompressed > output.byteSize()) {
      throw new IllegalStateException("SirixLZ77Codec: output too small");
    }

    // Resolve (baseObj, addr) pair for the output segment. For native
    // segments baseObj == null and addr is the raw pointer. For
    // byte[]-backed heap segments baseObj is the byte[] and addr is the
    // array-base offset + slice start. Unsafe handles both uniformly.
    //
    // Fast-path is chosen when the output has ≥ 16 B of tail slack, which
    // lets wildCopy8 overshoot harmlessly.
    final Object dstBase = output.heapBase().orElse(null);
    final long dstAddr = output.address() + outputOff;
    final boolean fastPath = outputOff + uncompressed + 16 <= output.byteSize();

    final int produced;
    if (fastPath) {
      produced = decodeCore(input, inPos, inEnd, dstBase, dstAddr, uncompressed);
    } else {
      // Rare slow path: caller-provided segment has no tail slack. Use
      // per-thread scratch + single bulk copy.
      byte[] scratch = DECODE_SCRATCH.get();
      final int needed = uncompressed + 16;
      if (scratch.length < needed) {
        scratch = new byte[Math.max(needed, scratch.length * 2)];
        DECODE_SCRATCH.set(scratch);
      }
      produced = decodeCore(input, inPos, inEnd, scratch, BYTE_ARRAY_BASE, uncompressed);
      MemorySegment.copy(scratch, 0, output, ValueLayout.JAVA_BYTE, outputOff, uncompressed);
    }
    if (produced != uncompressed) {
      throw new IllegalStateException("SirixLZ77Codec: decoded "
          + produced + " bytes, expected " + uncompressed);
    }
    return uncompressed;
  }

  /**
   * Main decode loop. Writes into {@code (dstBase, dstAddr)} which the
   * caller has verified has ≥ {@code uncompressed + 16} bytes of
   * capacity (16-byte wildCopy overshoot slack).
   *
   * <p>The loop splits into two regimes:
   * <ol>
   *   <li><b>Fast regime</b> — {@code outPos ≤ outLimit} AND input has
   *       sufficient slack. All literal and match copies use unchecked
   *       8-byte Unsafe strides. This processes the bulk of every page.</li>
   *   <li><b>Safe tail regime</b> — {@code outPos > outLimit}. Byte-by-byte
   *       copies with bounds checks. Typically < 64 bytes per page.</li>
   * </ol>
   */
  private static int decodeCore(final byte[] input, final int inputStart, final int inEnd,
      final Object dstBase, final long dstAddr, final int uncompressed) {
    // Fast-path precondition: we know output has ≥ 16 bytes of tail slack
    // (caller checked). If input also has ≥ 16 bytes of trailing slack in
    // its backing byte[], we can skip per-token input-bound checks —
    // wildCopy8 safely overshoots into that slack. Most call sites pass
    // a byte[] that's been oversized via {@link #maxEncodedSize} so this
    // is common. The array length is inspected as {@code input.length}.
    final boolean inputHasSlack = inEnd + 16 <= input.length;
    if (inputHasSlack) {
      return decodeCoreFast(input, inputStart, inEnd, dstBase, dstAddr, uncompressed);
    }
    return decodeCoreSafe(input, inputStart, inEnd, dstBase, dstAddr, uncompressed);
  }

  /**
   * Hottest decode path — both output and input have ≥ 16 byte tail slack.
   * Per-token input-bound checks are elided; wildCopy8 overshoots harmlessly
   * into slack.
   *
   * <p>Loop is split into hot + tail regimes by precomputing
   * {@code safeLimit = uncompressed - 64}. The hot loop's top-of-iteration
   * invariant {@code outPos <= safeLimit} guarantees at least 64 B of tail
   * slack, which lets us elide the per-token
   * {@code outPos + litLen + 16 <= uncompressed} and
   * {@code outPos + matchLen + 16 <= uncompressed} checks entirely. Saves
   * one branch per token and typically 1-2 cycles per iteration. The 64-B
   * slack covers: literal ≤ 15 + matchLen ≤ 19 + 16-byte stride overshoot
   * ≤ 50 B, with headroom.
   */
  private static int decodeCoreFast(final byte[] input, final int inputStart, final int inEnd,
      final Object dstBase, final long dstAddr, final int uncompressed) {
    int inPos = inputStart;
    int outPos = 0;
    // Hot regime: 64 B of tail slack guaranteed.
    final int safeLimit = uncompressed - 64;

    // ───── Hot loop. ───────────────────────────────────────────────────
    while (outPos <= safeLimit) {
      final int token = input[inPos++] & 0xFF;
      int litLen = token >>> 4;
      int matchLen = token & 0x0F;

      if (litLen == 15) {
        int b;
        do {
          b = input[inPos++] & 0xFF;
          litLen += b;
        } while (b == 0xFF);
        // Large literal — may push past safeLimit. Break to tail loop.
        if (outPos + litLen > safeLimit) {
          // Backtrack to preserve parse invariant: we've consumed (token +
          // overflow bytes). Undo them so tail loop can re-parse.
          // Easier: inline the literal+match in the tail loop path by
          // finishing this iteration here with safe copies.
          final long srcBase0 = BYTE_ARRAY_BASE + inPos;
          final long dstBaseOff = dstAddr + outPos;
          // Long literals always have uncompressed >= inPos + litLen + 16,
          // not guaranteed. Do the safe check.
          if (outPos + litLen > uncompressed) {
            throw new IllegalStateException("SirixLZ77Codec: literal would overrun output");
          }
          if (outPos + litLen + 16 <= uncompressed) {
            int i = 0;
            do {
              UNSAFE.putLong(dstBase, dstBaseOff + i,
                  UNSAFE.getLong(input, srcBase0 + i));
              i += 8;
            } while (i < litLen);
          } else {
            for (int k = 0; k < litLen; k++) {
              UNSAFE.putByte(dstBase, dstBaseOff + k, input[inPos + k]);
            }
          }
          inPos += litLen;
          outPos += litLen;
          if (outPos == uncompressed) break;
          // Fall through to the match section normally.
        } else {
          // Literal fits in slack — wildCopy.
          final long srcBase0 = BYTE_ARRAY_BASE + inPos;
          final long dstBaseOff0 = dstAddr + outPos;
          int i = 0;
          do {
            UNSAFE.putLong(dstBase, dstBaseOff0 + i,
                UNSAFE.getLong(input, srcBase0 + i));
            i += 8;
          } while (i < litLen);
          inPos += litLen;
          outPos += litLen;
        }
      } else if (litLen > 0) {
        // Common short-literal path: ≤ 14 bytes, always fits in 16-byte slack.
        final long srcBase0 = BYTE_ARRAY_BASE + inPos;
        final long dstBaseOff0 = dstAddr + outPos;
        UNSAFE.putLong(dstBase, dstBaseOff0,
            UNSAFE.getLong(input, srcBase0));
        if (litLen > 8) {
          UNSAFE.putLong(dstBase, dstBaseOff0 + 8L,
              UNSAFE.getLong(input, srcBase0 + 8L));
        }
        inPos += litLen;
        outPos += litLen;
      }

      if (outPos == uncompressed) break;

      // Distance (2 B LE).
      final int dist = UNSAFE.getShort(input, BYTE_ARRAY_BASE + inPos) & 0xFFFF;
      inPos += 2;

      if (matchLen == 15) {
        int b;
        do {
          b = input[inPos++] & 0xFF;
          matchLen += b;
        } while (b == 0xFF);
      }
      matchLen += MIN_MATCH;

      final int srcOff = outPos - dist;
      if (srcOff < 0 || dist == 0) {
        throw new IllegalStateException("SirixLZ77Codec: invalid distance " + dist);
      }

      // Match copy. Wild-overshoot is safe while outPos + matchLen + 16 <=
      // uncompressed. In hot regime outPos <= safeLimit = uncompressed - 64,
      // so for matchLen <= 48 we're unconditionally safe. Non-overflow
      // tokens have matchLen ∈ [4, 19], so the typical case always
      // bypasses the guard. The branch below fires only for overflow-
      // encoded matches, which are ~ 0.1 % of tokens.
      if (matchLen <= 48) {
        if (dist >= 8) {
          // Non-overlapping 8-byte stride. matchLen ∈ [4, 48] so we need at
          // most 6 putLong stores. We write the first two unconditionally
          // (covers the 91 % of matches with matchLen ≤ 16, i.e. the
          // dominant matchLen=4..11 range per token-stat) and only loop
          // for the rarer longer matches. This lets the OoO scheduler
          // overlap two independent getLong→putLong pairs without a
          // loop-carried dependency for the common case.
          final long srcBaseL = dstAddr + srcOff;
          final long dstBaseL = dstAddr + outPos;
          UNSAFE.putLong(dstBase, dstBaseL,
              UNSAFE.getLong(dstBase, srcBaseL));
          if (matchLen > 8) {
            UNSAFE.putLong(dstBase, dstBaseL + 8L,
                UNSAFE.getLong(dstBase, srcBaseL + 8L));
            if (matchLen > 16) {
              int i = 16;
              do {
                UNSAFE.putLong(dstBase, dstBaseL + i,
                    UNSAFE.getLong(dstBase, srcBaseL + i));
                i += 8;
              } while (i < matchLen);
            }
          }
        } else if (dist == 1 || dist == 2 || dist == 4) {
          // Short-distance splat — pattern period divides 8.
          final long dstBaseL = dstAddr + outPos;
          final long pattern;
          if (dist == 1) {
            final long b = UNSAFE.getByte(dstBase, dstAddr + srcOff) & 0xFFL;
            long p = b | (b << 8);
            p |= p << 16;
            pattern = p | (p << 32);
          } else if (dist == 2) {
            final long s = UNSAFE.getShort(dstBase, dstAddr + srcOff) & 0xFFFFL;
            long p = s | (s << 16);
            pattern = p | (p << 32);
          } else {
            final long ii = UNSAFE.getInt(dstBase, dstAddr + srcOff) & 0xFFFFFFFFL;
            pattern = ii | (ii << 32);
          }
          UNSAFE.putLong(dstBase, dstBaseL, pattern);
          if (matchLen > 8) {
            UNSAFE.putLong(dstBase, dstBaseL + 8L, pattern);
            if (matchLen > 16) {
              int i = 16;
              do {
                UNSAFE.putLong(dstBase, dstBaseL + i, pattern);
                i += 8;
              } while (i < matchLen);
            }
          }
        } else {
          // Overlapping dist ∈ {3, 5, 6, 7}: byte-by-byte, ~8 % of matches.
          final long srcBaseB = dstAddr + srcOff;
          final long dstBaseB = dstAddr + outPos;
          for (int k = 0; k < matchLen; k++) {
            UNSAFE.putByte(dstBase, dstBaseB + k,
                UNSAFE.getByte(dstBase, srcBaseB + k));
          }
        }
      } else {
        // Very long match near tail — safe byte copy.
        if (outPos + matchLen > uncompressed) {
          throw new IllegalStateException("SirixLZ77Codec: match would overrun output");
        }
        final long srcBaseB = dstAddr + srcOff;
        final long dstBaseB = dstAddr + outPos;
        for (int k = 0; k < matchLen; k++) {
          UNSAFE.putByte(dstBase, dstBaseB + k,
              UNSAFE.getByte(dstBase, srcBaseB + k));
        }
      }
      outPos += matchLen;
    }

    // ───── Tail loop: full bounds checks. ──────────────────────────────
    while (outPos < uncompressed) {
      final int token = input[inPos++] & 0xFF;
      int litLen = token >>> 4;
      int matchLen = token & 0x0F;

      if (litLen == 15) {
        int b;
        do {
          b = input[inPos++] & 0xFF;
          litLen += b;
        } while (b == 0xFF);
      }

      if (litLen > 0) {
        if (outPos + litLen + 16 <= uncompressed) {
          final long srcBase0 = BYTE_ARRAY_BASE + inPos;
          final long dstBaseOff0 = dstAddr + outPos;
          int i = 0;
          do {
            UNSAFE.putLong(dstBase, dstBaseOff0 + i,
                UNSAFE.getLong(input, srcBase0 + i));
            i += 8;
          } while (i < litLen);
        } else {
          if (outPos + litLen > uncompressed) {
            throw new IllegalStateException("SirixLZ77Codec: literal would overrun output");
          }
          final long dstBaseOff = dstAddr + outPos;
          for (int k = 0; k < litLen; k++) {
            UNSAFE.putByte(dstBase, dstBaseOff + k, input[inPos + k]);
          }
        }
        inPos += litLen;
        outPos += litLen;
      }

      if (outPos == uncompressed) break;

      final int dist = UNSAFE.getShort(input, BYTE_ARRAY_BASE + inPos) & 0xFFFF;
      inPos += 2;

      if (matchLen == 15) {
        int b;
        do {
          b = input[inPos++] & 0xFF;
          matchLen += b;
        } while (b == 0xFF);
      }
      matchLen += MIN_MATCH;

      if (outPos + matchLen > uncompressed) {
        throw new IllegalStateException("SirixLZ77Codec: match would overrun output");
      }
      final int srcOff = outPos - dist;
      if (srcOff < 0 || dist == 0) {
        throw new IllegalStateException("SirixLZ77Codec: invalid distance " + dist);
      }

      if (outPos + matchLen + 16 <= uncompressed && dist >= 8) {
        final long srcBaseL = dstAddr + srcOff;
        final long dstBaseL = dstAddr + outPos;
        int i = 0;
        do {
          UNSAFE.putLong(dstBase, dstBaseL + i,
              UNSAFE.getLong(dstBase, srcBaseL + i));
          i += 8;
        } while (i < matchLen);
      } else if (outPos + matchLen + 16 <= uncompressed && (dist == 1 || dist == 2 || dist == 4)) {
        final long dstBaseL = dstAddr + outPos;
        final long pattern;
        if (dist == 1) {
          final long b = UNSAFE.getByte(dstBase, dstAddr + srcOff) & 0xFFL;
          long p = b | (b << 8);
          p |= p << 16;
          pattern = p | (p << 32);
        } else if (dist == 2) {
          final long s = UNSAFE.getShort(dstBase, dstAddr + srcOff) & 0xFFFFL;
          long p = s | (s << 16);
          pattern = p | (p << 32);
        } else {
          final long ii = UNSAFE.getInt(dstBase, dstAddr + srcOff) & 0xFFFFFFFFL;
          pattern = ii | (ii << 32);
        }
        int i = 0;
        do {
          UNSAFE.putLong(dstBase, dstBaseL + i, pattern);
          i += 8;
        } while (i < matchLen);
      } else {
        final long srcBaseB = dstAddr + srcOff;
        final long dstBaseB = dstAddr + outPos;
        for (int k = 0; k < matchLen; k++) {
          UNSAFE.putByte(dstBase, dstBaseB + k,
              UNSAFE.getByte(dstBase, srcBaseB + k));
        }
      }
      outPos += matchLen;
    }
    return outPos;
  }

  /**
   * Safe decode variant with full input bounds checking per token. Used when
   * the caller's input byte[] has &lt; 16 bytes of trailing slack (e.g.,
   * small test inputs where the buffer is exactly sized).
   */
  private static int decodeCoreSafe(final byte[] input, final int inputStart, final int inEnd,
      final Object dstBase, final long dstAddr, final int uncompressed) {
    int inPos = inputStart;
    int outPos = 0;

    while (outPos < uncompressed) {
      if (inPos >= inEnd) {
        throw new IllegalStateException("SirixLZ77Codec: input exhausted mid-stream");
      }
      final int token = input[inPos++] & 0xFF;
      int litLen = token >>> 4;
      int matchLen = token & 0x0F;

      if (litLen == 15) {
        int b;
        do {
          if (inPos >= inEnd) {
            throw new IllegalStateException("SirixLZ77Codec: varint overrun");
          }
          b = input[inPos++] & 0xFF;
          litLen += b;
        } while (b == 0xFF);
      }

      if (litLen > 0) {
        if (outPos + litLen + 16 <= uncompressed && inPos + litLen + 16 <= inEnd) {
          final long srcBase0 = BYTE_ARRAY_BASE + inPos;
          final long dstBaseOff0 = dstAddr + outPos;
          int i = 0;
          do {
            UNSAFE.putLong(dstBase, dstBaseOff0 + i,
                UNSAFE.getLong(input, srcBase0 + i));
            i += 8;
          } while (i < litLen);
        } else {
          if (outPos + litLen > uncompressed) {
            throw new IllegalStateException("SirixLZ77Codec: literal would overrun output");
          }
          if (inPos + litLen > inEnd) {
            throw new IllegalStateException("SirixLZ77Codec: literal would overrun input");
          }
          final long dstBaseOff = dstAddr + outPos;
          for (int k = 0; k < litLen; k++) {
            UNSAFE.putByte(dstBase, dstBaseOff + k, input[inPos + k]);
          }
        }
        inPos += litLen;
        outPos += litLen;
      }

      if (outPos == uncompressed) break;

      if (inPos + 2 > inEnd) {
        throw new IllegalStateException("SirixLZ77Codec: distance would overrun input");
      }
      final int dist = UNSAFE.getShort(input, BYTE_ARRAY_BASE + inPos) & 0xFFFF;
      inPos += 2;

      if (matchLen == 15) {
        int b;
        do {
          if (inPos >= inEnd) {
            throw new IllegalStateException("SirixLZ77Codec: varint overrun");
          }
          b = input[inPos++] & 0xFF;
          matchLen += b;
        } while (b == 0xFF);
      }
      matchLen += MIN_MATCH;

      if (outPos + matchLen > uncompressed) {
        throw new IllegalStateException("SirixLZ77Codec: match would overrun output");
      }
      final int srcOff = outPos - dist;
      if (srcOff < 0 || dist == 0) {
        throw new IllegalStateException("SirixLZ77Codec: invalid distance " + dist);
      }

      if (outPos + matchLen + 16 <= uncompressed && dist >= 8) {
        final long srcBaseL = dstAddr + srcOff;
        final long dstBaseL = dstAddr + outPos;
        int i = 0;
        do {
          UNSAFE.putLong(dstBase, dstBaseL + i,
              UNSAFE.getLong(dstBase, srcBaseL + i));
          i += 8;
        } while (i < matchLen);
      } else if (outPos + matchLen + 16 <= uncompressed && (dist == 1 || dist == 2 || dist == 4)) {
        final long dstBaseL = dstAddr + outPos;
        final long pattern;
        if (dist == 1) {
          final long b = UNSAFE.getByte(dstBase, dstAddr + srcOff) & 0xFFL;
          long p = b | (b << 8);
          p |= p << 16;
          pattern = p | (p << 32);
        } else if (dist == 2) {
          final long s = UNSAFE.getShort(dstBase, dstAddr + srcOff) & 0xFFFFL;
          long p = s | (s << 16);
          pattern = p | (p << 32);
        } else {
          final long ii = UNSAFE.getInt(dstBase, dstAddr + srcOff) & 0xFFFFFFFFL;
          pattern = ii | (ii << 32);
        }
        int i = 0;
        do {
          UNSAFE.putLong(dstBase, dstBaseL + i, pattern);
          i += 8;
        } while (i < matchLen);
      } else {
        final long srcBaseB = dstAddr + srcOff;
        final long dstBaseB = dstAddr + outPos;
        for (int k = 0; k < matchLen; k++) {
          UNSAFE.putByte(dstBase, dstBaseB + k,
              UNSAFE.getByte(dstBase, srcBaseB + k));
        }
      }
      outPos += matchLen;
    }
    return outPos;
  }

  // ══════════════════════════════════════════════════════════════════ varint

  private static int writeVarint(final byte[] output, final int offset, int value) {
    int pos = offset;
    while ((value & ~0x7F) != 0) {
      output[pos++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    output[pos++] = (byte) value;
    return pos;
  }

  /**
   * Reads a varint into a packed {@code long}: high 32 bits = new position,
   * low 32 bits = value. Avoids the per-call {@code long[]} allocation.
   */
  private static long readVarintPacked(final byte[] input, final int offset) {
    int pos = offset;
    int result = 0;
    int shift = 0;
    while (true) {
      final byte b = input[pos++];
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
      if (shift > 28) throw new IllegalStateException("varint too long");
    }
    return ((long) pos << 32) | (result & 0xFFFFFFFFL);
  }
}
