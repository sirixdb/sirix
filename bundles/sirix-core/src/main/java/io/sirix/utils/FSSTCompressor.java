/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.utils;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Fast Static Symbol Table (FSST) compression for string values.
 * 
 * <p>
 * FSST is a lightweight compression algorithm designed for short strings that:
 * <ul>
 * <li>Builds a symbol table from sample strings to find common byte sequences</li>
 * <li>Replaces common sequences with 1-byte codes (up to 255 symbols)</li>
 * <li>Enables very fast decompression (~1-2 GB/s, vectorizable)</li>
 * <li>Works well for similar strings (e.g., JSON values from same page)</li>
 * </ul>
 * 
 * <p>
 * This implementation is optimized for zero-copy integration with MemorySegments.
 * 
 * <h2>Formal Correctness Properties</h2>
 * 
 * <pre>
 * P1: ∀ string s, table t: decode(encode(s, t), t) == s  (roundtrip correctness)
 * P2: ∀ input i: encode(i, emptyTable) == escape(i)     (graceful degradation)
 * P3: ∀ segment s: decode operates on segment without intermediate copies
 * </pre>
 * 
 * @author Johannes Lichtenberger
 */
public final class FSSTCompressor {

  private static final Logger LOGGER = LoggerFactory.getLogger(FSSTCompressor.class);

  /**
   * Maximum number of symbols in the table (codes 0-254, 255 reserved for escape).
   */
  public static final int MAX_SYMBOLS = 255;

  /**
   * Escape byte indicates the next byte is a literal (not compressed).
   */
  public static final byte ESCAPE_BYTE = (byte) 0xFF;

  /**
   * Header byte indicating data is FSST compressed.
   */
  public static final byte HEADER_COMPRESSED = (byte) 0x01;

  /**
   * Header byte indicating data is stored raw (not compressed).
   */
  public static final byte HEADER_RAW = (byte) 0x00;

  /**
   * Minimum string size to attempt compression (smaller strings unlikely to benefit).
   */
  public static final int MIN_COMPRESSION_SIZE = 32;

  /**
   * Maximum symbol length (longer patterns have diminishing returns).
   */
  public static final int MAX_SYMBOL_LENGTH = 8;

  /**
   * Minimum samples needed to build a useful symbol table. Higher values ensure we only build tables
   * when there's enough data for good patterns.
   */
  public static final int MIN_SAMPLES_FOR_TABLE = 64;

  /**
   * Minimum total bytes across all samples to attempt compression. Ensures we have enough data to
   * justify the symbol table overhead.
   */
  public static final int MIN_TOTAL_BYTES_FOR_TABLE = 4096;

  /**
   * Maximum samples to analyze (for performance).
   */
  public static final int MAX_SAMPLES_TO_ANALYZE = 128;

  /**
   * Header bytes in serialized symbol table:
   * [numSymbols(1)][symbolLengths(numSymbols)][symbolData...]
   */
  private static final int TABLE_HEADER_SIZE = 1;

  /**
   * Minimum compression ratio required to use FSST. 0.15 means we need at least 15% size reduction to
   * justify the overhead.
   */
  public static final double MIN_COMPRESSION_RATIO = 0.15;

  /**
   * Maximum size of the bounded buffer pool. Uses 2x CPU cores to handle concurrent virtual threads
   * without explosion.
   */
  private static final int BUFFER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

  /**
   * Default buffer size for encode operations (64KB covers most pages).
   */
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  /**
   * Bounded buffer pool for encode/decode operations. Loom-friendly: fixed size pool instead of
   * unbounded ThreadLocal.
   */
  private static final ArrayDeque<byte[]> BUFFER_POOL = new ArrayDeque<>(BUFFER_POOL_SIZE);

  /**
   * Reusable state for one synchronous train/trial/encode sequence.
   *
   * <p>
   * The projection segment codec builds a different table for every qualifying dictionary. A matcher
   * build needs three 256 KiB bucket work arrays, and iterative FSST training rebuilds it five times.
   * Keeping those arrays in an explicitly-owned workspace turns that multi-megabyte per-segment churn
   * into one bounded allocation per active encoder. The workspace is deliberately not thread-safe;
   * callers must give one instance to only one encode operation at a time. Published table and
   * encoded-value arrays never alias this object.
   */
  public static final class Workspace {
    private final SymbolMatcher matcher = new SymbolMatcher();
    private final CandidateWorkspace candidates = new CandidateWorkspace();
    private final int[] flatCorpusEntries = new int[MAX_SAMPLES_TO_ANALYZE];

    /** Create an owner-confined workspace. */
    public Workspace() {}

    /**
     * Drop references to the last table while retaining the primitive matcher arrays for reuse. Call
     * from an owner {@code finally} block before handing the workspace to another operation.
     */
    public void clear() {
      matcher.clear();
      candidates.clear();
    }
  }

  /**
   * Primitive, high-water-retained scratch for iterative table training.
   *
   * <p>
   * A candidate is at most eight bytes, so a packed {@code long} plus its length is the complete
   * key. The map deliberately reproduces fastutil's former {@code Object2IntOpenHashMap} layout:
   * the same default logical capacity/load factor, {@link Arrays#hashCode(byte[])} value,
   * {@link HashCommon#mix(int)} probe, descending-slot rehash, and descending-slot iteration. That
   * layout is observable because Java's stable gain sort preserves map iteration order for ties.
   * Retaining two primitive banks and two merge-sort index arrays removes the per-token key/copy and
   * per-iteration map/entry/list churn without changing a symbol code or serialized byte.
   *
   * <p>
   * The hard distinct-candidate ceiling makes adversarial giant samples fail closed to raw storage
   * instead of growing this reusable workspace until the JVM runs out of memory. The per-build
   * limit is tightened further from the exact maximum number of token and adjacent-token proposals
   * possible across the at-most {@link #MAX_SAMPLES_TO_ANALYZE} corpus entries.
   */
  private static final class CandidateWorkspace {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int INITIAL_MAP_SIZE = HashCommon.arraySize(16, LOAD_FACTOR);
    private static final int MAX_DISTINCT_CANDIDATES = 1 << 16;

    private long[] packed = new long[INITIAL_MAP_SIZE];
    private byte[] lengths = new byte[INITIAL_MAP_SIZE];
    private int[] hashes = new int[INITIAL_MAP_SIZE];
    private int[] gains = new int[INITIAL_MAP_SIZE];

    private long[] sparePacked = new long[INITIAL_MAP_SIZE];
    private byte[] spareLengths = new byte[INITIAL_MAP_SIZE];
    private int[] spareHashes = new int[INITIAL_MAP_SIZE];
    private int[] spareGains = new int[INITIAL_MAP_SIZE];

    private int[] order = new int[INITIAL_MAP_SIZE];
    private int[] sortScratch = new int[INITIAL_MAP_SIZE];
    private final int[] byteCounts = new int[256];
    private final long[] selectedPacked = new long[MAX_SYMBOLS];
    private final byte[] selectedLengths = new byte[MAX_SYMBOLS];

    private int n;
    private int mask;
    private int maxFill;
    private int size;
    private int candidateLimit;
    private int selectedCount;

    void reset(final int maximumCandidates) {
      if (maximumCandidates < 1 || maximumCandidates > MAX_DISTINCT_CANDIDATES) {
        throw new IllegalArgumentException("maximumCandidates out of range: " + maximumCandidates);
      }
      candidateLimit = maximumCandidates;
      n = INITIAL_MAP_SIZE;
      mask = n - 1;
      maxFill = HashCommon.maxFill(n, LOAD_FACTOR);
      size = 0;
      selectedCount = 0;
      Arrays.fill(lengths, 0, n, (byte) 0);
    }

    /** Drop only logical results; all retained state is primitive and owns no corpus references. */
    void clear() {
      size = 0;
      selectedCount = 0;
    }

    int[] byteCounts() {
      Arrays.fill(byteCounts, 0);
      return byteCounts;
    }

    /** Add a source slice using the former immutable ByteSequence hash and equality. */
    boolean addTo(final byte[] source, final int offset, final int length, final int delta) {
      long key = 0;
      int hash = 1;
      for (int i = 0; i < length; i++) {
        final byte value = source[offset + i];
        key |= (long) (value & 0xFF) << (i << 3);
        hash = 31 * hash + value;
      }
      return addPacked(key, length, hash, delta);
    }

    boolean putByte(final int unsignedByte, final int gain) {
      final byte value = (byte) unsignedByte;
      return addPacked(unsignedByte, 1, 31 + value, gain);
    }

    private boolean addPacked(final long key, final int length, final int hash, final int delta) {
      int pos = HashCommon.mix(hash) & mask;
      while (lengths[pos] != 0) {
        if ((lengths[pos] & 0xFF) == length && packed[pos] == key) {
          gains[pos] += delta;
          return true;
        }
        pos = (pos + 1) & mask;
      }

      if (size >= candidateLimit) {
        return false;
      }
      packed[pos] = key;
      lengths[pos] = (byte) length;
      hashes[pos] = hash;
      gains[pos] = delta;
      if (size++ >= maxFill) {
        rehash(HashCommon.arraySize(size + 1, LOAD_FACTOR));
      }
      return true;
    }

    private void rehash(final int newN) {
      ensureSpareCapacity(newN);
      Arrays.fill(spareLengths, 0, newN, (byte) 0);
      final int newMask = newN - 1;
      int remaining = size;
      int sourceSlot = n;
      while (remaining-- != 0) {
        do {
          sourceSlot--;
        } while (lengths[sourceSlot] == 0);
        int targetSlot = HashCommon.mix(hashes[sourceSlot]) & newMask;
        while (spareLengths[targetSlot] != 0) {
          targetSlot = (targetSlot + 1) & newMask;
        }
        sparePacked[targetSlot] = packed[sourceSlot];
        spareLengths[targetSlot] = lengths[sourceSlot];
        spareHashes[targetSlot] = hashes[sourceSlot];
        spareGains[targetSlot] = gains[sourceSlot];
      }

      final long[] oldPacked = packed;
      packed = sparePacked;
      sparePacked = oldPacked;
      final byte[] oldLengths = lengths;
      lengths = spareLengths;
      spareLengths = oldLengths;
      final int[] oldHashes = hashes;
      hashes = spareHashes;
      spareHashes = oldHashes;
      final int[] oldGains = gains;
      gains = spareGains;
      spareGains = oldGains;

      n = newN;
      mask = newMask;
      maxFill = HashCommon.maxFill(newN, LOAD_FACTOR);
    }

    private void ensureSpareCapacity(final int required) {
      if (sparePacked.length >= required) {
        return;
      }
      // Allocate a complete bank into locals first: an allocation failure leaves the live map and
      // the previously reusable spare bank internally consistent.
      final long[] newPacked = new long[required];
      final byte[] newLengths = new byte[required];
      final int[] newHashes = new int[required];
      final int[] newGains = new int[required];
      sparePacked = newPacked;
      spareLengths = newLengths;
      spareHashes = newHashes;
      spareGains = newGains;
    }

    /**
     * Stable gain-descending selection followed by the former stable length-descending table order.
     */
    void selectTop() {
      ensureSortCapacity(size);
      int count = 0;
      for (int slot = n; slot-- != 0;) {
        if (lengths[slot] != 0) {
          order[count++] = slot;
        }
      }
      stableSortByGain(count);

      selectedCount = 0;
      for (int i = 0; i < count && selectedCount < MAX_SYMBOLS; i++) {
        final int slot = order[i];
        final int length = lengths[slot] & 0xFF;
        final int minimumGain = length == 1
            ? 2
            : 2 * (2 * length - 1);
        if (gains[slot] >= minimumGain) {
          selectedPacked[selectedCount] = packed[slot];
          selectedLengths[selectedCount] = lengths[slot];
          selectedCount++;
        }
      }

      // Stable insertion sort is bounded by MAX_SYMBOLS. Move only strictly shorter entries so
      // equal lengths retain the gain-sort/map-iteration tie order byte-for-byte.
      for (int i = 1; i < selectedCount; i++) {
        final long key = selectedPacked[i];
        final byte length = selectedLengths[i];
        int target = i;
        while (target > 0 && (selectedLengths[target - 1] & 0xFF) < (length & 0xFF)) {
          selectedPacked[target] = selectedPacked[target - 1];
          selectedLengths[target] = selectedLengths[target - 1];
          target--;
        }
        selectedPacked[target] = key;
        selectedLengths[target] = length;
      }
    }

    private void stableSortByGain(final int count) {
      int[] source = order;
      int[] destination = sortScratch;
      for (int width = 1; width < count; width <<= 1) {
        for (int left = 0; left < count; left += width << 1) {
          final int middle = Math.min(left + width, count);
          final int right = Math.min(left + (width << 1), count);
          int first = left;
          int second = middle;
          int output = left;
          while (first < middle && second < right) {
            // Prefer the left run on equal gain: this is the stability contract of List.sort.
            if (gains[source[first]] >= gains[source[second]]) {
              destination[output++] = source[first++];
            } else {
              destination[output++] = source[second++];
            }
          }
          while (first < middle) {
            destination[output++] = source[first++];
          }
          while (second < right) {
            destination[output++] = source[second++];
          }
        }
        final int[] swap = source;
        source = destination;
        destination = swap;
      }
      if (source != order) {
        System.arraycopy(source, 0, order, 0, count);
      }
    }

    private void ensureSortCapacity(final int required) {
      if (order.length >= required) {
        return;
      }
      final int capacity = HashCommon.nextPowerOfTwo(required);
      order = new int[capacity];
      sortScratch = new int[capacity];
    }

    int selectedCount() {
      return selectedCount;
    }

    long selectedPacked(final int code) {
      return selectedPacked[code];
    }

    int selectedLength(final int code) {
      return selectedLengths[code] & 0xFF;
    }

    static int maximumCandidates(final List<byte[]> corpus) {
      long proposals = 0;
      for (final byte[] sample : corpus) {
        // At most one emitted-token proposal per input byte and one adjacent-token proposal after
        // the first token. Saturate at the hard ceiling; the map itself checks the actual distinct
        // count, so a long but repetitive corpus is still allowed to train.
        proposals = Math.min(MAX_DISTINCT_CANDIDATES,
            proposals + Math.max(1L, 2L * sample.length - 1L));
      }
      return (int) Math.max(256L, proposals);
    }

    /** Flat-range counterpart that preserves corpus order without manufacturing entry arrays. */
    static int maximumCandidates(final int[] lengths, final int[] corpusEntries, final int corpusSize) {
      long proposals = 0;
      for (int i = 0; i < corpusSize; i++) {
        final int length = lengths[corpusEntries[i]];
        proposals = Math.min(MAX_DISTINCT_CANDIDATES, proposals + Math.max(1L, 2L * length - 1L));
      }
      return (int) Math.max(256L, proposals);
    }
  }

  /** Bounded compatibility pool for callers that use the original stateless table-build API. */
  private static final int WORKSPACE_POOL_SIZE = Math.min(8, Math.max(1, Runtime.getRuntime().availableProcessors()));
  private static final ArrayDeque<Workspace> WORKSPACE_POOL = new ArrayDeque<>(WORKSPACE_POOL_SIZE);

  private static Workspace acquireWorkspace() {
    synchronized (WORKSPACE_POOL) {
      final Workspace workspace = WORKSPACE_POOL.pollFirst();
      if (workspace != null) {
        return workspace;
      }
    }
    return new Workspace();
  }

  private static void releaseWorkspace(final Workspace workspace) {
    workspace.clear();
    synchronized (WORKSPACE_POOL) {
      if (WORKSPACE_POOL.size() < WORKSPACE_POOL_SIZE) {
        WORKSPACE_POOL.addFirst(workspace);
      }
    }
  }

  /**
   * Acquire a buffer from the pool, or allocate a new one if pool is empty.
   *
   * @param minSize minimum required buffer size
   * @return a byte array of at least minSize bytes
   */
  private static byte[] acquireBuffer(int minSize) {
    synchronized (BUFFER_POOL) {
      byte[] buf = BUFFER_POOL.pollFirst();
      if (buf != null && buf.length >= minSize) {
        return buf;
      }
    }
    return new byte[Math.max(minSize, DEFAULT_BUFFER_SIZE)];
  }

  /**
   * Release a buffer back to the pool if there's room.
   *
   * @param buf the buffer to release
   */
  private static void releaseBuffer(byte[] buf) {
    if (buf == null || buf.length < DEFAULT_BUFFER_SIZE) {
      return; // Don't pool small buffers
    }
    synchronized (BUFFER_POOL) {
      if (BUFFER_POOL.size() < BUFFER_POOL_SIZE) {
        BUFFER_POOL.addFirst(buf);
      }
    }
  }

  private FSSTCompressor() {
    // Utility class
  }

  private static void validateFlatRanges(final byte[] backing, final int[] offsets, final int[] lengths,
      final int sampleCount) {
    Objects.requireNonNull(backing, "backing must not be null");
    Objects.requireNonNull(offsets, "offsets must not be null");
    Objects.requireNonNull(lengths, "lengths must not be null");
    if (sampleCount < 0 || sampleCount > offsets.length || sampleCount > lengths.length) {
      throw new IllegalArgumentException("sampleCount " + sampleCount + " exceeds offsets/lengths capacity ("
          + offsets.length + "/" + lengths.length + ")");
    }
    for (int i = 0; i < sampleCount; i++) {
      final int offset = offsets[i];
      final int length = lengths[i];
      if (rangeOutsideBacking(backing.length, offset, length)) {
        throw new IllegalArgumentException("entry " + i + " range [" + offset + ", "
            + ((long) offset + length) + ") outside a " + backing.length + "-byte backing");
      }
    }
  }

  private static void validateRange(final byte[] backing, final int offset, final int length, final String label) {
    Objects.requireNonNull(backing, "backing must not be null");
    if (rangeOutsideBacking(backing.length, offset, length)) {
      throw new IllegalArgumentException(label + " range [" + offset + ", " + ((long) offset + length)
          + ") outside a " + backing.length + "-byte backing");
    }
  }

  /** Validate a range without overflowing {@code offset + length}. */
  private static boolean rangeOutsideBacking(final int backingLength, final int offset, final int length) {
    return offset < 0 || length < 0 || offset > backingLength - length;
  }

  /**
   * Check if compression is beneficial by trial-encoding a sample of strings. This prevents applying
   * FSST when the overhead exceeds the savings.
   *
   * @param samples list of sample byte arrays to test
   * @param symbolTable the symbol table to use for trial compression
   * @return true if compression achieves at least MIN_COMPRESSION_RATIO savings
   */
  public static boolean isCompressionBeneficial(final List<byte[]> samples, final byte[] symbolTable) {
    if (symbolTable == null || symbolTable.length == 0 || samples == null || samples.isEmpty()) {
      return false;
    }

    final byte[][] parsedSymbols = parsedFor(symbolTable);
    if (parsedSymbols.length == 0) {
      return false;
    }

    final Workspace workspace = acquireWorkspace();
    try {
      return isCompressionBeneficial(samples, parsedSymbols, workspace);
    } finally {
      releaseWorkspace(workspace);
    }
  }

  /**
   * Workspace-backed trial used by streaming encoders that already parsed the table. The caller must
   * exclusively own {@code workspace} until this method and all encodes against the table finish.
   *
   * @param samples representative values to trial
   * @param parsedSymbols pre-parsed symbols in deterministic wire-code order
   * @param workspace owner-confined reusable matcher state
   * @return whether FSST saves at least {@link #MIN_COMPRESSION_RATIO}
   */
  public static boolean isCompressionBeneficial(final List<byte[]> samples, final byte[][] parsedSymbols,
      final Workspace workspace) {
    Objects.requireNonNull(workspace, "workspace must not be null");
    if (parsedSymbols == null || parsedSymbols.length == 0 || samples == null || samples.isEmpty()) {
      return false;
    }
    workspace.matcher.prepare(parsedSymbols);

    // Trial-compress a REPRESENTATIVE spread of samples. The previous first-16 trial was a
    // biased subset: samples arrive in slot order, so the first sixteen were typically one
    // page's first field — on real data a run of short ids or titles — and the verdict they
    // produced ("not beneficial") vetoed compression for whole workloads whose long text fields,
    // sitting later in the list, compress two-fold. A fixed stride sees every region of the
    // sample list at the same fixed cost.
    final int trialTarget = Math.min(samples.size(), 64);
    final int stride = Math.max(1, samples.size() / trialTarget);
    long originalSize = 0;
    long compressedSize = 0;

    for (int i = 0; i < samples.size(); i += stride) {
      byte[] sample = samples.get(i);
      if (sample == null || sample.length < MIN_COMPRESSION_SIZE) {
        continue;
      }
      originalSize += sample.length;
      final byte[] encoded = encodeWithParsedSymbols(sample, 0, sample.length, workspace.matcher);
      compressedSize += encoded.length;
    }

    if (originalSize == 0) {
      return false;
    }

    // Calculate savings ratio
    double ratio = 1.0 - ((double) compressedSize / originalSize);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("FSST trial: original={} compressed={} savings={}", originalSize, compressedSize,
          String.format(Locale.ROOT, "%.3f", ratio));
    }
    return ratio >= MIN_COMPRESSION_RATIO;
  }

  /**
   * Trial-compress entries addressed inside one flat backing array. Entry {@code i} is the range
   * {@code [offsets[i], offsets[i] + lengths[i])}; neither the trial nor matcher preparation creates
   * an entry-sized byte array.
   *
   * @param backing flat byte storage containing every entry
   * @param offsets entry start offsets
   * @param lengths entry byte lengths
   * @param sampleCount number of live entries in the primitive indexes
   * @param parsedSymbols pre-parsed symbols in wire-code order
   * @param workspace exclusively owned reusable matcher state
   * @return whether FSST saves at least {@link #MIN_COMPRESSION_RATIO}
   */
  public static boolean isCompressionBeneficial(final byte[] backing, final int[] offsets, final int[] lengths,
      final int sampleCount, final byte[][] parsedSymbols, final Workspace workspace) {
    Objects.requireNonNull(workspace, "workspace must not be null");
    validateFlatRanges(backing, offsets, lengths, sampleCount);
    if (parsedSymbols == null || parsedSymbols.length == 0 || sampleCount == 0) {
      return false;
    }
    workspace.matcher.prepare(parsedSymbols);

    final int trialTarget = Math.min(sampleCount, 64);
    final int stride = Math.max(1, sampleCount / trialTarget);
    long originalSize = 0;
    long compressedSize = 0;
    for (int i = 0; i < sampleCount; i += stride) {
      final int length = lengths[i];
      if (length < MIN_COMPRESSION_SIZE) {
        continue;
      }
      originalSize += length;
      compressedSize += encodeWithParsedSymbols(backing, offsets[i], length, workspace.matcher).length;
    }
    if (originalSize == 0) {
      return false;
    }
    final double ratio = 1.0 - ((double) compressedSize / originalSize);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("FSST flat-range trial: original={} compressed={} savings={}", originalSize, compressedSize,
          String.format(Locale.ROOT, "%.3f", ratio));
    }
    return ratio >= MIN_COMPRESSION_RATIO;
  }

  /**
   * Build a symbol table from sample strings.
   * 
   * <p>
   * Analyzes the samples to find frequently occurring byte sequences (1-8 bytes) and creates a lookup
   * table for compression.
   * 
   * @param samples list of sample byte arrays to analyze
   * @return symbol table bytes, or empty array if compression not beneficial
   * @throws NullPointerException if samples is null
   */
  public static byte[] buildSymbolTable(final List<byte[]> samples) {
    return buildSymbolTableInternal(samples, null);
  }

  /**
   * Build a symbol table using reusable, owner-confined matcher scratch.
   *
   * <p>
   * The returned table is a fresh array and never aliases {@code workspace}.
   *
   * @param samples list of sample byte arrays to analyze
   * @param workspace owner-confined reusable matcher state
   * @return symbol table bytes, or an empty array when the FSST eligibility gates do not pass
   */
  public static byte[] buildSymbolTable(final List<byte[]> samples, final Workspace workspace) {
    return buildSymbolTableInternal(samples, Objects.requireNonNull(workspace, "workspace must not be null"));
  }

  /**
   * Build a table directly from entries in one flat byte array. This is the stateless compatibility
   * form; a pooled owner-confined workspace supplies the primitive training scratch.
   */
  public static byte[] buildSymbolTable(final byte[] backing, final int[] offsets, final int[] lengths,
      final int sampleCount) {
    return buildSymbolTableFlat(backing, offsets, lengths, sampleCount, null);
  }

  /**
   * Build a table directly from entries in one flat byte array using caller-owned scratch. Training
   * reads the ranges in entry order, preserving the legacy map iteration, stable gain ties, symbol
   * codes, and serialized table bytes exactly.
   */
  public static byte[] buildSymbolTable(final byte[] backing, final int[] offsets, final int[] lengths,
      final int sampleCount, final Workspace workspace) {
    return buildSymbolTableFlat(backing, offsets, lengths, sampleCount,
        Objects.requireNonNull(workspace, "workspace must not be null"));
  }

  private static byte[] buildSymbolTableFlat(final byte[] backing, final int[] offsets, final int[] lengths,
      final int sampleCount, final Workspace callerWorkspace) {
    validateFlatRanges(backing, offsets, lengths, sampleCount);
    if (sampleCount < MIN_SAMPLES_FOR_TABLE) {
      return new byte[0];
    }

    int eligibleSamples = 0;
    long totalBytes = 0;
    for (int i = 0; i < sampleCount; i++) {
      final int length = lengths[i];
      if (length >= MIN_COMPRESSION_SIZE) {
        eligibleSamples++;
        totalBytes += length;
      }
    }
    if (eligibleSamples < MIN_SAMPLES_FOR_TABLE || totalBytes < MIN_TOTAL_BYTES_FOR_TABLE) {
      return new byte[0];
    }

    final boolean borrowed = callerWorkspace == null;
    final Workspace workspace = borrowed ? acquireWorkspace() : callerWorkspace;
    try {
      final int[] corpusEntries = workspace.flatCorpusEntries;
      int corpusSize = 0;
      for (int i = 0; i < sampleCount && corpusSize < MAX_SAMPLES_TO_ANALYZE; i++) {
        if (lengths[i] >= MIN_COMPRESSION_SIZE) {
          corpusEntries[corpusSize++] = i;
        }
      }
      if (!buildSymbolTableIteratively(backing, offsets, lengths, corpusEntries, corpusSize, workspace.matcher,
          workspace.candidates) || workspace.candidates.selectedCount() == 0) {
        return new byte[0];
      }
      return serializeSymbolTable(workspace.candidates);
    } finally {
      if (borrowed) {
        releaseWorkspace(workspace);
      }
    }
  }

  private static byte[] buildSymbolTableInternal(final List<byte[]> samples, final Workspace callerWorkspace) {
    Objects.requireNonNull(samples, "samples must not be null");

    if (samples.size() < MIN_SAMPLES_FOR_TABLE) {
      return new byte[0];
    }

    // First pass: check if we have enough data to justify compression
    int eligibleSamples = 0;
    long totalBytes = 0;
    for (final byte[] sample : samples) {
      if (sample != null && sample.length >= MIN_COMPRESSION_SIZE) {
        eligibleSamples++;
        totalBytes += sample.length;
      }
    }

    // Need enough samples AND enough total bytes
    if (eligibleSamples < MIN_SAMPLES_FOR_TABLE || totalBytes < MIN_TOTAL_BYTES_FOR_TABLE) {
      return new byte[0];
    }

    // Gather the analysis corpus once; the iterative builder makes several passes over it.
    final List<byte[]> corpus = new ArrayList<>(Math.min(samples.size(), MAX_SAMPLES_TO_ANALYZE));
    int sampleCount = 0;
    for (final byte[] sample : samples) {
      // Only analyze strings that are long enough to potentially benefit
      if (sample == null || sample.length < MIN_COMPRESSION_SIZE) {
        continue;
      }
      if (++sampleCount > MAX_SAMPLES_TO_ANALYZE) {
        break;
      }
      corpus.add(sample);
    }

    if (corpus.isEmpty()) {
      return new byte[0];
    }

    final boolean borrowed = callerWorkspace == null;
    final Workspace workspace = borrowed
        ? acquireWorkspace()
        : callerWorkspace;
    try {
      if (!buildSymbolTableIteratively(corpus, workspace.matcher, workspace.candidates)
          || workspace.candidates.selectedCount() == 0) {
        return new byte[0];
      }

      // Serialize symbol table. The result is detached from the workspace and remains valid after
      // a pooled workspace is cleared and handed to another caller.
      return serializeSymbolTable(workspace.candidates);
    } finally {
      if (borrowed) {
        releaseWorkspace(workspace);
      }
    }
  }

  /**
   * FSST-style iterative table construction (after Boncz, Neumann and Leis, "FSST: Fast Random Access
   * String Compression", VLDB 2020 — simplified to this encoder's escape scheme).
   *
   * <p>
   * One frequency pass over raw bytes cannot find good symbols: counting every 1..8-byte window
   * inflates substrings of frequent strings, and it scores text the encoder will never stand on —
   * after a symbol matches, the encoder is at a different position than the sliding window assumed.
   * FSST's insight is to make the table a fixed point of the encoder itself: encode the corpus with
   * the current table, credit what was actually emitted, credit the concatenation of each adjacent
   * token pair (capped at the wire's symbol width) so that useful symbols can grow — "htt" then "p:/"
   * proposes "http:/" — and rebuild from the highest-gain candidates. A few iterations converge;
   * symbols that stop earning their slot fall out.
   *
   * <p>
   * Gains use this encoder's real cost model: an unmatched byte escapes to TWO bytes, so an emitted
   * length-L symbol saves {@code 2L - 1} per use, and a frequent single byte always deserves a code
   * (saving 1 per use) — which is also why the seed table is the byte histogram.
   */
  private static boolean buildSymbolTableIteratively(final List<byte[]> corpus, final SymbolMatcher matcher,
      final CandidateWorkspace candidates) {
    final int maximumCandidates = CandidateWorkspace.maximumCandidates(corpus);
    candidates.reset(maximumCandidates);

    // Seed histogram over a flat int[256] — one map entry per DISTINCT byte value, not one
    // hash probe plus wrapper allocation per corpus byte.
    final int[] byteCounts = candidates.byteCounts();
    for (final byte[] sample : corpus) {
      for (final byte b : sample) {
        byteCounts[b & 0xFF]++;
      }
    }
    for (int b = 0; b < 256; b++) {
      if (byteCounts[b] > 0 && !candidates.putByte(b, byteCounts[b])) {
        candidates.clear();
        return false;
      }
    }
    candidates.selectTop();

    for (int iteration = 0; iteration < TABLE_BUILD_ITERATIONS; iteration++) {
      matcher.reset(candidates);
      candidates.reset(maximumCandidates);
      for (final byte[] sample : corpus) {
        int pos = 0;
        int prevStart = -1;
        int prevLen = 0;
        while (pos < sample.length) {
          final long match = matcher.longestPackedMatch(sample, pos, sample.length);
          final int len = match >= 0
              ? SymbolMatcher.matchLength(match)
              : 1;
          if (!candidates.addTo(sample, pos, len, match >= 0
              ? (2 * len - 1)
              : 1)) {
            candidates.clear();
            matcher.clear();
            return false;
          }
          if (prevStart >= 0) {
            final int concatLen = Math.min(prevLen + len, MAX_SYMBOL_LENGTH);
            if (concatLen > prevLen && prevStart + concatLen <= sample.length) {
              if (!candidates.addTo(sample, prevStart, concatLen, 2 * concatLen - 1)) {
                candidates.clear();
                matcher.clear();
                return false;
              }
            }
          }
          prevStart = pos;
          prevLen = len;
          pos += len;
        }
      }
      candidates.selectTop();
    }
    return true;
  }

  /** Flat-range training twin of {@link #buildSymbolTableIteratively(List, SymbolMatcher, CandidateWorkspace)}. */
  private static boolean buildSymbolTableIteratively(final byte[] backing, final int[] offsets, final int[] lengths,
      final int[] corpusEntries, final int corpusSize, final SymbolMatcher matcher,
      final CandidateWorkspace candidates) {
    final int maximumCandidates = CandidateWorkspace.maximumCandidates(lengths, corpusEntries, corpusSize);
    candidates.reset(maximumCandidates);

    final int[] byteCounts = candidates.byteCounts();
    for (int corpusIndex = 0; corpusIndex < corpusSize; corpusIndex++) {
      final int entry = corpusEntries[corpusIndex];
      final int end = offsets[entry] + lengths[entry];
      for (int pos = offsets[entry]; pos < end; pos++) {
        byteCounts[backing[pos] & 0xFF]++;
      }
    }
    for (int value = 0; value < 256; value++) {
      if (byteCounts[value] > 0 && !candidates.putByte(value, byteCounts[value])) {
        candidates.clear();
        return false;
      }
    }
    candidates.selectTop();

    for (int iteration = 0; iteration < TABLE_BUILD_ITERATIONS; iteration++) {
      matcher.reset(candidates);
      candidates.reset(maximumCandidates);
      for (int corpusIndex = 0; corpusIndex < corpusSize; corpusIndex++) {
        final int entry = corpusEntries[corpusIndex];
        final int start = offsets[entry];
        final int end = start + lengths[entry];
        int pos = start;
        int previousStart = -1;
        int previousLength = 0;
        while (pos < end) {
          final long match = matcher.longestPackedMatch(backing, pos, end);
          final int length = match >= 0 ? SymbolMatcher.matchLength(match) : 1;
          if (!candidates.addTo(backing, pos, length, match >= 0 ? 2 * length - 1 : 1)) {
            candidates.clear();
            matcher.clear();
            return false;
          }
          if (previousStart >= 0) {
            final int concatenatedLength = Math.min(previousLength + length, MAX_SYMBOL_LENGTH);
            if (concatenatedLength > previousLength && previousStart + concatenatedLength <= end
                && !candidates.addTo(backing, previousStart, concatenatedLength, 2 * concatenatedLength - 1)) {
              candidates.clear();
              matcher.clear();
              return false;
            }
          }
          previousStart = pos;
          previousLength = length;
          pos += length;
        }
      }
      candidates.selectTop();
    }
    return true;
  }

  /** Fixed-point iterations for {@link #buildSymbolTableIteratively}; FSST proper uses five. */
  private static final int TABLE_BUILD_ITERATIONS = 5;

  /**
   * Serialize symbol table to bytes. Format:
   * [numSymbols:1][len1:1][len2:1]...[symbol1:len1][symbol2:len2]...
   */
  private static byte[] serializeSymbolTable(final CandidateWorkspace symbols) {
    // Calculate total size
    final int symbolCount = symbols.selectedCount();
    int totalSize = TABLE_HEADER_SIZE + symbolCount; // numSymbols + length bytes
    for (int code = 0; code < symbolCount; code++) {
      totalSize += symbols.selectedLength(code);
    }

    final byte[] table = new byte[totalSize];
    int pos = 0;

    // Number of symbols
    table[pos++] = (byte) symbolCount;

    // Symbol lengths
    for (int code = 0; code < symbolCount; code++) {
      table[pos++] = (byte) symbols.selectedLength(code);
    }

    // Symbol data
    for (int code = 0; code < symbolCount; code++) {
      final long packed = symbols.selectedPacked(code);
      final int length = symbols.selectedLength(code);
      for (int i = 0; i < length; i++) {
        table[pos++] = (byte) (packed >>> (i << 3));
      }
    }

    return table;
  }

  /**
   * Parse symbol table from bytes.
   * 
   * @param tableBytes serialized symbol table
   * @return array of symbol byte arrays, indexed by code (0 to numSymbols-1)
   */
  public static byte[][] parseSymbolTable(final byte[] tableBytes) {
    if (tableBytes == null || tableBytes.length == 0) {
      return new byte[0][];
    }

    int pos = 0;
    final int numSymbols = tableBytes[pos++] & 0xFF;

    if (numSymbols == 0 || pos + numSymbols > tableBytes.length) {
      return new byte[0][];
    }

    // Read lengths. A zero-length symbol can never be produced by the builder and would match
    // vacuously at every position without consuming input — the linear encode path would spin
    // until it overran its scratch — so a table claiming one is corrupt and rejected whole.
    final int[] lengths = new int[numSymbols];
    for (int i = 0; i < numSymbols; i++) {
      lengths[i] = tableBytes[pos++] & 0xFF;
      if (lengths[i] == 0) {
        return new byte[0][];
      }
    }

    // Read symbols
    final byte[][] symbols = new byte[numSymbols][];
    for (int i = 0; i < numSymbols; i++) {
      final int len = lengths[i];
      if (pos + len > tableBytes.length) {
        return new byte[0][]; // Corrupted table
      }
      symbols[i] = Arrays.copyOfRange(tableBytes, pos, pos + len);
      pos += len;
    }

    return symbols;
  }

  /**
   * Encode a byte array using the symbol table.
   * 
   * <p>
   * Uses greedy matching: tries longest symbols first. Unmatched bytes are escaped with ESCAPE_BYTE
   * prefix.
   * 
   * <p>
   * Encoding scheme:
   * <ul>
   * <li>Bytes 0 to (numSymbols-1): symbol codes</li>
   * <li>Byte 0xFF: escape marker, next byte is literal</li>
   * <li>All literals are escaped to avoid confusion with symbol codes</li>
   * </ul>
   * 
   * @param input data to compress
   * @param symbolTable serialized symbol table (from buildSymbolTable)
   * @return compressed data, or original if compression not beneficial
   * @throws NullPointerException if input is null
   */
  public static byte[] encode(final byte[] input, final byte[] symbolTable) {
    Objects.requireNonNull(input, "input must not be null");

    // If no symbol table, return input as-is (no header, no modification)
    if (symbolTable == null || symbolTable.length == 0) {
      return input.clone();
    }

    final byte[][] symbols = parsedFor(symbolTable);
    if (symbols.length == 0) {
      return input.clone();
    }

    return encodeWithParsedSymbols(input, 0, input.length, symbols);
  }

  /**
   * Encode using pre-parsed symbol table. Avoids re-parsing the symbol table on every call when
   * encoding multiple values with the same table (e.g., all strings on a page).
   *
   * @param input data to compress
   * @param parsedSymbols pre-parsed symbol table from {@link #parseSymbolTable(byte[])}
   * @return compressed data, or original if compression not beneficial
   */
  public static byte[] encode(final byte[] input, final byte[][] parsedSymbols) {
    Objects.requireNonNull(input, "input must not be null");

    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return input.clone();
    }

    return encodeWithParsedSymbols(input, 0, input.length, parsedSymbols);
  }

  /**
   * Encode using an explicitly-owned reusable matcher. This is the allocation-stable bulk form; the
   * returned byte array is fresh and does not alias {@code workspace}.
   *
   * @param input data to compress
   * @param parsedSymbols pre-parsed symbol table in wire-code order
   * @param workspace owner-confined reusable matcher state
   * @return compressed or raw-headered data, exactly as {@link #encode(byte[], byte[][])}
   */
  public static byte[] encode(final byte[] input, final byte[][] parsedSymbols, final Workspace workspace) {
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(workspace, "workspace must not be null");

    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return input.clone();
    }

    workspace.matcher.prepare(parsedSymbols);
    return encodeWithParsedSymbols(input, 0, input.length, workspace.matcher);
  }

  /**
   * Workspace-backed encode of one range in a flat dictionary backing. The returned array is fresh;
   * raw fallback copies only the live range and never aliases {@code input} or {@code workspace}.
   */
  public static byte[] encode(final byte[] input, final int offset, final int length,
      final byte[][] parsedSymbols, final Workspace workspace) {
    Objects.requireNonNull(workspace, "workspace must not be null");
    validateRange(input, offset, length, "input");
    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return Arrays.copyOfRange(input, offset, offset + length);
    }
    workspace.matcher.prepare(parsedSymbols);
    return encodeWithParsedSymbols(input, offset, length, workspace.matcher);
  }

  /**
   * Encode a slice of {@code input}, or return {@code null} when the encoding would not shrink it.
   * Built for the store-if-smaller call sites (insert-time and commit-time compression), which only
   * ever keep the beneficial outcome: handing them a raw-headered copy to discard — what
   * {@link #encode(byte[], byte[][])} produces — was one wasted allocation plus memcpy per
   * incompressible string, and materializing parser-buffer slices into arrays first was another per
   * string at ingest rate.
   *
   * @param input data to compress
   * @param off start of the value within {@code input}
   * @param len value length
   * @param parsedSymbols pre-parsed symbol table from {@link #parseSymbolTable(byte[])}
   * @return headered compressed bytes strictly shorter than {@code len}, or {@code null} to store the
   *         value raw
   */
  public static byte[] encodeOrNull(final byte[] input, final int off, final int len, final byte[][] parsedSymbols) {
    Objects.requireNonNull(input, "input must not be null");

    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return null;
    }

    return encodeBeneficialOrNull(input, off, len, parsedSymbols);
  }

  /**
   * Largest encode scratch retained across calls; anything bigger is a one-off allocation. An
   * uncapped scratch grows to twice the largest value a thread ever encodes and pins that memory for
   * the thread's life — one 50 MB text value must not cost 100 MB per pool worker forever.
   */
  private static final int MAX_RETAINED_SCRATCH = 1 << 20;

  /** Per-thread encode output scratch; grows with demand up to {@link #MAX_RETAINED_SCRATCH}. */
  private static final ThreadLocal<byte[]> ENCODE_SCRATCH = ThreadLocal.withInitial(() -> new byte[8 * 1024]);

  /** Entries in each identity-keyed ring cache below. */
  private static final int RING_ENTRIES = 8;

  /**
   * A small identity-keyed ring: key/value pairs plus a primitive insertion cursor (an
   * {@code Object[]} slot would box the cursor on every insert). Eight entries because a single-entry
   * cache thrashes the moment a thread alternates between two tables — exactly what happens when a
   * commit combines pages bound to an old table with pages on the new one, or when a query scans
   * column segments carrying different tables.
   */
  private static final class Ring {
    final Object[] slots = new Object[2 * RING_ENTRIES];
    int cursor;

    Object lookup(final Object key) {
      for (int i = 0; i < RING_ENTRIES; i++) {
        if (slots[2 * i] == key) {
          return slots[2 * i + 1];
        }
      }
      return null;
    }

    void insert(final Object key, final Object value) {
      slots[2 * cursor] = key;
      slots[2 * cursor + 1] = value;
      cursor = (cursor + 1) & (RING_ENTRIES - 1);
    }
  }

  /**
   * Per-thread identity ring from parsed tables to their {@link SymbolMatcher}. Reuse contract: the
   * caller-facing API passes a parsed {@code byte[][]} per call, and every page-, trial- and
   * combine-level flow passes the SAME array instance for a whole batch of values, so one identity
   * check replaces rebuilding the bucket index per string.
   */
  private static final ThreadLocal<Ring> MATCHER_CACHE = ThreadLocal.withInitial(Ring::new);

  /**
   * Identity ring from serialized table bytes to their parsed form. The revision build hands every
   * page of a commit the SAME table byte array, so parsing per page — and therefore rebuilding the
   * matcher per page, since the matcher cache keys on the parsed array's identity — was pure waste
   * multiplied by thousands of pages.
   */
  private static final ThreadLocal<Ring> PARSED_CACHE = ThreadLocal.withInitial(Ring::new);

  /**
   * Parse {@code tableBytes}, reusing a recent result when the same array instance is asked for again
   * on this thread.
   *
   * @param tableBytes the serialized symbol table
   * @return the parsed symbols; empty when the table is null or empty
   */
  public static byte[][] parsedFor(final byte[] tableBytes) {
    if (tableBytes == null || tableBytes.length == 0) {
      return EMPTY_PARSED;
    }
    final Ring ring = PARSED_CACHE.get();
    final Object cached = ring.lookup(tableBytes);
    if (cached != null) {
      return (byte[][]) cached;
    }
    final byte[][] parsed = parseSymbolTable(tableBytes);
    ring.insert(tableBytes, parsed);
    return parsed;
  }

  private static final byte[][] EMPTY_PARSED = new byte[0][];

  /**
   * Inputs at or below this length take the linear-scan encode path when no matcher is cached for the
   * table yet. Building a {@link SymbolMatcher} allocates and zeroes ~½ MB of bucket index — sound
   * when a whole page or commit amortizes it, absurd for encoding one short probe value (a query
   * filter constant, a single trial string) against a table this thread has never batch-encoded with.
   */
  private static final int SMALL_INPUT_LINEAR_LIMIT = 256;

  /**
   * Consecutive small-input linear encodes against the same table before this thread builds the
   * matcher anyway. Bulk flows are dominated by strings under {@link #SMALL_INPUT_LINEAR_LIMIT} —
   * JSON values average well below it — so without promotion a thread that only ever sees small
   * strings would take the linear scan forever, reinstating the
   * encode-cost-proportional-to-table-size pathology the matcher was built to kill. A handful of
   * one-off probes stay cheap; the fifth encode against the same table is evidence of a batch, and
   * one matcher build amortizes across everything that follows.
   */
  private static final int LINEAR_PROMOTION_THRESHOLD = 4;

  /** Per-thread linear-encode streak: the table identity and a primitive run length. */
  private static final class LinearStreak {
    byte[][] table;
    int count;
  }

  private static final ThreadLocal<LinearStreak> LINEAR_STREAK = ThreadLocal.withInitial(LinearStreak::new);

  /**
   * Whether this thread has linear-encoded against {@code symbols} often enough to justify a matcher.
   */
  private static boolean promoteToMatcher(final byte[][] symbols) {
    final LinearStreak streak = LINEAR_STREAK.get();
    if (streak.table != symbols) {
      streak.table = symbols;
      streak.count = 1;
      return false;
    }
    return ++streak.count > LINEAR_PROMOTION_THRESHOLD;
  }

  /**
   * Inputs longer than this store raw: the escape-worst-case doubling would overflow an int-indexed
   * array, and a value of this size has no business in a string region anyway.
   */
  private static final int MAX_ENCODABLE_LENGTH = (Integer.MAX_VALUE - 8) / 2;

  private static byte[] encodeWithParsedSymbols(final byte[] input, final int off, final int len,
      final byte[][] symbols) {
    final byte[] beneficial = encodeBeneficialOrNull(input, off, len, symbols);
    return beneficial != null
        ? beneficial
        : markAsRaw(input, off, len);
  }

  private static byte[] encodeWithParsedSymbols(final byte[] input, final int off, final int len,
      final SymbolMatcher matcher) {
    final byte[] beneficial = encodeBeneficialOrNull(input, off, len, matcher);
    return beneficial != null
        ? beneficial
        : markAsRaw(input, off, len);
  }

  /**
   * The encode core over a slice: headered compressed bytes when they beat raw-plus-header,
   * {@code null} otherwise (too small, too big, or the encoding did not shrink the value).
   */
  private static byte[] encodeBeneficialOrNull(final byte[] input, final int off, final int len,
      final byte[][] symbols) {
    if (len < MIN_COMPRESSION_SIZE || len > MAX_ENCODABLE_LENGTH) {
      return null;
    }

    final Ring ring = MATCHER_CACHE.get();
    SymbolMatcher matcher = (SymbolMatcher) ring.lookup(symbols);
    if (matcher == null) {
      if (len <= SMALL_INPUT_LINEAR_LIMIT && !promoteToMatcher(symbols)) {
        return encodeLinear(input, off, len, symbols);
      }
      matcher = new SymbolMatcher(symbols);
      ring.insert(symbols, matcher);
    }

    return encodeBeneficialOrNull(input, off, len, matcher);
  }

  /** Encode core for a matcher already prepared by either the identity cache or a workspace. */
  private static byte[] encodeBeneficialOrNull(final byte[] input, final int off, final int len,
      final SymbolMatcher matcher) {
    if (len < MIN_COMPRESSION_SIZE || len > MAX_ENCODABLE_LENGTH) {
      return null;
    }

    // Encode into a reused per-thread scratch (worst case 2x input due to escapes). A fresh
    // array per value was two zeroed allocations per string across millions of strings — pure
    // GC pressure, since only the exact-size copy at the end survives.
    final byte[] output = encodeScratch(len);
    final int end = off + len;
    int outPos = 0;
    int inPos = off;

    // Emit loop — keep in lockstep with encodeLinear below: same escape scheme, same wire
    // bytes, only the match source differs.
    while (inPos < end) {
      final long match = matcher.longestMatch(input, inPos, end);
      if (match >= 0) {
        output[outPos++] = (byte) SymbolMatcher.matchCode(match);
        inPos += SymbolMatcher.matchLength(match);
      } else {
        // Escape literal byte - ALL literals are escaped to avoid confusion with symbol codes
        output[outPos++] = ESCAPE_BYTE;
        output[outPos++] = input[inPos++];
      }
    }

    return finishEncode(len, output, outPos);
  }

  /**
   * Greedy longest-match encode by direct scan over the symbol list. Symbols are serialized
   * longest-first by the table builder, so the first symbol that matches IS the longest match — same
   * result as {@link SymbolMatcher#longestMatch}, none of its index cost.
   */
  private static byte[] encodeLinear(final byte[] input, final int off, final int len, final byte[][] symbols) {
    final byte[] output = encodeScratch(len);
    final int end = off + len;
    int outPos = 0;
    int inPos = off;

    // Emit loop — keep in lockstep with encodeBeneficialOrNull above: identical wire bytes
    // are a correctness requirement, since the decoder cannot tell which path produced them.
    while (inPos < end) {
      final int avail = end - inPos;
      int code = -1;
      for (int s = 0; s < symbols.length; s++) {
        final byte[] symbol = symbols[s];
        if (symbol.length <= avail && matches(input, inPos, symbol)) {
          code = s;
          break;
        }
      }
      if (code >= 0) {
        output[outPos++] = (byte) code;
        inPos += symbols[code].length;
      } else {
        output[outPos++] = ESCAPE_BYTE;
        output[outPos++] = input[inPos++];
      }
    }

    return finishEncode(len, output, outPos);
  }

  /**
   * The per-thread scratch, grown (with overflow-safe arithmetic) to hold 2x {@code inputLength}.
   * Demand beyond {@link #MAX_RETAINED_SCRATCH} gets a one-off array that is NOT stored back — one
   * oversized value must not pin megabytes on the thread forever.
   */
  private static byte[] encodeScratch(final int inputLength) {
    final byte[] output = ENCODE_SCRATCH.get();
    final long required = 2L * inputLength;
    if (output.length >= required) {
      return output;
    }
    if (required > MAX_RETAINED_SCRATCH) {
      return new byte[(int) required];
    }
    final byte[] grown = new byte[(int) Math.min(MAX_RETAINED_SCRATCH, Math.max(required, 2L * output.length))];
    ENCODE_SCRATCH.set(grown);
    return grown;
  }

  /**
   * Headered result copied out of the shared scratch, or {@code null} when the encoding did not beat
   * raw-plus-header — the caller decides whether "not beneficial" means a raw-marked copy or no bytes
   * at all.
   */
  private static byte[] finishEncode(final int len, final byte[] output, final int outPos) {
    // Check if compression was beneficial (include header byte in comparison)
    if (outPos + 1 >= len) {
      return null;
    }

    final byte[] result = new byte[outPos + 1];
    result[0] = HEADER_COMPRESSED;
    System.arraycopy(output, 0, result, 1, outPos);
    return result;
  }

  /**
   * Encode a MemorySegment using the symbol table.
   * 
   * @param segment data to compress
   * @param symbolTable serialized symbol table
   * @return compressed data
   */
  public static byte[] encode(final MemorySegment segment, final byte[] symbolTable) {
    Objects.requireNonNull(segment, "segment must not be null");
    return encode(segment.toArray(ValueLayout.JAVA_BYTE), symbolTable);
  }

  /**
   * Mark data as raw (not compressed) with header byte.
   */
  private static byte[] markAsRaw(final byte[] input) {
    return markAsRaw(input, 0, input.length);
  }

  private static byte[] markAsRaw(final byte[] input, final int off, final int len) {
    final byte[] result = new byte[len + 1];
    result[0] = HEADER_RAW;
    System.arraycopy(input, off, result, 1, len);
    return result;
  }

  /**
   * Check if input matches symbol at given position.
   */
  private static boolean matches(final byte[] input, final int pos, final byte[] symbol) {
    for (int i = 0; i < symbol.length; i++) {
      if (input[pos + i] != symbol[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Decode compressed data using the symbol table.
   * 
   * @param encoded compressed data (with header byte)
   * @param symbolTable serialized symbol table
   * @return decompressed data
   * @throws NullPointerException if encoded is null
   * @throws IllegalStateException if data is corrupted
   */
  public static byte[] decode(final byte[] encoded, final byte[] symbolTable) {
    Objects.requireNonNull(encoded, "encoded must not be null");

    if (encoded.length == 0) {
      return new byte[0];
    }

    if (symbolTable == null || symbolTable.length == 0) {
      // No compression was applied - return as-is (no header expected)
      return encoded.clone();
    }

    // parsedFor, not parseSymbolTable: per-node reads decode one value at a time against the
    // same shared table array, and re-parsing it per string was ~255 small allocations each.
    final byte[][] symbols = parsedFor(symbolTable);
    if (symbols.length == 0) {
      return encoded.clone();
    }

    return decodeWithParsedSymbols(encoded, symbols);
  }

  /**
   * Decode using pre-parsed symbol table. Avoids re-parsing the symbol table on every call when
   * decoding multiple values with the same table (e.g., all strings on a page).
   *
   * @param encoded compressed data (with header byte)
   * @param parsedSymbols pre-parsed symbol table from {@link #parseSymbolTable(byte[])}
   * @return decompressed data
   */
  public static byte[] decode(final byte[] encoded, final byte[][] parsedSymbols) {
    Objects.requireNonNull(encoded, "encoded must not be null");

    if (encoded.length == 0) {
      return new byte[0];
    }

    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return encoded.clone();
    }

    return decodeWithParsedSymbols(encoded, parsedSymbols);
  }

  private static byte[] decodeWithParsedSymbols(final byte[] encoded, final byte[][] symbols) {
    return decodeWithParsedSymbols(encoded, 0, encoded.length, symbols);
  }

  /**
   * Decode FSST-compressed data from a slice of a byte array. Avoids the need to copy the slice into
   * a separate array first.
   *
   * @param data the byte array containing encoded data
   * @param offset start offset within data
   * @param length number of bytes to decode
   * @param symbols pre-parsed FSST symbol table
   * @return decoded byte array
   */
  private static byte[] decodeWithParsedSymbols(final byte[] data, final int offset, final int length,
      final byte[][] symbols) {
    if (length == 0) {
      return EMPTY_BYTES;
    }

    // Check header byte
    final int end = offset + length;
    final byte header = data[offset];
    if (header == HEADER_RAW) {
      // Data is raw, just strip the header
      return Arrays.copyOfRange(data, offset + 1, end);
    }

    if (header != HEADER_COMPRESSED) {
      // Unknown header - treat as legacy raw data
      return Arrays.copyOfRange(data, offset, end);
    }

    // Decode compressed data (skip header byte)
    return decodeRawCompressed(data, offset + 1, length - 1, symbols);
  }

  /**
   * Decode headerless FSST-compressed data from a slice of a byte array. Unlike
   * {@link #decodeWithParsedSymbols}, this method does NOT expect a header byte — the data starts
   * directly with compressed symbol codes and escape sequences.
   *
   * <p>
   * Use this for page-extracted compressed payloads where the on-page format is
   * {@code [isCompressed:1byte][length:varint][payload_bytes]} and the payload contains raw
   * compressed bytes without the FSST header.
   * </p>
   *
   * @param data the byte array containing raw compressed data (no header)
   * @param offset start offset within data
   * @param length number of bytes to decode
   * @param symbols pre-parsed FSST symbol table
   * @return decoded byte array
   */
  static byte[] decodeRawCompressed(final byte[] data, final int offset, final int length, final byte[][] symbols) {
    if (length == 0) {
      return EMPTY_BYTES;
    }

    final int end = offset + length;
    // Pre-allocate output buffer — use 3x as typical FSST expansion rather than 8x
    byte[] output = new byte[Math.max(length * 3, 64)];
    int outPos = 0;
    int pos = offset;

    while (pos < end) {
      final int b = data[pos++] & 0xFF;

      if (b == 0xFF) {
        // Escape: next byte is literal
        if (pos >= end) {
          throw new IllegalStateException("Corrupted FSST data: escape at end");
        }
        if (outPos >= output.length) {
          output = Arrays.copyOf(output, output.length * 2);
        }
        output[outPos++] = data[pos++];
      } else if (b < symbols.length) {
        // Symbol code: expand to symbol bytes using System.arraycopy for efficiency
        final byte[] symbol = symbols[b];
        if (outPos + symbol.length > output.length) {
          output = Arrays.copyOf(output, Math.max(output.length * 2, outPos + symbol.length));
        }
        System.arraycopy(symbol, 0, output, outPos, symbol.length);
        outPos += symbol.length;
      } else {
        // This shouldn't happen with proper encoding (all literals escaped)
        throw new IllegalStateException("Corrupted FSST data: unexpected byte code " + b);
      }
    }

    // Single allocation for final sized array
    return Arrays.copyOf(output, outPos);
  }

  /**
   * Worst-case decoded size of {@code length} encoded bytes: every byte a symbol code expanding to
   * the longest symbol. An escape pair yields one byte, so it never exceeds this.
   */
  public static int maxDecodedLength(final int length) {
    return length * MAX_SYMBOL_LENGTH;
  }

  /**
   * Decode one FSST-compressed value straight into a caller-owned buffer — the allocation-free twin
   * of {@link #decode(byte[], byte[][])}, for decoding many values into one flat run.
   *
   * <p>
   * The caller must have reserved {@link #maxDecodedLength(int)} bytes at {@code outPos}, which lifts
   * the capacity check out of the byte loop; a short buffer is a caller bug and throws rather than
   * silently truncating.
   *
   * @param data the buffer holding the encoded value (header byte first, as {@link #encode} writes
   *        it)
   * @param offset start of the encoded value within {@code data}
   * @param length its encoded length
   * @param symbols pre-parsed symbol table, see {@link #parsedFor(byte[])}
   * @param out destination buffer
   * @param outPos where to write the decoded bytes
   * @return the position in {@code out} one past the last decoded byte
   * @throws IllegalStateException if the encoded data is corrupt
   * @throws IllegalArgumentException if {@code out} has no room for the worst case
   */
  public static int decodeInto(final byte[] data, final int offset, final int length, final byte[][] symbols,
      final byte[] out, final int outPos) {
    Objects.requireNonNull(data, "data must not be null");
    Objects.requireNonNull(out, "out must not be null");
    if (length == 0) {
      return outPos;
    }
    if (out.length - outPos < maxDecodedLength(length)) {
      throw new IllegalArgumentException(
          "out has " + (out.length - outPos) + " bytes free, worst case needs " + maxDecodedLength(length));
    }
    if (symbols == null || symbols.length == 0) {
      // No table: the value was never encoded, so it is the slice verbatim — header and all,
      // exactly what decode(byte[], byte[][]) hands back in this case.
      System.arraycopy(data, offset, out, outPos, length);
      return outPos + length;
    }
    final byte header = data[offset];
    if (header != HEADER_COMPRESSED) {
      // Raw with its header stripped, or a legacy payload carrying no header at all — the same two
      // shapes decodeWithParsedSymbols distinguishes.
      final int from = header == HEADER_RAW
          ? offset + 1
          : offset;
      final int n = offset + length - from;
      System.arraycopy(data, from, out, outPos, n);
      return outPos + n;
    }
    final int end = offset + length;
    int pos = offset + 1;
    int o = outPos;
    while (pos < end) {
      final int b = data[pos++] & 0xFF;
      if (b == 0xFF) {
        if (pos >= end) {
          throw new IllegalStateException("Corrupted FSST data: escape at end");
        }
        out[o++] = data[pos++];
      } else if (b < symbols.length) {
        final byte[] symbol = symbols[b];
        final int len = symbol.length;
        if (o + len > out.length) {
          // The reservation assumes MAX_SYMBOL_LENGTH, which every table this codec WRITES obeys;
          // the parser accepts a length byte up to 255, so a table claiming a longer symbol is
          // corrupt. Say so in the codec's own currency rather than letting an array store throw —
          // callers memoize IllegalStateException as permanent corruption and decline fail-soft.
          throw new IllegalStateException("Corrupted FSST symbol table: symbol " + b + " is " + len
              + " bytes, over the " + MAX_SYMBOL_LENGTH + "-byte maximum");
        }
        // Short symbols dominate (the table caps at MAX_SYMBOL_LENGTH), and arraycopy's call
        // overhead beats a hand loop only well past that — copy the common lengths inline.
        if (len == 1) {
          out[o] = symbol[0];
        } else {
          System.arraycopy(symbol, 0, out, o, len);
        }
        o += len;
      } else {
        throw new IllegalStateException("Corrupted FSST data: unexpected byte code " + b);
      }
    }
    return o;
  }

  /**
   * Decode headerless FSST-compressed data using pre-parsed symbols. For page-extracted compressed
   * payloads that do NOT have the FSST header byte.
   *
   * <p>
   * Pages store compressed string values as
   * {@code [isCompressed:1byte][length:varint][payload_bytes]} where {@code payload_bytes} are the
   * raw compressed bytes without the FSST header. Use this method (instead of
   * {@link #decode(byte[], byte[][])}) when decoding such payloads.
   * </p>
   *
   * @param data headerless compressed byte array
   * @param symbols pre-parsed FSST symbol table from {@link #parseSymbolTable(byte[])}
   * @return decoded byte array
   */
  public static byte[] decodeRaw(final byte[] data, final byte[][] symbols) {
    Objects.requireNonNull(data, "data must not be null");

    if (data.length == 0) {
      return EMPTY_BYTES;
    }

    if (symbols == null || symbols.length == 0) {
      return data.clone();
    }

    return decodeRawCompressed(data, 0, data.length, symbols);
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Decode from MemorySegment (zero-copy input).
   * 
   * @param segment compressed data segment
   * @param symbolTable serialized symbol table
   * @return decompressed data
   */
  public static byte[] decode(final MemorySegment segment, final byte[] symbolTable) {
    Objects.requireNonNull(segment, "segment must not be null");
    return decode(segment.toArray(ValueLayout.JAVA_BYTE), symbolTable);
  }

  /**
   * Check if compression would be beneficial for the given samples.
   * 
   * @param samples list of sample byte arrays
   * @return true if FSST compression is likely to reduce size
   */
  public static boolean isCompressible(final List<byte[]> samples) {
    if (samples == null || samples.size() < MIN_SAMPLES_FOR_TABLE) {
      return false;
    }

    // First pass: count eligible samples and total bytes
    int eligibleSamples = 0;
    long totalBytes = 0;
    for (final byte[] sample : samples) {
      if (sample != null && sample.length >= MIN_COMPRESSION_SIZE) {
        eligibleSamples++;
        totalBytes += sample.length;
      }
    }

    // Need enough samples AND enough total bytes
    if (eligibleSamples < MIN_SAMPLES_FOR_TABLE || totalBytes < MIN_TOTAL_BYTES_FOR_TABLE) {
      return false;
    }

    // Quick heuristic: check if there are common patterns in a subset
    final Object2IntOpenHashMap<ByteSequence> frequencyMap = new Object2IntOpenHashMap<>();
    int sampleCount = 0;

    for (final byte[] sample : samples) {
      if (sample == null || sample.length < MIN_COMPRESSION_SIZE) {
        continue;
      }
      if (++sampleCount > 32) { // Quick sample
        break;
      }

      // Count 2-byte sequences only for quick check
      for (int i = 0; i < sample.length - 1; i++) {
        final ByteSequence seq = new ByteSequence(sample, i, 2);
        frequencyMap.addTo(seq, 1);
      }
    }

    // Check if any pattern occurs frequently enough
    int frequentPatterns = 0;
    for (final int freq : frequencyMap.values()) {
      if (freq >= 3) {
        frequentPatterns++;
      }
    }

    return frequentPatterns >= 8; // Require more frequent patterns
  }

  /**
   * Batch-decode FSST values from MemorySegments, only for selected rows. Each row may reference a
   * different page (multi-page batches). Reuses a single decode buffer to minimize allocation.
   *
   * @param pages array of backing page MemorySegments
   * @param pageIndices per-row page index into pages array
   * @param offsets per-row absolute byte offset in the page
   * @param lengths per-row compressed value byte length
   * @param isCompressed per-row compression flag
   * @param selectionVector indices of rows to decode
   * @param selectionCount number of valid entries in selectionVector
   * @param symbolsByPage per-page pre-parsed symbol tables (null entry = no FSST)
   * @param output output String[] (indexed by original row position)
   */
  public static void batchDecode(final MemorySegment[] pages, final int[] pageIndices, final int[] offsets,
      final int[] lengths, final boolean[] isCompressed, final int[] selectionVector, final int selectionCount,
      final byte[][][] symbolsByPage, final String[] output) {

    byte[] buffer = acquireBuffer(4096);
    try {
      for (int i = 0; i < selectionCount; i++) {
        final int row = selectionVector[i];
        if (output[row] != null) {
          continue; // already materialized
        }
        final int len = lengths[row];
        if (len <= 0) {
          output[row] = "";
          continue;
        }
        // Grow buffer if needed — use max with DEFAULT_BUFFER_SIZE to keep poolable
        if (buffer.length < len) {
          releaseBuffer(buffer);
          buffer = new byte[Math.max(len, DEFAULT_BUFFER_SIZE)];
        }
        final MemorySegment page = pages[pageIndices[row]];
        MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offsets[row], buffer, 0, len);

        final byte[][] symbols;
        if (isCompressed[row] && (symbols = symbolsByPage[pageIndices[row]]) != null && symbols.length > 0) {
          // Decode directly from buffer slice — no copy needed (headerless page data)
          final byte[] decoded = decodeRawCompressed(buffer, 0, len, symbols);
          output[row] = new String(decoded, StandardCharsets.UTF_8);
        } else {
          output[row] = new String(buffer, 0, len, StandardCharsets.UTF_8);
        }
      }
    } finally {
      releaseBuffer(buffer);
    }
  }

  /**
   * Two-byte-indexed longest-match over a symbol table, with an O(1) single-byte fallback — the
   * lookup shape of FSST proper, adapted to this table's greedy-longest semantics.
   *
   * <p>
   * Indexing by the first byte alone left four to eight candidates per position on text tables, and
   * the scan over them profiled as the whole cost of FSST ingest (longestMatch was the top
   * application frame after the page encoder). Two leading bytes discriminate text so well that a
   * bucket almost always holds zero or one live candidate; single-byte symbols — which terminate
   * every miss — move to a direct 256-entry code table instead of occupying buckets. The retained
   * index costs ~256 KiB per table; construction also needs two 256 KiB work arrays. Identity-cached
   * matchers use one-shot construction, while projection encoders retain all three arrays in an
   * owner-confined {@link Workspace} and reset them in place.
   *
   * <p>
   * Match results are packed into a long ({@code (length << 32) | code}); -1 means no symbol matches
   * and the byte must be escaped.
   */
  private static final class SymbolMatcher {
    /** Little-endian int view over byte[] for the four-byte prefix load in the match loop. */
    private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private byte[][] symbols;
    /** Training-only packed table; null for the generic immutable byte[][] matcher path. */
    private long[] packedSymbols;
    /** Exact lengths parallel to {@link #packedSymbols}. */
    private byte[] packedSymbolLengths;
    private int packedSymbolCount;
    /** Bucket boundaries into {@link #bucketSymbolIds}, indexed by the first TWO bytes, +1 end. */
    private final int[] bucketStart;
    /** Multi-byte symbol ids grouped by leading byte pair, longest-first within each bucket. */
    private final int[] bucketSymbolIds;
    /** First (up to) four symbol bytes little-endian-packed, parallel to {@link #bucketSymbolIds}. */
    private final int[] bucketPrefix4;
    /** Significant bytes of {@code bucketPrefix4} (min(len, 4)), parallel. */
    private final byte[] bucketPrefixLen;
    /** Code of the single-byte symbol for each byte value, or -1 — the O(1) miss terminator. */
    private final short[] singleByteCode;
    /** Reusable construction counters; null for identity-cached, one-shot matchers. */
    private final int[] countsScratch;
    /** Reusable construction cursors; null for identity-cached, one-shot matchers. */
    private final int[] cursorScratch;

    SymbolMatcher(final byte[][] symbols) {
      bucketStart = new int[65537];
      singleByteCode = new short[256];
      countsScratch = null;
      cursorScratch = null;
      final int[] counts = new int[65536];
      final int multiByte = countMultiByteSymbols(symbols, counts);
      bucketSymbolIds = new int[multiByte];
      bucketPrefix4 = new int[multiByte];
      bucketPrefixLen = new byte[multiByte];
      final int[] cursor = new int[65536];
      populateIndex(symbols, counts, cursor);
      this.symbols = symbols;
    }

    /** Reusable construction form owned by {@link Workspace}. */
    SymbolMatcher() {
      bucketStart = new int[65537];
      bucketSymbolIds = new int[MAX_SYMBOLS];
      bucketPrefix4 = new int[MAX_SYMBOLS];
      bucketPrefixLen = new byte[MAX_SYMBOLS];
      singleByteCode = new short[256];
      countsScratch = new int[65536];
      cursorScratch = new int[65536];
    }

    /** Prepare this reusable matcher for {@code newSymbols}, rebuilding only on identity change. */
    void prepare(final byte[][] newSymbols) {
      if (symbols != newSymbols) {
        reset(newSymbols);
      }
    }

    /**
     * Fully rebuild the reusable index. Every mutable primitive slot that affects lookup is reset, so
     * an A/B/A sequence and reuse after a failed reset cannot inherit stale bucket state.
     */
    void reset(final byte[][] newSymbols) {
      if (countsScratch == null || cursorScratch == null) {
        throw new IllegalStateException("Only a workspace matcher can be reset");
      }
      // Fail closed: until the rebuild completes, even the previous table identity is invalid.
      symbols = null;
      packedSymbols = null;
      packedSymbolLengths = null;
      packedSymbolCount = 0;
      Arrays.fill(countsScratch, 0);
      final int multiByte = countMultiByteSymbols(newSymbols, countsScratch);
      if (multiByte > bucketSymbolIds.length) {
        throw new IllegalArgumentException("symbol table has too many multi-byte symbols: " + multiByte);
      }
      Arrays.fill(cursorScratch, 0);
      populateIndex(newSymbols, countsScratch, cursorScratch);
      symbols = newSymbols;
    }

    /** Rebuild from the candidate workspace without materializing one byte[] per selected symbol. */
    void reset(final CandidateWorkspace candidates) {
      if (countsScratch == null || cursorScratch == null) {
        throw new IllegalStateException("Only a workspace matcher can be reset");
      }
      symbols = null;
      packedSymbols = null;
      packedSymbolLengths = null;
      packedSymbolCount = 0;
      Arrays.fill(countsScratch, 0);
      final int symbolCount = candidates.selectedCount();
      int multiByte = 0;
      for (int code = 0; code < symbolCount; code++) {
        final int length = candidates.selectedLength(code);
        if (length >= 2) {
          final long symbol = candidates.selectedPacked(code);
          countsScratch[(int) (((symbol & 0xFF) << 8) | ((symbol >>> 8) & 0xFF))]++;
          multiByte++;
        }
      }
      if (multiByte > bucketSymbolIds.length) {
        throw new IllegalArgumentException("symbol table has too many multi-byte symbols: " + multiByte);
      }
      Arrays.fill(cursorScratch, 0);
      populatePackedIndex(candidates, countsScratch, cursorScratch);
      packedSymbols = candidates.selectedPacked;
      packedSymbolLengths = candidates.selectedLengths;
      packedSymbolCount = symbolCount;
    }

    /** Drop the table reference without discarding the retained primitive arrays. */
    void clear() {
      symbols = null;
      packedSymbols = null;
      packedSymbolLengths = null;
      packedSymbolCount = 0;
    }

    private static int countMultiByteSymbols(final byte[][] symbols, final int[] counts) {
      Objects.requireNonNull(symbols, "symbols must not be null");
      if (symbols.length > MAX_SYMBOLS) {
        throw new IllegalArgumentException(
            "symbol table has " + symbols.length + " symbols; maximum is " + MAX_SYMBOLS);
      }
      int multiByte = 0;
      for (final byte[] symbol : symbols) {
        if (symbol == null || symbol.length == 0) {
          throw new IllegalArgumentException("symbols must be non-null and non-empty");
        }
        if (symbol.length >= 2) {
          counts[((symbol[0] & 0xFF) << 8) | (symbol[1] & 0xFF)]++;
          multiByte++;
        }
      }
      return multiByte;
    }

    private void populateIndex(final byte[][] newSymbols, final int[] counts, final int[] cursor) {
      Arrays.fill(singleByteCode, (short) -1);
      int running = 0;
      for (int b = 0; b < 65536; b++) {
        bucketStart[b] = running;
        running += counts[b];
      }
      bucketStart[65536] = running;
      // The symbols array arrives longest-first (the table's serialized order), so insertion
      // order preserves longest-first within each bucket.
      for (int code = 0; code < newSymbols.length; code++) {
        final byte[] symbol = newSymbols[code];
        if (symbol.length == 1) {
          singleByteCode[symbol[0] & 0xFF] = (short) code;
        } else if (symbol.length >= 2) {
          final int pair = ((symbol[0] & 0xFF) << 8) | (symbol[1] & 0xFF);
          final int slot = bucketStart[pair] + cursor[pair]++;
          bucketSymbolIds[slot] = code;
          final int prefixLen = Math.min(symbol.length, 4);
          bucketPrefixLen[slot] = (byte) prefixLen;
          bucketPrefix4[slot] = packPrefix(symbol, 0, prefixLen);
        }
      }
    }

    private void populatePackedIndex(final CandidateWorkspace candidates, final int[] counts, final int[] cursor) {
      Arrays.fill(singleByteCode, (short) -1);
      int running = 0;
      for (int b = 0; b < 65536; b++) {
        bucketStart[b] = running;
        running += counts[b];
      }
      bucketStart[65536] = running;
      for (int code = 0; code < candidates.selectedCount(); code++) {
        final long symbol = candidates.selectedPacked(code);
        final int length = candidates.selectedLength(code);
        if (length == 1) {
          singleByteCode[(int) symbol & 0xFF] = (short) code;
        } else {
          final int pair = (int) (((symbol & 0xFF) << 8) | ((symbol >>> 8) & 0xFF));
          final int slot = bucketStart[pair] + cursor[pair]++;
          bucketSymbolIds[slot] = code;
          final int prefixLength = Math.min(length, 4);
          bucketPrefixLen[slot] = (byte) prefixLength;
          bucketPrefix4[slot] = (int) symbol;
        }
      }
    }

    private static int packPrefix(final byte[] bytes, final int off, final int n) {
      int packed = 0;
      for (int i = 0; i < n; i++) {
        packed |= (bytes[off + i] & 0xFF) << (i * 8);
      }
      return packed;
    }

    /**
     * The longest symbol matching {@code input} at {@code pos}, reading no further than {@code end}
     * (exclusive — encodes operate on slices), packed; -1 when none does.
     */
    long longestMatch(final byte[] input, final int pos, final int end) {
      final int avail = end - pos;
      final int b0 = input[pos] & 0xFF;
      if (avail >= 2) {
        final int pair = (b0 << 8) | (input[pos + 1] & 0xFF);
        final int bucketEnd = bucketStart[pair + 1];
        int i = bucketStart[pair];
        if (i < bucketEnd) {
          // Input's next four bytes, packed once; masked per candidate width. A single
          // little-endian int load replaces the four-iteration shift/or loop in the common
          // avail >= 4 case — this runs once per position on text.
          final int inputPrefix = avail >= 4
              ? (int) INT_LE.get(input, pos)
              : packPrefix(input, pos, avail);
          do {
            final int code = bucketSymbolIds[i];
            final byte[] symbol = symbols[code];
            if (symbol.length <= avail) {
              final int prefixLen = bucketPrefixLen[i];
              final int mask = prefixLen == 4
                  ? -1
                  : (1 << (prefixLen * 8)) - 1;
              if ((inputPrefix & mask) == bucketPrefix4[i] && (symbol.length <= 4 || matchesTail(input, pos, symbol))) {
                return ((long) symbol.length << 32) | code;
              }
            }
            i++;
          } while (i < bucketEnd);
        }
      }
      final short single = singleByteCode[b0];
      return single >= 0
          ? (1L << 32) | single
          : -1L;
    }

    /** Training counterpart of {@link #longestMatch}; reads selected symbols from packed scratch. */
    long longestPackedMatch(final byte[] input, final int pos, final int end) {
      if (packedSymbols == null || packedSymbolLengths == null || packedSymbolCount == 0) {
        throw new IllegalStateException("Packed matcher is not prepared");
      }
      final int available = end - pos;
      final int firstByte = input[pos] & 0xFF;
      if (available >= 2) {
        final int pair = (firstByte << 8) | (input[pos + 1] & 0xFF);
        final int bucketEnd = bucketStart[pair + 1];
        int candidate = bucketStart[pair];
        if (candidate < bucketEnd) {
          final int inputPrefix = available >= 4
              ? (int) INT_LE.get(input, pos)
              : packPrefix(input, pos, available);
          do {
            final int code = bucketSymbolIds[candidate];
            final int symbolLength = packedSymbolLengths[code] & 0xFF;
            if (symbolLength <= available) {
              final int prefixLength = bucketPrefixLen[candidate];
              final int prefixMask = prefixLength == 4
                  ? -1
                  : (1 << (prefixLength << 3)) - 1;
              final long symbol = packedSymbols[code];
              if ((inputPrefix & prefixMask) == bucketPrefix4[candidate]
                  && (symbolLength <= 4 || matchesPackedTail(input, pos, symbol, symbolLength))) {
                return ((long) symbolLength << 32) | code;
              }
            }
            candidate++;
          } while (candidate < bucketEnd);
        }
      }
      final short single = singleByteCode[firstByte];
      return single >= 0
          ? (1L << 32) | single
          : -1L;
    }

    /** Byte-compare from the fifth byte on; the packed prefix already covered the first four. */
    private static boolean matchesTail(final byte[] input, final int pos, final byte[] symbol) {
      for (int i = 4; i < symbol.length; i++) {
        if (input[pos + i] != symbol[i]) {
          return false;
        }
      }
      return true;
    }

    private static boolean matchesPackedTail(final byte[] input, final int pos, final long symbol,
        final int symbolLength) {
      for (int i = 4; i < symbolLength; i++) {
        if (input[pos + i] != (byte) (symbol >>> (i << 3))) {
          return false;
        }
      }
      return true;
    }

    static int matchLength(final long match) {
      return (int) (match >>> 32);
    }

    static int matchCode(final long match) {
      return (int) match;
    }
  }

  /** Immutable byte sequence for use as map key. */
  private static final class ByteSequence {
    private final byte[] data;
    private final int length;

    ByteSequence(byte[] source, int offset, int length) {
      this.data = Arrays.copyOfRange(source, offset, offset + length);
      this.length = length;
    }

    byte[] data() {
      return data;
    }

    int length() {
      return length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ByteSequence that))
        return false;
      return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }
}
