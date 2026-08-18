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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
  private static final java.util.ArrayDeque<byte[]> BUFFER_POOL = new java.util.ArrayDeque<>(BUFFER_POOL_SIZE);

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
      byte[] encoded = encode(sample, parsedSymbols);
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

    final List<ByteSequence> symbols = buildSymbolTableIteratively(corpus);
    if (symbols.isEmpty()) {
      return new byte[0];
    }

    // Serialize symbol table
    return serializeSymbolTable(symbols);
  }

  /**
   * FSST-style iterative table construction (after Boncz, Neumann and Leis, "FSST: Fast Random
   * Access String Compression", VLDB 2020 — simplified to this encoder's escape scheme).
   *
   * <p>One frequency pass over raw bytes cannot find good symbols: counting every 1..8-byte
   * window inflates substrings of frequent strings, and it scores text the encoder will never
   * stand on — after a symbol matches, the encoder is at a different position than the sliding
   * window assumed. FSST's insight is to make the table a fixed point of the encoder itself:
   * encode the corpus with the current table, credit what was actually emitted, credit the
   * concatenation of each adjacent token pair (capped at the wire's symbol width) so that useful
   * symbols can grow — "htt" then "p:/" proposes "http:/" — and rebuild from the highest-gain
   * candidates. A few iterations converge; symbols that stop earning their slot fall out.
   *
   * <p>Gains use this encoder's real cost model: an unmatched byte escapes to TWO bytes, so an
   * emitted length-L symbol saves {@code 2L - 1} per use, and a frequent single byte always
   * deserves a code (saving 1 per use) — which is also why the seed table is the byte histogram.
   */
  private static List<ByteSequence> buildSymbolTableIteratively(final List<byte[]> corpus) {
    // Seed histogram over a flat int[256] — one map entry per DISTINCT byte value, not one
    // hash probe plus wrapper allocation per corpus byte.
    final int[] byteCounts = new int[256];
    for (final byte[] sample : corpus) {
      for (final byte b : sample) {
        byteCounts[b & 0xFF]++;
      }
    }
    Object2IntOpenHashMap<ByteSequence> gains = new Object2IntOpenHashMap<>();
    for (int b = 0; b < 256; b++) {
      if (byteCounts[b] > 0) {
        gains.put(new ByteSequence(new byte[] { (byte) b }, 0, 1), byteCounts[b]);
      }
    }
    List<ByteSequence> table = topByGain(gains);

    for (int iteration = 0; iteration < TABLE_BUILD_ITERATIONS; iteration++) {
      final SymbolMatcher matcher = new SymbolMatcher(toArrays(table));
      gains = new Object2IntOpenHashMap<>();
      for (final byte[] sample : corpus) {
        int pos = 0;
        int prevStart = -1;
        int prevLen = 0;
        while (pos < sample.length) {
          final long match = matcher.longestMatch(sample, pos, sample.length);
          final int len = match >= 0 ? SymbolMatcher.matchLength(match) : 1;
          gains.addTo(new ByteSequence(sample, pos, len), match >= 0 ? (2 * len - 1) : 1);
          if (prevStart >= 0) {
            final int concatLen = Math.min(prevLen + len, MAX_SYMBOL_LENGTH);
            if (concatLen > prevLen && prevStart + concatLen <= sample.length) {
              gains.addTo(new ByteSequence(sample, prevStart, concatLen), 2 * concatLen - 1);
            }
          }
          prevStart = pos;
          prevLen = len;
          pos += len;
        }
      }
      table = topByGain(gains);
    }
    return table;
  }

  /** Fixed-point iterations for {@link #buildSymbolTableIteratively}; FSST proper uses five. */
  private static final int TABLE_BUILD_ITERATIONS = 5;

  private static byte[][] toArrays(final List<ByteSequence> table) {
    final byte[][] out = new byte[table.size()][];
    for (int i = 0; i < table.size(); i++) {
      out[i] = table.get(i).data();
    }
    return out;
  }

  private static List<ByteSequence> topByGain(final Object2IntOpenHashMap<ByteSequence> gains) {
    final List<Object2IntMap.Entry<ByteSequence>> entries =
        new ArrayList<>(gains.object2IntEntrySet());
    entries.sort((a, b) -> Integer.compare(b.getIntValue(), a.getIntValue()));
    final List<ByteSequence> selected = new ArrayList<>(Math.min(entries.size(), MAX_SYMBOLS));
    for (final var entry : entries) {
      if (selected.size() >= MAX_SYMBOLS) {
        break;
      }
      // A symbol must earn at least two uses' worth of gain to keep a slot. One use proves
      // nothing beyond "this exact string occurred once in the corpus" — a length-L symbol
      // seen once carries gain 2L-1 from that single sighting, and admitting it spends one
      // of 255 codes memorizing a sample instead of generalizing. Requiring two sightings'
      // gain (2 for singles, 2*(2L-1) for multi-byte) keeps slots for patterns that repeat.
      final int len = entry.getKey().length();
      final int minGain = len == 1 ? 2 : 2 * (2 * len - 1);
      if (entry.getIntValue() < minGain) {
        continue;
      }
      selected.add(entry.getKey());
    }
    // Longest-first order in the serialized table, so greedy consumers see long symbols first.
    selected.sort(Comparator.comparingInt(ByteSequence::length).reversed());
    return selected;
  }

  /**
   * Serialize symbol table to bytes. Format:
   * [numSymbols:1][len1:1][len2:1]...[symbol1:len1][symbol2:len2]...
   */
  private static byte[] serializeSymbolTable(final List<ByteSequence> symbols) {
    // Calculate total size
    int totalSize = TABLE_HEADER_SIZE + symbols.size(); // numSymbols + length bytes
    for (final ByteSequence seq : symbols) {
      totalSize += seq.length();
    }

    final byte[] table = new byte[totalSize];
    int pos = 0;

    // Number of symbols
    table[pos++] = (byte) symbols.size();

    // Symbol lengths
    for (final ByteSequence seq : symbols) {
      table[pos++] = (byte) seq.length();
    }

    // Symbol data
    for (final ByteSequence seq : symbols) {
      System.arraycopy(seq.data(), 0, table, pos, seq.length());
      pos += seq.length();
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
   * Encode a slice of {@code input}, or return {@code null} when the encoding would not shrink
   * it. Built for the store-if-smaller call sites (insert-time and commit-time compression),
   * which only ever keep the beneficial outcome: handing them a raw-headered copy to discard —
   * what {@link #encode(byte[], byte[][])} produces — was one wasted allocation plus memcpy per
   * incompressible string, and materializing parser-buffer slices into arrays first was
   * another per string at ingest rate.
   *
   * @param input data to compress
   * @param off start of the value within {@code input}
   * @param len value length
   * @param parsedSymbols pre-parsed symbol table from {@link #parseSymbolTable(byte[])}
   * @return headered compressed bytes strictly shorter than {@code len}, or {@code null} to
   *         store the value raw
   */
  public static byte[] encodeOrNull(final byte[] input, final int off, final int len,
      final byte[][] parsedSymbols) {
    Objects.requireNonNull(input, "input must not be null");

    if (parsedSymbols == null || parsedSymbols.length == 0) {
      return null;
    }

    return encodeBeneficialOrNull(input, off, len, parsedSymbols);
  }

  /**
   * Largest encode scratch retained across calls; anything bigger is a one-off allocation. An
   * uncapped scratch grows to twice the largest value a thread ever encodes and pins that
   * memory for the thread's life — one 50 MB text value must not cost 100 MB per pool worker
   * forever.
   */
  private static final int MAX_RETAINED_SCRATCH = 1 << 20;

  /** Per-thread encode output scratch; grows with demand up to {@link #MAX_RETAINED_SCRATCH}. */
  private static final ThreadLocal<byte[]> ENCODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[8 * 1024]);

  /** Entries in each identity-keyed ring cache below. */
  private static final int RING_ENTRIES = 8;

  /**
   * A small identity-keyed ring: key/value pairs plus a primitive insertion cursor (an
   * {@code Object[]} slot would box the cursor on every insert). Eight entries because a
   * single-entry cache thrashes the moment a thread alternates between two tables — exactly
   * what happens when a commit combines pages bound to an old table with pages on the new one,
   * or when a query scans column segments carrying different tables.
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
   * Per-thread identity ring from parsed tables to their {@link SymbolMatcher}. Reuse contract:
   * the caller-facing API passes a parsed {@code byte[][]} per call, and every page-, trial-
   * and combine-level flow passes the SAME array instance for a whole batch of values, so one
   * identity check replaces rebuilding the bucket index per string.
   */
  private static final ThreadLocal<Ring> MATCHER_CACHE = ThreadLocal.withInitial(Ring::new);

  /**
   * Identity ring from serialized table bytes to their parsed form. The revision build hands
   * every page of a commit the SAME table byte array, so parsing per page — and therefore
   * rebuilding the matcher per page, since the matcher cache keys on the parsed array's
   * identity — was pure waste multiplied by thousands of pages.
   */
  private static final ThreadLocal<Ring> PARSED_CACHE = ThreadLocal.withInitial(Ring::new);

  /**
   * Parse {@code tableBytes}, reusing a recent result when the same array instance is asked
   * for again on this thread.
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
   * Inputs at or below this length take the linear-scan encode path when no matcher is cached
   * for the table yet. Building a {@link SymbolMatcher} allocates and zeroes ~½ MB of bucket
   * index — sound when a whole page or commit amortizes it, absurd for encoding one short
   * probe value (a query filter constant, a single trial string) against a table this thread
   * has never batch-encoded with.
   */
  private static final int SMALL_INPUT_LINEAR_LIMIT = 256;

  /**
   * Consecutive small-input linear encodes against the same table before this thread builds
   * the matcher anyway. Bulk flows are dominated by strings under
   * {@link #SMALL_INPUT_LINEAR_LIMIT} — JSON values average well below it — so without
   * promotion a thread that only ever sees small strings would take the linear scan forever,
   * reinstating the encode-cost-proportional-to-table-size pathology the matcher was built to
   * kill. A handful of one-off probes stay cheap; the fifth encode against the same table is
   * evidence of a batch, and one matcher build amortizes across everything that follows.
   */
  private static final int LINEAR_PROMOTION_THRESHOLD = 4;

  /** Per-thread linear-encode streak: the table identity and a primitive run length. */
  private static final class LinearStreak {
    byte[][] table;
    int count;
  }

  private static final ThreadLocal<LinearStreak> LINEAR_STREAK =
      ThreadLocal.withInitial(LinearStreak::new);

  /** Whether this thread has linear-encoded against {@code symbols} often enough to justify a matcher. */
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
   * Inputs longer than this store raw: the escape-worst-case doubling would overflow an
   * int-indexed array, and a value of this size has no business in a string region anyway.
   */
  private static final int MAX_ENCODABLE_LENGTH = (Integer.MAX_VALUE - 8) / 2;

  private static byte[] encodeWithParsedSymbols(final byte[] input, final int off, final int len,
      final byte[][] symbols) {
    final byte[] beneficial = encodeBeneficialOrNull(input, off, len, symbols);
    return beneficial != null ? beneficial : markAsRaw(input, off, len);
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
   * longest-first (see {@link #topByGain}), so the first symbol that matches IS the longest
   * match — same result as {@link SymbolMatcher#longestMatch}, none of its index cost.
   */
  private static byte[] encodeLinear(final byte[] input, final int off, final int len,
      final byte[][] symbols) {
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
   * The per-thread scratch, grown (with overflow-safe arithmetic) to hold 2x
   * {@code inputLength}. Demand beyond {@link #MAX_RETAINED_SCRATCH} gets a one-off array that
   * is NOT stored back — one oversized value must not pin megabytes on the thread forever.
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
   * Headered result copied out of the shared scratch, or {@code null} when the encoding did
   * not beat raw-plus-header — the caller decides whether "not beneficial" means a raw-marked
   * copy or no bytes at all.
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
   * Decode FSST-compressed data from a slice of a byte array.
   * Avoids the need to copy the slice into a separate array first.
   *
   * @param data    the byte array containing encoded data
   * @param offset  start offset within data
   * @param length  number of bytes to decode
   * @param symbols pre-parsed FSST symbol table
   * @return decoded byte array
   */
  private static byte[] decodeWithParsedSymbols(final byte[] data, final int offset,
      final int length, final byte[][] symbols) {
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
   * Decode headerless FSST-compressed data from a slice of a byte array.
   * Unlike {@link #decodeWithParsedSymbols}, this method does NOT expect a header byte —
   * the data starts directly with compressed symbol codes and escape sequences.
   *
   * <p>Use this for page-extracted compressed payloads where the on-page format is
   * {@code [isCompressed:1byte][length:varint][payload_bytes]} and the payload contains
   * raw compressed bytes without the FSST header.</p>
   *
   * @param data    the byte array containing raw compressed data (no header)
   * @param offset  start offset within data
   * @param length  number of bytes to decode
   * @param symbols pre-parsed FSST symbol table
   * @return decoded byte array
   */
  static byte[] decodeRawCompressed(final byte[] data, final int offset,
      final int length, final byte[][] symbols) {
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
   * @param data the buffer holding the encoded value (header byte first, as {@link #encode} writes it)
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
          throw new IllegalStateException(
              "Corrupted FSST symbol table: symbol " + b + " is " + len + " bytes, over the " + MAX_SYMBOL_LENGTH
                  + "-byte maximum");
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
   * Decode headerless FSST-compressed data using pre-parsed symbols.
   * For page-extracted compressed payloads that do NOT have the FSST header byte.
   *
   * <p>Pages store compressed string values as {@code [isCompressed:1byte][length:varint][payload_bytes]}
   * where {@code payload_bytes} are the raw compressed bytes without the FSST header.
   * Use this method (instead of {@link #decode(byte[], byte[][])}) when decoding such payloads.</p>
   *
   * @param data    headerless compressed byte array
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
   * Batch-decode FSST values from MemorySegments, only for selected rows.
   * Each row may reference a different page (multi-page batches).
   * Reuses a single decode buffer to minimize allocation.
   *
   * @param pages           array of backing page MemorySegments
   * @param pageIndices     per-row page index into pages array
   * @param offsets         per-row absolute byte offset in the page
   * @param lengths         per-row compressed value byte length
   * @param isCompressed    per-row compression flag
   * @param selectionVector indices of rows to decode
   * @param selectionCount  number of valid entries in selectionVector
   * @param symbolsByPage   per-page pre-parsed symbol tables (null entry = no FSST)
   * @param output          output String[] (indexed by original row position)
   */
  public static void batchDecode(
      final MemorySegment[] pages, final int[] pageIndices,
      final int[] offsets, final int[] lengths, final boolean[] isCompressed,
      final int[] selectionVector, final int selectionCount,
      final byte[][][] symbolsByPage,
      final String[] output) {

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
        if (isCompressed[row]
            && (symbols = symbolsByPage[pageIndices[row]]) != null
            && symbols.length > 0) {
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
   * Two-byte-indexed longest-match over a symbol table, with an O(1) single-byte fallback —
   * the lookup shape of FSST proper, adapted to this table's greedy-longest semantics.
   *
   * <p>Indexing by the first byte alone left four to eight candidates per position on text
   * tables, and the scan over them profiled as the whole cost of FSST ingest (longestMatch was
   * the top application frame after the page encoder). Two leading bytes discriminate text so
   * well that a bucket almost always holds zero or one live candidate; single-byte symbols —
   * which terminate every miss — move to a direct 256-entry code table instead of occupying
   * buckets. The index costs ~600 KB per table and is built once per commit thanks to the
   * matcher identity cache, so construction cost is irrelevant next to per-byte lookup cost.
   *
   * <p>Match results are packed into a long ({@code (length << 32) | code}); -1 means no symbol
   * matches and the byte must be escaped.
   */
  private static final class SymbolMatcher {
    /** Little-endian int view over byte[] for the four-byte prefix load in the match loop. */
    private static final VarHandle INT_LE =
        MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final byte[][] symbols;
    /** Bucket boundaries into {@link #bucketSymbolIds}, indexed by the first TWO bytes, +1 end. */
    private final int[] bucketStart = new int[65537];
    /** Multi-byte symbol ids grouped by leading byte pair, longest-first within each bucket. */
    private final int[] bucketSymbolIds;
    /** First (up to) four symbol bytes little-endian-packed, parallel to {@link #bucketSymbolIds}. */
    private final int[] bucketPrefix4;
    /** Significant bytes of {@code bucketPrefix4} (min(len, 4)), parallel. */
    private final byte[] bucketPrefixLen;
    /** Code of the single-byte symbol for each byte value, or -1 — the O(1) miss terminator. */
    private final short[] singleByteCode = new short[256];

    SymbolMatcher(final byte[][] symbols) {
      this.symbols = symbols;
      Arrays.fill(singleByteCode, (short) -1);
      final int[] counts = new int[65536];
      int multiByte = 0;
      for (final byte[] symbol : symbols) {
        if (symbol.length >= 2) {
          counts[((symbol[0] & 0xFF) << 8) | (symbol[1] & 0xFF)]++;
          multiByte++;
        }
      }
      int running = 0;
      for (int b = 0; b < 65536; b++) {
        bucketStart[b] = running;
        running += counts[b];
      }
      bucketStart[65536] = running;
      bucketSymbolIds = new int[multiByte];
      bucketPrefix4 = new int[multiByte];
      bucketPrefixLen = new byte[multiByte];
      final int[] cursor = new int[65536];
      // The symbols array arrives longest-first (the table's serialized order), so insertion
      // order preserves longest-first within each bucket.
      for (int code = 0; code < symbols.length; code++) {
        final byte[] symbol = symbols[code];
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

    private static int packPrefix(final byte[] bytes, final int off, final int n) {
      int packed = 0;
      for (int i = 0; i < n; i++) {
        packed |= (bytes[off + i] & 0xFF) << (i * 8);
      }
      return packed;
    }

    /**
     * The longest symbol matching {@code input} at {@code pos}, reading no further than
     * {@code end} (exclusive — encodes operate on slices), packed; -1 when none does.
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
              final int mask = prefixLen == 4 ? -1 : (1 << (prefixLen * 8)) - 1;
              if ((inputPrefix & mask) == bucketPrefix4[i]
                  && (symbol.length <= 4 || matchesTail(input, pos, symbol))) {
                return ((long) symbol.length << 32) | code;
              }
            }
            i++;
          } while (i < bucketEnd);
        }
      }
      final short single = singleByteCode[b0];
      return single >= 0 ? (1L << 32) | single : -1L;
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

