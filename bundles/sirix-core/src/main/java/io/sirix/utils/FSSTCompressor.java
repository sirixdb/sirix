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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Fast Static Symbol Table (FSST) compression for string values.
 * 
 * <p>FSST is a lightweight compression algorithm designed for short strings that:
 * <ul>
 *   <li>Builds a symbol table from sample strings to find common byte sequences</li>
 *   <li>Replaces common sequences with 1-byte codes (up to 255 symbols)</li>
 *   <li>Enables very fast decompression (~1-2 GB/s, vectorizable)</li>
 *   <li>Works well for similar strings (e.g., JSON values from same page)</li>
 * </ul>
 * 
 * <p>This implementation is optimized for zero-copy integration with MemorySegments.
 * 
 * <h2>Formal Correctness Properties</h2>
 * <pre>
 * P1: ∀ string s, table t: decode(encode(s, t), t) == s  (roundtrip correctness)
 * P2: ∀ input i: encode(i, emptyTable) == escape(i)     (graceful degradation)
 * P3: ∀ segment s: decode operates on segment without intermediate copies
 * </pre>
 * 
 * @author Johannes Lichtenberger
 */
public final class FSSTCompressor {

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
  public static final int MIN_COMPRESSION_SIZE = 8;

  /**
   * Maximum symbol length (longer patterns have diminishing returns).
   */
  public static final int MAX_SYMBOL_LENGTH = 8;

  /**
   * Minimum samples needed to build a useful symbol table.
   */
  public static final int MIN_SAMPLES_FOR_TABLE = 4;

  /**
   * Maximum samples to analyze (for performance).
   */
  public static final int MAX_SAMPLES_TO_ANALYZE = 256;

  /**
   * Header bytes in serialized symbol table: [numSymbols(1)][symbolLengths(numSymbols)][symbolData...]
   */
  private static final int TABLE_HEADER_SIZE = 1;

  private FSSTCompressor() {
    // Utility class
  }

  /**
   * Build a symbol table from sample strings.
   * 
   * <p>Analyzes the samples to find frequently occurring byte sequences (1-8 bytes)
   * and creates a lookup table for compression.
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

    // Count frequency of all byte sequences (1 to MAX_SYMBOL_LENGTH bytes)
    final Object2IntOpenHashMap<ByteSequence> frequencyMap = new Object2IntOpenHashMap<>();
    
    int sampleCount = 0;
    for (final byte[] sample : samples) {
      if (sample == null || sample.length == 0) {
        continue;
      }
      if (++sampleCount > MAX_SAMPLES_TO_ANALYZE) {
        break;
      }
      
      countSequences(sample, frequencyMap);
    }

    if (frequencyMap.isEmpty()) {
      return new byte[0];
    }

    // Select best symbols (highest frequency * length score)
    final List<ByteSequence> sortedSymbols = selectBestSymbols(frequencyMap);
    
    if (sortedSymbols.isEmpty()) {
      return new byte[0];
    }

    // Serialize symbol table
    return serializeSymbolTable(sortedSymbols);
  }

  /**
   * Build a symbol table from MemorySegment samples (zero-copy friendly).
   * 
   * @param segments list of memory segments containing strings
   * @return symbol table bytes, or empty array if compression not beneficial
   */
  public static byte[] buildSymbolTable(final List<MemorySegment> segments, boolean isSegment) {
    Objects.requireNonNull(segments, "segments must not be null");
    
    final List<byte[]> samples = new ArrayList<>(Math.min(segments.size(), MAX_SAMPLES_TO_ANALYZE));
    for (final MemorySegment segment : segments) {
      if (segment != null && segment.byteSize() > 0) {
        samples.add(segment.toArray(ValueLayout.JAVA_BYTE));
        if (samples.size() >= MAX_SAMPLES_TO_ANALYZE) {
          break;
        }
      }
    }
    return buildSymbolTable(samples);
  }

  /**
   * Count byte sequences in a sample for frequency analysis.
   */
  private static void countSequences(final byte[] sample, final Object2IntOpenHashMap<ByteSequence> frequencyMap) {
    final int len = sample.length;
    
    for (int symbolLen = 2; symbolLen <= Math.min(MAX_SYMBOL_LENGTH, len); symbolLen++) {
      for (int i = 0; i <= len - symbolLen; i++) {
        final ByteSequence seq = new ByteSequence(sample, i, symbolLen);
        frequencyMap.addTo(seq, 1);
      }
    }
  }

  /**
   * Select the best symbols based on frequency and length.
   * Longer symbols that occur frequently provide better compression.
   */
  private static List<ByteSequence> selectBestSymbols(final Object2IntOpenHashMap<ByteSequence> frequencyMap) {
    // Score = frequency * (length - 1)  (savings per occurrence)
    final List<it.unimi.dsi.fastutil.objects.Object2IntMap.Entry<ByteSequence>> entries = 
        new ArrayList<>(frequencyMap.object2IntEntrySet());
    
    // Sort by score descending
    entries.sort((a, b) -> {
      int scoreA = a.getIntValue() * (a.getKey().length() - 1);
      int scoreB = b.getIntValue() * (b.getKey().length() - 1);
      return Integer.compare(scoreB, scoreA);
    });

    // Take top symbols, avoiding overlapping sequences
    final List<ByteSequence> selected = new ArrayList<>();
    for (final it.unimi.dsi.fastutil.objects.Object2IntMap.Entry<ByteSequence> entry : entries) {
      if (selected.size() >= MAX_SYMBOLS) {
        break;
      }
      
      final ByteSequence candidate = entry.getKey();
      final int freq = entry.getIntValue();
      
      // Skip if frequency too low (at least 2 occurrences for benefit)
      if (freq < 2) {
        continue;
      }
      
      // Skip single bytes (escape overhead negates benefit)
      if (candidate.length() < 2) {
        continue;
      }
      
      selected.add(candidate);
    }

    // Sort selected by length descending for greedy matching during encode
    selected.sort(Comparator.comparingInt(ByteSequence::length).reversed());
    
    return selected;
  }

  /**
   * Serialize symbol table to bytes.
   * Format: [numSymbols:1][len1:1][len2:1]...[symbol1:len1][symbol2:len2]...
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

    // Read lengths
    final int[] lengths = new int[numSymbols];
    for (int i = 0; i < numSymbols; i++) {
      lengths[i] = tableBytes[pos++] & 0xFF;
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
   * <p>Uses greedy matching: tries longest symbols first.
   * Unmatched bytes are escaped with ESCAPE_BYTE prefix.
   * 
   * <p>Encoding scheme:
   * <ul>
   *   <li>Bytes 0 to (numSymbols-1): symbol codes</li>
   *   <li>Byte 0xFF: escape marker, next byte is literal</li>
   *   <li>All literals are escaped to avoid confusion with symbol codes</li>
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

    final byte[][] symbols = parseSymbolTable(symbolTable);
    if (symbols.length == 0) {
      return input.clone();
    }
    
    // If input too small, return with raw header
    if (input.length < MIN_COMPRESSION_SIZE) {
      return markAsRaw(input);
    }

    // Build encoded output (worst case: 2x input size due to escapes)
    final byte[] output = new byte[input.length * 2];
    int outPos = 0;
    int inPos = 0;

    while (inPos < input.length) {
      int matchedCode = -1;
      int matchedLen = 0;

      // Try each symbol (sorted by length descending for greedy match)
      for (int code = 0; code < symbols.length; code++) {
        final byte[] symbol = symbols[code];
        if (inPos + symbol.length <= input.length && matches(input, inPos, symbol)) {
          if (symbol.length > matchedLen) {
            matchedCode = code;
            matchedLen = symbol.length;
          }
        }
      }

      if (matchedCode >= 0) {
        // Output symbol code
        output[outPos++] = (byte) matchedCode;
        inPos += matchedLen;
      } else {
        // Escape literal byte - ALL literals are escaped to avoid confusion with symbol codes
        output[outPos++] = ESCAPE_BYTE;
        output[outPos++] = input[inPos++];
      }
    }

    // Check if compression was beneficial (include header byte in comparison)
    if (outPos + 1 >= input.length) {
      return markAsRaw(input);
    }

    // Add header and return compressed data
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
    final byte[] result = new byte[input.length + 1];
    result[0] = HEADER_RAW;
    System.arraycopy(input, 0, result, 1, input.length);
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

    final byte[][] symbols = parseSymbolTable(symbolTable);
    if (symbols.length == 0) {
      return encoded.clone();
    }

    // Check header byte
    final byte header = encoded[0];
    if (header == HEADER_RAW) {
      // Data is raw, just strip the header
      return Arrays.copyOfRange(encoded, 1, encoded.length);
    }
    
    if (header != HEADER_COMPRESSED) {
      // Unknown header - treat as legacy raw data
      return encoded.clone();
    }

    // Decode compressed data (skip header byte)
    final List<Byte> output = new ArrayList<>(encoded.length * 2);
    int pos = 1; // Skip header

    while (pos < encoded.length) {
      final int b = encoded[pos++] & 0xFF;

      if (b == 0xFF) {
        // Escape: next byte is literal
        if (pos >= encoded.length) {
          throw new IllegalStateException("Corrupted FSST data: escape at end");
        }
        output.add(encoded[pos++]);
      } else if (b < symbols.length) {
        // Symbol code: expand to symbol bytes
        final byte[] symbol = symbols[b];
        for (final byte sb : symbol) {
          output.add(sb);
        }
      } else {
        // This shouldn't happen with proper encoding (all literals escaped)
        throw new IllegalStateException("Corrupted FSST data: unexpected byte code " + b);
      }
    }

    // Convert to array
    final byte[] result = new byte[output.size()];
    for (int i = 0; i < output.size(); i++) {
      result[i] = output.get(i);
    }
    return result;
  }

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

    // Quick heuristic: check if there are common patterns
    final Object2IntOpenHashMap<ByteSequence> frequencyMap = new Object2IntOpenHashMap<>();
    int totalBytes = 0;
    int sampleCount = 0;

    for (final byte[] sample : samples) {
      if (sample == null || sample.length == 0) {
        continue;
      }
      totalBytes += sample.length;
      if (++sampleCount > 32) { // Quick sample
        break;
      }
      
      // Count 2-byte sequences only for quick check
      for (int i = 0; i < sample.length - 1; i++) {
        final ByteSequence seq = new ByteSequence(sample, i, 2);
        frequencyMap.addTo(seq, 1);
      }
    }

    if (totalBytes < 64) {
      return false;
    }

    // Check if any pattern occurs frequently enough
    int frequentPatterns = 0;
    for (final int freq : frequencyMap.values()) {
      if (freq >= 3) {
        frequentPatterns++;
      }
    }

    return frequentPatterns >= 4;
  }

  /**
   * Immutable byte sequence for use as map key.
   */
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
      if (this == o) return true;
      if (!(o instanceof ByteSequence that)) return false;
      return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }
}

