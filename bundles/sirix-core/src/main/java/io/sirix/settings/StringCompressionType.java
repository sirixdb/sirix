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

package io.sirix.settings;

/**
 * Compression type for string values in nodes (StringNode, ObjectStringNode, TextNode, etc.).
 * 
 * <p>
 * Controls how string content is compressed when stored on disk:
 * <ul>
 * <li>{@link #NONE} - No per-string compression; rely on page-level LZ4 only</li>
 * <li>{@link #FSST} - Fast Static Symbol Table compression; best for similar strings</li>
 * </ul>
 * 
 * <p>
 * Note: Page-level compression (e.g., LZ4) still applies regardless of this setting. This setting
 * controls additional per-string or columnar compression.
 * 
 * @author Johannes Lichtenberger
 */
public enum StringCompressionType {

  /**
   * No per-string compression.
   * <p>
   * Strings are stored as raw bytes. Page-level LZ4 compression still applies. This is the default
   * and safest option.
   * 
   * <p>
   * Pros:
   * <ul>
   * <li>No CPU overhead for encode/decode</li>
   * <li>True zero-copy reads (no decompression step)</li>
   * <li>Best for already-compressed or high-entropy content</li>
   * </ul>
   * 
   * <p>
   * Cons:
   * <ul>
   * <li>Larger on-disk size for compressible text</li>
   * </ul>
   */
  NONE((byte) 0),

  /**
   * Fast Static Symbol Table (FSST) compression.
   * <p>
   * Builds a symbol table from similar strings (e.g., JSON values on a page) and replaces common byte
   * sequences with 1-byte codes.
   * 
   * <p>
   * Pros:
   * <ul>
   * <li>Very fast decompression (~1-2 GB/s)</li>
   * <li>Works well for repetitive/similar strings (JSON, XML text)</li>
   * <li>Synergizes with page-level LZ4 for better overall compression</li>
   * </ul>
   * 
   * <p>
   * Cons:
   * <ul>
   * <li>Requires decompression on read (not true zero-copy)</li>
   * <li>Symbol table overhead (stored per page)</li>
   * <li>Less effective for random/high-entropy content</li>
   * </ul>
   */
  FSST((byte) 1);

  private final byte id;

  StringCompressionType(byte id) {
    this.id = id;
  }

  /**
   * Get the byte ID for serialization.
   * 
   * @return the ID byte
   */
  public byte getId() {
    return id;
  }

  /**
   * Get the StringCompressionType from its byte ID.
   * 
   * @param id the byte ID
   * @return the corresponding StringCompressionType
   * @throws IllegalArgumentException if id is unknown
   */
  public static StringCompressionType fromId(byte id) {
    return switch (id) {
      case 0 -> NONE;
      case 1 -> FSST;
      default -> throw new IllegalArgumentException("Unknown StringCompressionType id: " + id);
    };
  }

  /**
   * Check if this compression type requires decompression on read.
   * 
   * @return true if strings need to be decompressed when read
   */
  public boolean requiresDecompression() {
    return this != NONE;
  }
}

