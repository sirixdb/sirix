/**
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.settings;

/**
 * How PAX region payloads are compressed on the wire.
 *
 * <p>This is a speed/size dial, and it exists because the region table is where the two goals
 * pull apart. Everything else the page encoder does is lightweight, data-aware encoding in the
 * BtrBlocks/Data Blocks tradition — structural-key columns, dictionary + bit-packed regions,
 * hash/name-key/value elision — which costs near nothing at ingest because the encoder knows
 * exactly what each byte is. The string region's <em>values</em>, though, are mostly-distinct
 * text on real-world data: dictionaries cannot dedup them, so the region is written large, and
 * the only general-purpose lever left is byte compression, which is the one encode whose cost is
 * proportional to the data rather than to its structure.
 *
 * <p>Measured on the 176 MB reference ingest: {@link #LZ77} shrinks the database 169 → 107 MB
 * and costs ~13% ingest time (best-of-5 2.53–2.99 s → 3.33–3.37 s); {@link #NONE} is that trade
 * refused. The wire format is self-describing per payload (each carries a codec byte), so
 * databases written under either setting are readable regardless of the current one — the
 * setting only chooses what future commits write.
 *
 * <p>The planned lightweight replacement for text regions is FSST against the per-revision
 * symbol table already stored in the name dictionary's trie — symbol-level, data-aware, and
 * scan-friendly — at which point it joins this enum and LZ77 goes back to being a fallback.
 */
public enum RegionCompressionType {

  /**
   * Region payloads are written raw. Fastest ingest; the string region dominates database size
   * on string-heavy data.
   */
  NONE((byte) 0),

  /**
   * Region payloads of 64 bytes or more elect LZ77 per payload, kept only when strictly smaller
   * than raw. Reads decompress once per page load into the cached in-memory arrays, so scans
   * are unaffected; ingest pays the encode.
   */
  LZ77((byte) 1);

  private final byte id;

  RegionCompressionType(final byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }
}
