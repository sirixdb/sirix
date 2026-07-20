/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Self-describing metadata payload persisted alongside projection leaves
 * (slot 0 of the HOT sub-tree, leaves at slots 1..{@link #leafCount()}): the
 * projection's root path, per-column field paths, column names, and column
 * kinds. Hydration reads the projection's shape from HERE instead of
 * trusting the caller's argument list — without it, a re-create with a
 * same-arity but different field list would silently install the persisted
 * columns under the wrong names (the exact corruption the column-count guard
 * alone cannot catch).
 *
 * <p>Wire form: {@link #MAGIC} ("PIXM" little-endian), a version byte, a
 * flags byte ({@link #FLAG_STALE}), the leaf count and build revision as
 * little-endian ints, per leaf a (firstRecordKey, lastRecordKey) fence pair
 * as little-endian longs (the incremental maintenance's zone maps — one
 * slot-0 read instead of probing every leaf), the root path as a
 * length-prefixed UTF-8 string, an
 * int column count, then per column: path (UTF-8, length-prefixed), name
 * (UTF-8, length-prefixed), and one column-kind byte
 * ({@link ProjectionIndexLeafPage#COLUMN_KIND_NUMERIC_LONG} /
 * {@code BOOLEAN} / {@code STRING_DICT}).
 *
 * <p>The <b>stale</b> flag is the update-time invalidation hook: the
 * projection change listener overwrites slot 0 with {@link #staleTombstone()}
 * when a write transaction modifies the indexed record set, so a later
 * hydrate refuses the outdated columns and rebuilds instead. The leaf count
 * bounds the hydrate read — a rebuild that shrinks the projection may leave
 * stale payloads at higher slots, which hydration must ignore.
 *
 * <p>{@link #parse} returns {@code null} for payloads without the magic, so
 * hydrate paths can probe slot 0 and fall back to metadata-less handling for
 * stores written by the bench setups (which persist leaves only).
 */
public final class ProjectionIndexMetadata {

  /** Leading magic of a metadata payload ("PIXM" little-endian). */
  public static final int MAGIC = 0x4D585049;

  /** Flags bit0: the projection was invalidated by an update transaction. */
  public static final byte FLAG_STALE = 0x01;

  /**
   * Wire-format version. MUST be bumped on every layout change — an unknown
   * version parses to {@code null} (same as "no metadata"), which hydrate
   * paths treat as "rebuild", so older-format stores degrade to a rebuild
   * instead of a misparse or a spurious corruption error. Version 1 is the
   * current layout; no databases with earlier layouts exist.
   */
  private static final byte VERSION = 2;

  private final String rootPath;
  private final String[] fieldPaths;
  private final String[] fieldNames;
  private final byte[] columnKinds;
  private final int leafCount;
  private final int buildRevision;

  /**
   * Per-leaf record-key fences, index-aligned with leaf slots 1..leafCount
   * (entry {@code i} describes slot {@code i + 1}). Read by the incremental
   * maintenance's leaf location in ONE slot-0 read instead of probing every
   * leaf's head chunk per commit (O(leafCount) HOT descents). Empty leaves
   * carry the degenerate ({@code Long.MAX_VALUE}, {@code Long.MIN_VALUE})
   * range.
   */
  private final long[] leafFirstRecordKeys;
  private final long[] leafLastRecordKeys;

  private final byte flags;

  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths,
      final String[] fieldNames, final byte[] columnKinds, final int leafCount,
      final int buildRevision, final long[] leafFirstRecordKeys,
      final long[] leafLastRecordKeys) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, leafCount, buildRevision,
        leafFirstRecordKeys, leafLastRecordKeys, (byte) 0);
  }

  private ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths,
      final String[] fieldNames, final byte[] columnKinds, final int leafCount,
      final int buildRevision, final long[] leafFirstRecordKeys,
      final long[] leafLastRecordKeys, final byte flags) {
    if (fieldPaths.length != fieldNames.length || fieldPaths.length != columnKinds.length) {
      throw new IllegalArgumentException("paths/names/kinds must be index-aligned");
    }
    if (leafCount < 0) {
      throw new IllegalArgumentException("leafCount must be >= 0, got " + leafCount);
    }
    if (buildRevision < 0) {
      throw new IllegalArgumentException("buildRevision must be >= 0, got " + buildRevision);
    }
    if (leafFirstRecordKeys.length != leafCount || leafLastRecordKeys.length != leafCount) {
      throw new IllegalArgumentException("leaf record-key fences must carry one entry per leaf ("
          + leafCount + "), got " + leafFirstRecordKeys.length + "/" + leafLastRecordKeys.length);
    }
    this.rootPath = rootPath;
    this.fieldPaths = fieldPaths.clone();
    this.fieldNames = fieldNames.clone();
    this.columnKinds = columnKinds.clone();
    this.leafCount = leafCount;
    this.buildRevision = buildRevision;
    this.leafFirstRecordKeys = leafFirstRecordKeys.clone();
    this.leafLastRecordKeys = leafLastRecordKeys.clone();
    this.flags = flags;
  }

  private static final long[] NO_FENCES = new long[0];

  /** Minimal stale marker the change listener writes over slot 0 on invalidation. */
  public static ProjectionIndexMetadata staleTombstone() {
    return new ProjectionIndexMetadata("", new String[0], new String[0], new byte[0], 0, 0,
        NO_FENCES, NO_FENCES, FLAG_STALE);
  }

  public String rootPath() {
    return rootPath;
  }

  public String[] fieldPaths() {
    return fieldPaths.clone();
  }

  public String[] fieldNames() {
    return fieldNames.clone();
  }

  public byte[] columnKinds() {
    return columnKinds.clone();
  }

  /** Number of leaf payloads at slots 1..leafCount; higher slots are stale remnants. */
  public int leafCount() {
    return leafCount;
  }

  /**
   * Revision the columns were built over — hydration installs the registry
   * handle with this as its valid-from revision, so time-travel executors
   * bound to earlier revisions refuse it.
   */
  public int buildRevision() {
    return buildRevision;
  }

  /** First record key of 0-based leaf {@code i} (slot {@code i + 1}). */
  public long leafFirstRecordKey(final int i) {
    return leafFirstRecordKeys[i];
  }

  /** Last record key of 0-based leaf {@code i} (slot {@code i + 1}). */
  public long leafLastRecordKey(final int i) {
    return leafLastRecordKeys[i];
  }

  /** Whether an update transaction invalidated this projection. */
  public boolean isStale() {
    return (flags & FLAG_STALE) != 0;
  }

  /** Whether this metadata describes exactly the given shape. */
  public boolean matches(final String otherRootPath, final String[] otherFieldPaths,
      final byte[] otherColumnKinds) {
    return rootPath.equals(otherRootPath)
        && Arrays.equals(fieldPaths, otherFieldPaths)
        && Arrays.equals(columnKinds, otherColumnKinds);
  }

  public byte[] serialize() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    putIntLE(out, MAGIC);
    out.write(VERSION);
    out.write(flags);
    putIntLE(out, leafCount);
    putIntLE(out, buildRevision);
    for (int i = 0; i < leafCount; i++) {
      putLongLE(out, leafFirstRecordKeys[i]);
      putLongLE(out, leafLastRecordKeys[i]);
    }
    putString(out, rootPath);
    putIntLE(out, fieldPaths.length);
    for (int i = 0; i < fieldPaths.length; i++) {
      putString(out, fieldPaths[i]);
      putString(out, fieldNames[i]);
      out.write(columnKinds[i]);
    }
    return out.toByteArray();
  }

  /**
   * Parse a metadata payload; {@code null} when {@code payload} does not
   * carry the metadata magic (e.g. a leaf payload from a metadata-less
   * store).
   *
   * @throws IllegalStateException on a structurally corrupt metadata payload
   */
  public static ProjectionIndexMetadata parse(final byte[] payload) {
    if (payload == null || payload.length < 6 || getIntLE(payload, 0) != MAGIC) {
      return null;
    }
    try {
      final int[] pos = {4};
      final byte version = payload[pos[0]++];
      if (version != VERSION) {
        // Older/newer wire format — treated like "no metadata": hydrate
        // paths rebuild instead of misparsing bytes at shifted offsets.
        return null;
      }
      final byte flags = payload[pos[0]++];
      final int leafCount = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (leafCount < 0) {
        throw new IllegalStateException("Implausible projection leaf count " + leafCount);
      }
      final int buildRevision = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (buildRevision < 0) {
        throw new IllegalStateException("Implausible projection build revision " + buildRevision);
      }
      if (pos[0] + 16L * leafCount > payload.length) {
        throw new IllegalStateException("Truncated projection leaf fences (" + leafCount
            + " leaves declared, " + (payload.length - pos[0]) + " bytes left)");
      }
      final long[] firstKeys = new long[leafCount];
      final long[] lastKeys = new long[leafCount];
      for (int i = 0; i < leafCount; i++) {
        firstKeys[i] = getLongLE(payload, pos[0]);
        pos[0] += 8;
        lastKeys[i] = getLongLE(payload, pos[0]);
        pos[0] += 8;
      }
      final String rootPath = getString(payload, pos);
      final int n = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (n < 0 || n > 4096) {
        throw new IllegalStateException("Implausible projection column count " + n);
      }
      final String[] paths = new String[n];
      final String[] names = new String[n];
      final byte[] kinds = new byte[n];
      for (int i = 0; i < n; i++) {
        paths[i] = getString(payload, pos);
        names[i] = getString(payload, pos);
        kinds[i] = payload[pos[0]++];
      }
      return new ProjectionIndexMetadata(rootPath, paths, names, kinds, leafCount, buildRevision,
          firstKeys, lastKeys, flags);
    } catch (final IndexOutOfBoundsException truncated) {
      throw new IllegalStateException("Corrupt projection metadata payload", truncated);
    }
  }

  private static void putString(final ByteArrayOutputStream out, final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    putIntLE(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static String getString(final byte[] payload, final int[] pos) {
    final int len = getIntLE(payload, pos[0]);
    pos[0] += 4;
    if (len < 0 || pos[0] + len > payload.length) {
      throw new IndexOutOfBoundsException("string length " + len);
    }
    final String value = new String(payload, pos[0], len, StandardCharsets.UTF_8);
    pos[0] += len;
    return value;
  }

  private static void putIntLE(final ByteArrayOutputStream out, final int v) {
    out.write(v);
    out.write(v >>> 8);
    out.write(v >>> 16);
    out.write(v >>> 24);
  }

  private static void putLongLE(final ByteArrayOutputStream out, final long v) {
    putIntLE(out, (int) v);
    putIntLE(out, (int) (v >>> 32));
  }

  private static long getLongLE(final byte[] b, final int off) {
    return (getIntLE(b, off) & 0xFFFFFFFFL) | ((long) getIntLE(b, off + 4) << 32);
  }

  private static int getIntLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
  }
}
