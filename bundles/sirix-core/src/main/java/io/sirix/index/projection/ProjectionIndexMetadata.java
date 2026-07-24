/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Self-describing metadata payload persisted alongside projection leaves
 * (slot 0 of the HOT sub-tree, leaves at slots 1..{@link #rowGroupCount()}): the
 * projection's root path, per-column field paths, column names, and column
 * kinds. Hydration reads the projection's shape from HERE instead of
 * trusting the caller's argument list — without it, a re-create with a
 * same-arity but different field list would silently install the persisted
 * columns under the wrong names (the exact corruption the column-count guard
 * alone cannot catch).
 *
 * <p>Wire form: {@link #MAGIC} ("PIXM" little-endian), a version byte, a
 * flags byte ({@link #FLAG_STALE}), the leaf count and build revision as
 * little-endian ints, the root path as a length-prefixed UTF-8 string, an
 * int column count, then per column: path (UTF-8, length-prefixed), name
 * (UTF-8, length-prefixed), and one column-kind byte
 * ({@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_LONG} /
 * {@code BOOLEAN} / {@code STRING_DICT}).
 *
 * <p>The per-leaf {@code (firstRecordKey, lastRecordKey)} fences — the
 * incremental maintenance's zone map — used to ride inside this blob, but at
 * scale that made every commit re-persist the whole fence array (~1.5&nbsp;MB at
 * 100k leaves) just because one leaf moved. They now live in their own
 * carry-forward chunks ({@link ProjectionIndexFences}), so this metadata blob
 * stays tiny (shape only) and a commit rewrites only the fence chunks it
 * actually changed.
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
   * Layout discriminator (F2): set when the leaves are stored in the <em>segment-slot</em> layout
   * (one HOT slot per segment, descriptor at slotKind 0) rather than the descriptor layout (one slot
   * per leaf). The catalog reads this before enumerating so a segment-slot sub-tree is never fed to
   * the descriptor-layout reader (which would skip its blob descriptor slots and see zero leaves).
   */
  public static final byte FLAG_COLUMN_SEGMENT_SLOT_LAYOUT = 0x02;

  /**
   * Wire-format version. An unknown version parses to {@code null} (same as
   * "no metadata"), which hydrate paths treat as "rebuild", so a layout change
   * can bump this and degrade gracefully.
   *
   * <p>VERSION 2 moved the per-leaf fences out of this blob into
   * {@link ProjectionIndexFences} chunks. Unlike the earlier
   * segment-directory switch (which stayed at VERSION 1 because a legacy blob
   * was detectable STRUCTURALLY — its slot-0 payload is not a blob marker), a
   * VERSION-1 fenced blob and a VERSION-2 shape-only blob share the same magic
   * and header prefix, so the version byte is the ONLY signal that the bytes
   * after {@code buildRevision} are the root path rather than a fence array.
   * Bumping to 2 makes {@link #parse} reject an old fenced blob cleanly (→
   * {@code null} → rebuild) instead of misreading a fence long as a string
   * length.
   */
  private static final byte VERSION = 2;

  private final String rootPath;
  private final String[] fieldPaths;
  private final String[] fieldNames;
  private final byte[] columnKinds;
  private final int rowGroupCount;
  private final int buildRevision;

  private final byte flags;

  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths,
      final String[] fieldNames, final byte[] columnKinds, final int rowGroupCount,
      final int buildRevision) {
    this(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount, buildRevision, (byte) 0);
  }

  private ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths,
      final String[] fieldNames, final byte[] columnKinds, final int rowGroupCount,
      final int buildRevision, final byte flags) {
    if (fieldPaths.length != fieldNames.length || fieldPaths.length != columnKinds.length) {
      throw new IllegalArgumentException("paths/names/kinds must be index-aligned");
    }
    if (rowGroupCount < 0) {
      throw new IllegalArgumentException("rowGroupCount must be >= 0, got " + rowGroupCount);
    }
    if (buildRevision < 0) {
      throw new IllegalArgumentException("buildRevision must be >= 0, got " + buildRevision);
    }
    this.rootPath = rootPath;
    this.fieldPaths = fieldPaths.clone();
    this.fieldNames = fieldNames.clone();
    this.columnKinds = columnKinds.clone();
    this.rowGroupCount = rowGroupCount;
    this.buildRevision = buildRevision;
    this.flags = flags;
  }

  /** Minimal stale marker the change listener writes over slot 0 on invalidation. */
  public static ProjectionIndexMetadata staleTombstone() {
    return new ProjectionIndexMetadata("", new String[0], new String[0], new byte[0], 0, 0,
        FLAG_STALE);
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

  /** Number of leaf payloads at slots 1..rowGroupCount; higher slots are stale remnants. */
  public int rowGroupCount() {
    return rowGroupCount;
  }

  /**
   * Revision the columns were built over — hydration installs the registry
   * handle with this as its valid-from revision, so time-travel executors
   * bound to earlier revisions refuse it.
   */
  public int buildRevision() {
    return buildRevision;
  }

  /** Whether an update transaction invalidated this projection. */
  public boolean isStale() {
    return (flags & FLAG_STALE) != 0;
  }

  /** Whether the leaves are stored in the segment-slot layout (F2 discriminator). */
  public boolean isColumnSegmentSlotLayout() {
    return (flags & FLAG_COLUMN_SEGMENT_SLOT_LAYOUT) != 0;
  }

  /** This metadata with the segment-slot layout flag set — stamped by a segment-slot builder. */
  public ProjectionIndexMetadata withColumnSegmentSlotLayout() {
    return new ProjectionIndexMetadata(rootPath, fieldPaths, fieldNames, columnKinds, rowGroupCount,
        buildRevision, (byte) (flags | FLAG_COLUMN_SEGMENT_SLOT_LAYOUT));
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
    putIntLE(out, rowGroupCount);
    putIntLE(out, buildRevision);
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
      final int rowGroupCount = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (rowGroupCount < 0) {
        throw new IllegalStateException("Implausible projection leaf count " + rowGroupCount);
      }
      final int buildRevision = getIntLE(payload, pos[0]);
      pos[0] += 4;
      if (buildRevision < 0) {
        throw new IllegalStateException("Implausible projection build revision " + buildRevision);
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
      return new ProjectionIndexMetadata(rootPath, paths, names, kinds, rowGroupCount, buildRevision,
          flags);
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

  private static int getIntLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
  }
}
