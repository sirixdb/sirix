/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Self-describing metadata payload persisted alongside projection leaves
 * (slot 0 of the HOT sub-tree, leaves at slots 1..N): the projection's root
 * path, per-column field paths, column names, and column kinds. Hydration
 * reads the projection's shape from HERE instead of trusting the caller's
 * argument list — without it, a re-create with a same-arity but different
 * field list would silently install the persisted columns under the wrong
 * names (the exact corruption the column-count guard alone cannot catch).
 *
 * <p>Wire form: {@link #MAGIC} ("PIXM" little-endian), a version byte, the
 * root path as a length-prefixed UTF-8 string, an int column count, then per
 * column: path (UTF-8, length-prefixed), name (UTF-8, length-prefixed), and
 * one column-kind byte ({@link ProjectionIndexLeafPage#COLUMN_KIND_NUMERIC_LONG}
 * / {@code BOOLEAN} / {@code STRING_DICT}).
 *
 * <p>{@link #parse} returns {@code null} for payloads without the magic, so
 * hydrate paths can probe slot 0 and fall back to metadata-less handling for
 * stores written by the bench setups (which persist leaves only).
 */
public final class ProjectionIndexMetadata {

  /** Leading magic of a metadata payload ("PIXM" little-endian). */
  public static final int MAGIC = 0x4D585049;

  private static final byte VERSION = 1;

  private final String rootPath;
  private final String[] fieldPaths;
  private final String[] fieldNames;
  private final byte[] columnKinds;

  public ProjectionIndexMetadata(final String rootPath, final String[] fieldPaths,
      final String[] fieldNames, final byte[] columnKinds) {
    if (fieldPaths.length != fieldNames.length || fieldPaths.length != columnKinds.length) {
      throw new IllegalArgumentException("paths/names/kinds must be index-aligned");
    }
    this.rootPath = rootPath;
    this.fieldPaths = fieldPaths.clone();
    this.fieldNames = fieldNames.clone();
    this.columnKinds = columnKinds.clone();
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
    if (payload == null || payload.length < 5 || getIntLE(payload, 0) != MAGIC) {
      return null;
    }
    try {
      final int[] pos = {4};
      final byte version = payload[pos[0]++];
      if (version != VERSION) {
        throw new IllegalStateException("Unknown projection metadata version " + version);
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
      return new ProjectionIndexMetadata(rootPath, paths, names, kinds);
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
