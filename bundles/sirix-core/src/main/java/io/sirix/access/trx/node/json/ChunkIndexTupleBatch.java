/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * One chunk's PATH/CAS/NAME index tuples, collected in the parallel importer's build worker from
 * the same primitives the worker writes into page bytes, and drained by the coordinator into the
 * families' {@code add(...)} entry points in chunk order.
 *
 * <h2>Filtering split</h2> The worker pre-prunes with UNION filters snapshot at chunk dispatch —
 * the union of every PATH/CAS definition's resolved path classes and of every NAME definition's
 * included dictionary name keys ({@code null} union = a definition indexes everything, collect
 * all). The exact per-definition filter, include/exclude semantics and CAS type conversion run at
 * drain, inside the builders themselves — the one place those semantics already live. The snapshots
 * are exact for their chunk by the importer's standing argument: a chunk's paths and names are
 * resolved into the summary and the dictionary BEFORE the chunk is dispatched, so a class first
 * occurring in this chunk is in this chunk's snapshot.
 *
 * <h2>Memory discipline</h2> Primitive parallel lists grown amortized, one UTF-8 arena for CAS
 * string values, {@code Number} references reused from the parser's own boxes — no per-record
 * allocation beyond the amortized growth. The batch dies once the coordinator drains it.
 */
final class ChunkIndexTupleBatch {

  static final byte CAS_KIND_STRING = 0;
  static final byte CAS_KIND_NUMBER = 1;
  static final byte CAS_KIND_BOOLEAN_TRUE = 2;
  static final byte CAS_KIND_BOOLEAN_FALSE = 3;
  static final byte CAS_KIND_INT = 4;
  static final byte CAS_KIND_LONG = 5;

  private final @Nullable LongOpenHashSet pathPcrUnion;
  private final @Nullable LongOpenHashSet casPcrUnion;
  private final @Nullable IntOpenHashSet nameKeyUnion;
  private final boolean pathActive;
  private final boolean casActive;
  private final boolean nameActive;

  // PATH: (pathNodeKey, nodeKey) for ARRAY and every OBJECT_NAMED_* create.
  private final LongArrayList pathPcrs = new LongArrayList(64);
  private final LongArrayList pathNodeKeys = new LongArrayList(64);

  // OBJECT_NAMED_ARRAY mirror candidates: the OBJECT_KEY-layer entry lives under the PARENT path
  // class of the array-layer one, which only the coordinator's path summary can resolve — so these
  // carry the array-layer class and are re-keyed (and union-filtered) at drain.
  private final LongArrayList mirrorArrayPcrs = new LongArrayList(8);
  private final LongArrayList mirrorNodeKeys = new LongArrayList(8);

  // NAME: (dictionary name key, nodeKey) for every OBJECT_NAMED_* create.
  private final IntArrayList nameKeys = new IntArrayList(64);
  private final LongArrayList nameNodeKeys = new LongArrayList(64);

  // CAS: (pathNodeKey, nodeKey, kind, value) for the six value kinds. String payloads live in the
  // arena in tuple order; uncommon boxed Number payloads and common integral payloads each have a
  // dense side lane in tuple order.
  private final LongArrayList casPcrs = new LongArrayList(64);
  private final LongArrayList casNodeKeys = new LongArrayList(64);
  private final ByteArrayList casKinds = new ByteArrayList(64);
  private final IntArrayList casStringOffsets = new IntArrayList(32);
  private final IntArrayList casStringLengths = new IntArrayList(32);
  private final ObjectArrayList<Number> casNumbers = new ObjectArrayList<>(32);
  private final LongArrayList casIntegralNumbers = new LongArrayList(32);
  private byte[] casStringArena = new byte[1024];
  private int casStringArenaUsed;

  ChunkIndexTupleBatch(final boolean pathActive, final @Nullable LongOpenHashSet pathPcrUnion, final boolean casActive,
      final @Nullable LongOpenHashSet casPcrUnion, final boolean nameActive,
      final @Nullable IntOpenHashSet nameKeyUnion) {
    this.pathActive = pathActive;
    this.pathPcrUnion = pathPcrUnion;
    this.casActive = casActive;
    this.casPcrUnion = casPcrUnion;
    this.nameActive = nameActive;
    this.nameKeyUnion = nameKeyUnion;
  }

  // ==== worker feed ============================================================================

  /** An ARRAY or OBJECT_NAMED_* create — the kinds the PATH family indexes. */
  void onPathEntry(final long pathNodeKey, final long nodeKey) {
    if (pathActive && (pathPcrUnion == null || pathPcrUnion.contains(pathNodeKey))) {
      pathPcrs.add(pathNodeKey);
      pathNodeKeys.add(nodeKey);
    }
  }

  /** An OBJECT_NAMED_ARRAY create — carries a second, OBJECT_KEY-layer PATH entry after re-keying. */
  void onNamedArrayMirrorCandidate(final long arrayLayerPathNodeKey, final long nodeKey) {
    if (pathActive) {
      mirrorArrayPcrs.add(arrayLayerPathNodeKey);
      mirrorNodeKeys.add(nodeKey);
    }
  }

  /** Any OBJECT_NAMED_* create — the kinds the NAME family indexes, keyed by dictionary name key. */
  void onNameEntry(final int nameKey, final long nodeKey) {
    if (nameActive && (nameKeyUnion == null || nameKeyUnion.contains(nameKey))) {
      nameKeys.add(nameKey);
      nameNodeKeys.add(nodeKey);
    }
  }

  void onCasString(final long pathNodeKey, final long nodeKey, final byte[] utf8, final int length) {
    if (!casActive || (casPcrUnion != null && !casPcrUnion.contains(pathNodeKey))) {
      return;
    }
    if (casStringArenaUsed + length > casStringArena.length) {
      int grown = casStringArena.length;
      while (grown < casStringArenaUsed + length) {
        grown = Math.multiplyExact(grown, 2);
      }
      casStringArena = Arrays.copyOf(casStringArena, grown);
    }
    System.arraycopy(utf8, 0, casStringArena, casStringArenaUsed, length);
    casStringOffsets.add(casStringArenaUsed);
    casStringLengths.add(length);
    casStringArenaUsed += length;
    casPcrs.add(pathNodeKey);
    casNodeKeys.add(nodeKey);
    casKinds.add(CAS_KIND_STRING);
  }

  void onCasNumber(final long pathNodeKey, final long nodeKey, final Number value) {
    if (!casActive || (casPcrUnion != null && !casPcrUnion.contains(pathNodeKey))) {
      return;
    }
    casNumbers.add(value);
    casPcrs.add(pathNodeKey);
    casNodeKeys.add(nodeKey);
    casKinds.add(CAS_KIND_NUMBER);
  }

  void onCasInt(final long pathNodeKey, final long nodeKey, final int value) {
    onCasIntegral(pathNodeKey, nodeKey, value, CAS_KIND_INT);
  }

  void onCasLong(final long pathNodeKey, final long nodeKey, final long value) {
    onCasIntegral(pathNodeKey, nodeKey, value, CAS_KIND_LONG);
  }

  private void onCasIntegral(final long pathNodeKey, final long nodeKey, final long value, final byte kind) {
    if (!casActive || (casPcrUnion != null && !casPcrUnion.contains(pathNodeKey))) {
      return;
    }
    casIntegralNumbers.add(value);
    casPcrs.add(pathNodeKey);
    casNodeKeys.add(nodeKey);
    casKinds.add(kind);
  }

  void onCasBoolean(final long pathNodeKey, final long nodeKey, final boolean value) {
    if (!casActive || (casPcrUnion != null && !casPcrUnion.contains(pathNodeKey))) {
      return;
    }
    casPcrs.add(pathNodeKey);
    casNodeKeys.add(nodeKey);
    casKinds.add(value
        ? CAS_KIND_BOOLEAN_TRUE
        : CAS_KIND_BOOLEAN_FALSE);
  }

  // ==== coordinator drain ======================================================================

  int pathEntryCount() {
    return pathPcrs.size();
  }

  long pathPcrAt(final int index) {
    return pathPcrs.getLong(index);
  }

  long pathNodeKeyAt(final int index) {
    return pathNodeKeys.getLong(index);
  }

  int mirrorCandidateCount() {
    return mirrorArrayPcrs.size();
  }

  long mirrorArrayPcrAt(final int index) {
    return mirrorArrayPcrs.getLong(index);
  }

  long mirrorNodeKeyAt(final int index) {
    return mirrorNodeKeys.getLong(index);
  }

  int nameEntryCount() {
    return nameKeys.size();
  }

  int nameKeyAt(final int index) {
    return nameKeys.getInt(index);
  }

  long nameNodeKeyAt(final int index) {
    return nameNodeKeys.getLong(index);
  }

  int casEntryCount() {
    return casPcrs.size();
  }

  long casPcrAt(final int index) {
    return casPcrs.getLong(index);
  }

  long casNodeKeyAt(final int index) {
    return casNodeKeys.getLong(index);
  }

  byte casKindAt(final int index) {
    return casKinds.getByte(index);
  }

  byte[] casStringArena() {
    return casStringArena;
  }

  /** Offset of the {@code stringOrdinal}-th CAS string tuple's payload in {@link #casStringArena}. */
  int casStringOffsetAt(final int stringOrdinal) {
    return casStringOffsets.getInt(stringOrdinal);
  }

  int casStringLengthAt(final int stringOrdinal) {
    return casStringLengths.getInt(stringOrdinal);
  }

  Number casNumberAt(final int numberOrdinal) {
    return casNumbers.get(numberOrdinal);
  }

  long casIntegralNumberAt(final int numberOrdinal) {
    return casIntegralNumbers.getLong(numberOrdinal);
  }
}
