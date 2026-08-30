/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.OverflowPage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2 suite: descriptor + per-segment codec round trips (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md
 * §9 P2). The load-bearing assertion everywhere is <b>byte-identity of the assembled raw form</b> —
 * segments → raw must equal the original {@code serialize()} output, provenance included — plus the
 * fail-loud integrity contract (hash/length/ kind mismatches throw at assembly, never mid-kernel).
 */
final class ProjectionIndexColumnSegmentCodecTest {

  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
          ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};

  /** Representative bench-shaped row group with mixed scalar column kinds. */
  private static ProjectionIndexRowGroupPage benchLeaf(final int rows, final long keyBase) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random rng = new Random(7);
    final long[] longs = new long[4];
    final boolean[] bools = new boolean[4];
    final String[] strings = new String[4];
    final boolean[] present = new boolean[4];
    final boolean[] unrep = new boolean[4];
    final boolean[] nonIntegral = new boolean[4];
    long key = keyBase;
    for (int i = 0; i < rows; i++) {
      key += 8 + rng.nextInt(9);
      final boolean deptMissing = i % 5 == 0;
      longs[0] = 18 + rng.nextInt(48);
      bools[1] = rng.nextBoolean();
      strings[2] = deptMissing
          ? ""
          : DEPTS[rng.nextInt(DEPTS.length)];
      longs[3] = 0L;
      present[0] = true;
      present[1] = true;
      present[2] = !deptMissing;
      present[3] = false;
      Arrays.fill(unrep, false);
      nonIntegral[0] = i % 11 == 0;
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  private static ProjectionIndexColumnSegmentCodec.SegmentResolver resolverOf(
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    final Map<Integer, byte[]> byId = new HashMap<>();
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      byId.put(encoded.columnSegmentIds()[i], encoded.segments()[i]);
    }
    return byId::get;
  }

  /** Simulate a cold read: neither descriptor nor resolved segment aliases the encoder output. */
  private static ProjectionIndexColumnSegmentCodec.SegmentResolver coldResolverOf(
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    final Map<Integer, byte[]> byId = new HashMap<>();
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      byId.put(encoded.columnSegmentIds()[i], encoded.segments()[i].clone());
    }
    return byId::get;
  }

  private static void assertEncodedEquals(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expected,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup actual) {
    assertArrayEquals(expected.descriptor(), actual.descriptor());
    assertArrayEquals(expected.columnSegmentIds(), actual.columnSegmentIds());
    assertEquals(expected.segments().length, actual.segments().length);
    for (int i = 0; i < expected.segments().length; i++) {
      assertArrayEquals(expected.segments()[i], actual.segments()[i], "segment " + i + " differs");
    }
  }

  /** Deep snapshot used to prove a later workspace reset cannot mutate already-published output. */
  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup snapshot(
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    final byte[][] segments = new byte[encoded.segments().length][];
    for (int i = 0; i < segments.length; i++) {
      segments[i] = encoded.segments()[i].clone();
    }
    return new ProjectionIndexColumnSegmentCodec.EncodedRowGroup(encoded.descriptor().clone(),
        encoded.columnSegmentIds().clone(), segments);
  }

  /**
   * Every output container and segment must be owned by one encode, never by the reusable workspace.
   */
  private static void assertOutputsDetached(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup first,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup second) {
    assertNotSame(first.descriptor(), second.descriptor());
    assertNotSame(first.columnSegmentIds(), second.columnSegmentIds());
    assertNotSame(first.segments(), second.segments());
    for (final byte[] firstSegment : first.segments()) {
      for (final byte[] secondSegment : second.segments()) {
        assertNotSame(firstSegment, secondSegment, "two encodes must not share a segment output array");
      }
    }
  }

  /** Encode the same page through the serialized-raw and borrowed live-page boundaries. */
  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup assertLiveEncodingEqualsRaw(
      final ProjectionIndexRowGroupPage page, final ProjectionIndexColumnSegmentCodec.EncodeWorkspace rawWorkspace,
      final ProjectionIndexColumnSegmentCodec.EncodeWorkspace liveWorkspace) {
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expected =
        ProjectionIndexColumnSegmentCodec.encode(raw, rawWorkspace);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup actual =
        ProjectionIndexColumnSegmentCodec.encode(page, liveWorkspace);
    assertEncodedEquals(expected, actual);
    return actual;
  }

  private static ProjectionIndexRowGroupPage setLeaf() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET});
    appendSet(page, 700L, "Drama", "Short");
    appendSet(page, 701L);
    appendSet(page, 702L, "Comedy");
    appendSet(page, 703L, "Drama", "Comedy", "Silent");
    return page;
  }

  /** One populated column of {@code kind}; enough to force every mandatory segment for that kind. */
  private static ProjectionIndexRowGroupPage singleKindLeaf(final byte kind) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {kind});
    if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
      page.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {new GlobalValueDictionaryWriter()});
    }
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrepresentable = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    final boolean[] nonDoubleSource = new boolean[1];
    for (int row = 0; row < 8; row++) {
      longs[0] = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE
          ? ProjectionDoubleEncoding.encode(row + 0.25)
          : row * 11L;
      bools[0] = (row & 1) == 0;
      strings[0] = "value-" + row;
      final String[][] sets = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET
          ? new String[][] {{"value-" + row, "shared"}}
          : null;
      assertTrue(page.appendRow(100L + row, longs, bools, strings, sets, present, unrepresentable, nonIntegral,
          nonDoubleSource));
    }
    return page;
  }

  private static void appendSet(final ProjectionIndexRowGroupPage page, final long key, final String... values) {
    assertTrue(page.appendRow(key, new long[1], new boolean[1], new String[] {""}, new String[][] {values},
        new boolean[] {true}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false}));
  }

  private static void assertRoundTrip(final ProjectionIndexRowGroupPage page) {
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    RowGroupDescriptor.validate(encoded.descriptor());
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)),
        "assembleRaw(encode(raw)) must be byte-identical");
  }

  private static boolean appendNumericRow(final ProjectionIndexRowGroupPage page, final long recordKey,
      final long value, final boolean orderException) {
    return page.appendExtractedUtf8Row(recordKey, new long[] {value}, new boolean[1], new byte[1][], new int[1],
        new String[1][], new boolean[] {true}, new boolean[1], new boolean[1], new boolean[1], orderException);
  }

  private static ProjectionIndexRowGroupPage orderMetadataLeaf(final boolean dense) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(appendNumericRow(page, 2L, 10L, false));
    if (dense) {
      assertTrue(appendNumericRow(page, 100L, 20L, true));
    }
    assertTrue(appendNumericRow(page, 5L, 30L, false));
    assertTrue(appendNumericRow(page, 8L, 40L, false));
    return page;
  }

  // ==================== dict-entry hashes ====================

  /**
   * The segment bytes for {@code columnSegmentId}, or {@code null} when the leaf has no such entry.
   */
  private static byte[] segmentOf(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final int segId) {
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == segId) {
        return encoded.segments()[i];
      }
    }
    return null;
  }

  /** Rebuild a descriptor over exactly {@code ids}, retaining mirrors for entries present in {@code source}. */
  private static byte[] descriptorWithIds(final byte[] source, final int... ids) {
    final int columnCount = RowGroupDescriptor.columnCount(source);
    final byte[] kinds = new byte[columnCount];
    for (int column = 0; column < columnCount; column++) {
      kinds[column] = RowGroupDescriptor.kind(source, column);
    }
    final int count = ids.length;
    final int[] lengths = new int[count];
    final long[] hashes = new long[count];
    final byte[] flags = new byte[count];
    final long[] mins = new long[count];
    final long[] maxs = new long[count];
    for (int entry = 0; entry < count; entry++) {
      final int sourceEntry = RowGroupDescriptor.entryIndexOf(source, ids[entry]);
      if (sourceEntry < 0) {
        lengths[entry] = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES;
        continue;
      }
      lengths[entry] = RowGroupDescriptor.entryByteLen(source, sourceEntry);
      hashes[entry] = RowGroupDescriptor.entryContentHash(source, sourceEntry);
      flags[entry] = RowGroupDescriptor.entryColFlags(source, sourceEntry);
      mins[entry] = RowGroupDescriptor.entryMin(source, sourceEntry);
      maxs[entry] = RowGroupDescriptor.entryMax(source, sourceEntry);
    }
    return RowGroupDescriptor.serialize(RowGroupDescriptor.rowCount(source), RowGroupDescriptor.firstRecordKey(source),
        RowGroupDescriptor.lastRecordKey(source), kinds, count, ids, lengths, hashes, flags, mins, maxs);
  }

  private static byte[] descriptorWithoutId(final byte[] source, final int removedId) {
    final int[] retained = new int[RowGroupDescriptor.columnSegmentCount(source) - 1];
    int target = 0;
    for (int entry = 0; entry < RowGroupDescriptor.columnSegmentCount(source); entry++) {
      final int id = RowGroupDescriptor.entryColumnSegmentId(source, entry);
      if (id != removedId) {
        retained[target++] = id;
      }
    }
    assertEquals(retained.length, target, "test premise: descriptor contains segment " + removedId);
    return descriptorWithIds(source, retained);
  }

  private static int descriptorEntryOffset(final byte[] descriptor, final int entry) {
    return 27 + RowGroupDescriptor.columnCount(descriptor) + 2 + entry * RowGroupDescriptor.ENTRY_BYTES;
  }

  private static void putSegmentId(final byte[] descriptor, final int entry, final int id) {
    final int offset = descriptorEntryOffset(descriptor, entry);
    descriptor[offset] = (byte) id;
    descriptor[offset + 1] = (byte) (id >>> 8);
  }

  private static void assertNonBodyMirrorsRejected(
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final int segmentId) {
    final int entry = RowGroupDescriptor.entryIndexOf(encoded.descriptor(), segmentId);
    assertTrue(entry >= 0, "test premise: descriptor contains segment " + segmentId);
    final int offset = descriptorEntryOffset(encoded.descriptor(), entry);

    final byte[] flags = encoded.descriptor().clone();
    flags[offset + 14] = 1;
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(flags),
        "segment " + segmentId + " accepted BODY flags");

    final byte[] min = encoded.descriptor().clone();
    RowGroupDescriptor.putLongLE(min, offset + 15, 1L);
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(min),
        "segment " + segmentId + " accepted a BODY min mirror");

    final byte[] max = encoded.descriptor().clone();
    RowGroupDescriptor.putLongLE(max, offset + 23, 1L);
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(max),
        "segment " + segmentId + " accepted a BODY max mirror");
  }

  @Test
  void dictHashSegmentCarriesEveryEntrysContentHashInDictIdOrder() {
    // The whole point of the segment is that a distinct fold can read identities without the
    // dictionary — which only holds if the two hashes are the SAME function over the same bytes.
    // Compare entry by entry against the flat dictionary the ordinary slice decode produces.
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(1024, 1_000_000L).serialize());
    final int col = 2; // the STRING_DICT column of KINDS
    final long[] hashes = ProjectionIndexColumnSegmentCodec.decodeDictHashes(encoded.descriptor(),
        segmentOf(encoded, ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(col)), col);
    final ProjectionColumnStore.ColumnSlice full = ProjectionIndexColumnSegmentCodec.decodeStringSlice(
        encoded.descriptor(), segmentOf(encoded, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col)),
        segmentOf(encoded, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col)), col);
    assertNotNull(hashes, "a STRING_DICT column with rows must carry a DICT_HASHES segment");
    assertEquals(full.dictSize(), hashes.length, "one hash per dictionary entry, in dict-id order");
    for (int id = 0; id < hashes.length; id++) {
      final int off = full.dictOffsets()[id];
      assertEquals(ProjectionIndexByteScan.fnv1a64(full.dictBytes(), off, full.dictOffsets()[id + 1] - off), hashes[id],
          "entry " + id + "'s precomputed hash must equal the on-the-fly one");
    }
  }

  @Test
  void identitySliceCarriesTheSameIdsAndHashesAsTheFullSlice() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(1024, 1_000_000L).serialize());
    final int col = 2;
    final byte[] body = segmentOf(encoded, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col));
    final ProjectionColumnStore.ColumnSlice full =
        ProjectionIndexColumnSegmentCodec.decodeStringSlice(encoded.descriptor(), body,
            segmentOf(encoded, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col)), col);
    final ProjectionColumnStore.ColumnSlice identity = ProjectionIndexColumnSegmentCodec.decodeStringIdentitySlice(
        encoded.descriptor(), body, ProjectionIndexColumnSegmentCodec.decodeDictHashes(encoded.descriptor(),
            segmentOf(encoded, ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(col)), col),
        col);
    assertNull(identity.dictBytes(), "the identity fill must fetch NO dictionary bytes");
    assertNull(identity.dictOffsets(), "the identity fill must fetch NO dictionary bytes");
    assertEquals(full.rowCount(), identity.rowCount());
    assertEquals(full.dictSize(), identity.dictSize(), "dictSize must survive without the offsets array");
    assertArrayEquals(full.stringDictIds(), identity.stringDictIds());
    assertArrayEquals(full.presenceWords(), identity.presenceWords());
    for (int id = 0; id < full.dictSize(); id++) {
      assertEquals(full.dictHash(id), identity.dictHash(id), "entry " + id + " must have one identity, not two");
    }
  }

  // ==================== round trips ====================

  @Test
  void benchShapedLeafRoundTripsAndShrinks() {
    final ProjectionIndexRowGroupPage page = benchLeaf(1024, 1_000_000L);
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    int total = encoded.descriptor().length;
    for (final byte[] seg : encoded.segments()) {
      total += seg.length;
    }
    final int orderLabelLaneBytes = Integer.BYTES + (page.getRowCount() + 1) * Integer.BYTES + page.orderLabelLength();
    assertTrue((total - orderLabelLaneBytes) * 4 < raw.length - orderLabelLaneBytes,
        "expected >4x compaction on bench-shaped column data excluding exact Dewey labels, got "
            + (raw.length - orderLabelLaneBytes) + " -> " + (total - orderLabelLaneBytes));
  }

  @Test
  void partialSingleRowAndWordBoundaryLeavesRoundTrip() {
    assertRoundTrip(benchLeaf(100, 42L));
    assertRoundTrip(benchLeaf(1, 42L));
    assertRoundTrip(benchLeaf(64, 42L));
    assertRoundTrip(benchLeaf(65, 42L));
  }

  @Test
  void emptyLeafRoundTripsWithFlagTruthAndNoDicts() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    // KEYS + one BODY per column, no DICT segments for an empty leaf.
    assertEquals(1 + KINDS.length, encoded.columnSegmentIds().length);
    assertEquals(0, RowGroupDescriptor.rowCount(encoded.descriptor()));
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
  }

  @Test
  void nonAscendingDocumentKeysRoundTripWhenExceptionsAreMarked() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] keys = {Long.MAX_VALUE - 1, 5L, Long.MAX_VALUE, 0L};
    final long[] extremes = {Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L};
    for (int i = 0; i < keys.length; i++) {
      assertTrue(appendNumericRow(page, keys[i], extremes[i], i == 1 || i == 3));
    }
    assertRoundTrip(page);
  }

  @Test
  void keysViewReadsV0NoneAndDenseMetadataWithoutMaterializingTheBitmap() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup none =
        ProjectionIndexColumnSegmentCodec.encode(orderMetadataLeaf(false).serialize());
    final ProjectionIndexColumnSegmentCodec.KeysView noneView = ProjectionIndexColumnSegmentCodec.decodeKeysView(
        none.descriptor(), segmentOf(none, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()));
    assertFalse(noneView.dense());
    assertArrayEquals(new long[] {2L, 5L, 8L}, noneView.recordKeys());
    assertEquals(2L, noneView.firstRecordKey());
    assertEquals(8L, noneView.lastRecordKey());
    for (int row = 0; row < noneView.recordKeys().length; row++) {
      assertFalse(noneView.orderExceptionAt(row));
    }

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup dense =
        ProjectionIndexColumnSegmentCodec.encode(orderMetadataLeaf(true).serialize());
    final byte[] denseKeys = segmentOf(dense, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId());
    final ProjectionIndexColumnSegmentCodec.KeysView denseView =
        ProjectionIndexColumnSegmentCodec.decodeKeysView(dense.descriptor(), denseKeys);
    assertTrue(denseView.dense());
    assertArrayEquals(new long[] {2L, 100L, 5L, 8L}, denseView.recordKeys());
    assertEquals(2L, denseView.firstRecordKey());
    assertEquals(8L, denseView.lastRecordKey());
    assertFalse(denseView.orderExceptionAt(0));
    assertTrue(denseView.orderExceptionAt(1));
    assertFalse(denseView.orderExceptionAt(2));
    assertFalse(denseView.orderExceptionAt(3));
  }

  @Test
  void keysSliceMaterializesOnlyV0DenseOrderMetadata() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup none =
        ProjectionIndexColumnSegmentCodec.encode(orderMetadataLeaf(false).serialize());
    final ProjectionIndexColumnSegmentCodec.KeysSlice noneSlice =
        ProjectionIndexColumnSegmentCodec.decodeKeysAndOrderSlice(none.descriptor(),
            segmentOf(none, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()));
    assertNull(noneSlice.orderExceptionBits());
    assertFalse(noneSlice.orderExceptionAt(0));

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup dense =
        ProjectionIndexColumnSegmentCodec.encode(orderMetadataLeaf(true).serialize());
    final ProjectionIndexColumnSegmentCodec.KeysSlice denseSlice =
        ProjectionIndexColumnSegmentCodec.decodeKeysAndOrderSlice(dense.descriptor(),
            segmentOf(dense, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()));
    assertNotNull(denseSlice.orderExceptionBits());
    assertEquals(1, denseSlice.orderExceptionBits().length);
    assertEquals(1L << 1, denseSlice.orderExceptionBits()[0]);
    assertTrue(denseSlice.orderExceptionAt(1));
  }

  /** Ported width sweep: every FOR width 1..64 must survive segmentation. */
  @Test
  void wideBitWidthRangesRoundTripExactly() {
    for (int width = 48; width <= 64; width++) {
      final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
      final long span = width == 64
          ? -1L
          : (1L << (width - 1));
      final long[] longs = new long[1];
      final boolean[] bools = new boolean[1];
      final String[] strings = new String[1];
      final boolean[] present = {true};
      final boolean[] unrep = new boolean[1];
      final boolean[] nonIntegral = new boolean[1];
      final Random rng = new Random(width);
      for (int i = 0; i < 200; i++) {
        longs[0] = width == 64
            ? rng.nextLong()
            : (rng.nextLong() & span) - (span >>> 1);
        assertTrue(page.appendRow(1000L + i, longs, bools, strings, present, unrep, nonIntegral));
      }
      assertRoundTrip(page);
    }
  }

  @Test
  void hugeAscendingRecordKeyDeltasUseTheRawWidthAndRoundTrip() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    long key = 10L;
    for (int row = 0; row < 10; row++) {
      assertTrue(appendNumericRow(page, key, row, false));
      key += 1L << 58;
    }
    assertRoundTrip(page);
  }

  @Test
  void singleValueDictionaryUsesZeroWidthIdsAndRoundTrips() {
    final String[] values = new String[256];
    Arrays.fill(values, "OnlyValue");
    final ProjectionIndexRowGroupPage page = stringLeaf(values);
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final byte[] body = segmentOf(encoded, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    final int idWidthOffset = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 1 + 2 * Long.BYTES + 1;
    assertEquals(0, body[idWidthOffset], "one dictionary entry must need no per-row id bits");
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
  }

  @Test
  void realEmptyStringAndUnrepresentableRoundTrip() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = new boolean[1];
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    // Row 0: genuine empty string. Row 1: missing. Row 2: unrepresentable. Row 3: value.
    strings[0] = "";
    present[0] = true;
    unrep[0] = false;
    assertTrue(page.appendRow(10L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "";
    present[0] = false;
    assertTrue(page.appendRow(20L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "";
    present[0] = true;
    unrep[0] = true;
    assertTrue(page.appendRow(30L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "x";
    present[0] = true;
    unrep[0] = false;
    assertTrue(page.appendRow(40L, longs, bools, strings, present, unrep, nonIntegral));
    assertRoundTrip(page);
  }

  @Test
  void multiKilobyteDictionaryRoundTrips() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    final StringBuilder sb = new StringBuilder(1024);
    for (int i = 0; i < 300; i++) {
      sb.setLength(0);
      sb.append("value-").append(i).append('-');
      for (int j = 0; j < 60; j++) {
        sb.append((char) ('a' + ((i + j) % 26)));
      }
      strings[0] = sb.toString();
      assertTrue(page.appendRow(100L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    // The dictionary segment dominates: verify it decodes standalone and the whole re-assembles.
    final int dictIdx =
        RowGroupDescriptor.entryIndexOf(encoded.descriptor(), ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0));
    // Post-P7 the repetitive multi-KB dictionary FSST-compresses; the segment must still be
    // substantial (hundreds of entries) but far below the ~20KB raw dictionary bytes.
    assertTrue(RowGroupDescriptor.entryByteLen(encoded.descriptor(), dictIdx) > 1_000,
        "dict segment implausibly small: " + RowGroupDescriptor.entryByteLen(encoded.descriptor(), dictIdx));
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
  }

  // ==================== descriptor + provenance ====================

  @Test
  void descriptorMirrorsHeaderStatsAndFlags() {
    final ProjectionIndexRowGroupPage page = benchLeaf(512, 9_000L);
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final byte[] d = encoded.descriptor();
    assertEquals(512, RowGroupDescriptor.rowCount(d));
    assertEquals(4, RowGroupDescriptor.columnCount(d));
    assertEquals(page.firstRecordKey(), RowGroupDescriptor.firstRecordKey(d));
    assertEquals(page.lastRecordKey(), RowGroupDescriptor.lastRecordKey(d));
    for (int c = 0; c < 4; c++) {
      assertEquals(KINDS[c], RowGroupDescriptor.kind(d, c));
      final int bodyIdx = RowGroupDescriptor.entryIndexOf(d, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(c));
      assertEquals(page.columnMin(c), RowGroupDescriptor.entryMin(d, bodyIdx), "min mirror col " + c);
      assertEquals(page.columnMax(c), RowGroupDescriptor.entryMax(d, bodyIdx), "max mirror col " + c);
      // Mirror flags must equal segment TRUTH (the head byte of BODY bytes).
      final byte truth = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentFlags(
          resolverOf(encoded).segment(ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(c)));
      assertEquals(truth, RowGroupDescriptor.entryColFlags(d, bodyIdx), "flags mirror col " + c);
    }
    // Column 0 saw non-integral marks; column 3 is all-missing (no flags).
    final int body0 = RowGroupDescriptor.entryIndexOf(d, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    assertTrue(
        (RowGroupDescriptor.entryColFlags(d, body0) & ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL) != 0);
  }

  @Test
  void descriptorRejectsCorruption() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(10, 1L).serialize());
    final byte[] d = encoded.descriptor();
    // Truncated.
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(Arrays.copyOf(d, d.length - 1)));
    // Bad version.
    final byte[] badVersion = d.clone();
    badVersion[4] = 99;
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(badVersion));
    // Not a descriptor at all.
    assertNull(nullOrNot(new byte[] {1, 2, 3}));
  }

  private static Object nullOrNot(final byte[] bytes) {
    return RowGroupDescriptor.isDescriptor(bytes)
        ? bytes
        : null;
  }

  // ==================== integrity fail-loud ====================

  @Test
  void corruptedSegmentFailsHashCheckAtAssembly() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(256, 77L).serialize());
    final int victim = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    final ProjectionIndexColumnSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final ProjectionIndexColumnSegmentCodec.SegmentResolver corrupting = columnSegmentId -> {
      final byte[] bytes = clean.segment(columnSegmentId);
      if (columnSegmentId == victim && bytes != null) {
        final byte[] flipped = bytes.clone();
        flipped[flipped.length - 1] ^= 0x40;
        return flipped;
      }
      return bytes;
    };
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), corrupting));
    assertTrue(e.getMessage().contains("hash"), e.getMessage());
  }

  @Test
  void truncatedSegmentFailsLengthCheckAtAssembly() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(256, 77L).serialize());
    final ProjectionIndexColumnSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final ProjectionIndexColumnSegmentCodec.SegmentResolver truncating = columnSegmentId -> {
      final byte[] bytes = clean.segment(columnSegmentId);
      return columnSegmentId == 0 && bytes != null
          ? Arrays.copyOf(bytes, bytes.length - 3)
          : bytes;
    };
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), truncating));
    assertTrue(e.getMessage().contains("length"), e.getMessage());
  }

  @Test
  void missingSegmentFailsAtAssembly() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(64, 3L).serialize());
    final ProjectionIndexColumnSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final int missing = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2);
    final ProjectionIndexColumnSegmentCodec.SegmentResolver dropping = columnSegmentId -> columnSegmentId == missing
        ? null
        : clean.segment(columnSegmentId);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), dropping));
  }

  @Test
  void hashChangesWhenContentChangesAndIsStableOtherwise() {
    // The no-op comparator contract (§3): identical re-encode → identical hash; any value
    // change → different hash for that column's BODY only.
    final ProjectionIndexRowGroupPage a = benchLeaf(300, 5_000L);
    final ProjectionIndexRowGroupPage b = benchLeaf(300, 5_000L);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup ea =
        ProjectionIndexColumnSegmentCodec.encode(a.serialize());
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup eb =
        ProjectionIndexColumnSegmentCodec.encode(b.serialize());
    assertArrayEquals(ea.descriptor(), eb.descriptor(), "deterministic build → identical descriptor");
    for (int i = 0; i < ea.segments().length; i++) {
      assertArrayEquals(ea.segments()[i], eb.segments()[i]);
    }
    // Different data → the affected BODY hash differs.
    final ProjectionIndexRowGroupPage c = benchLeaf(300, 5_001L);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup ec =
        ProjectionIndexColumnSegmentCodec.encode(c.serialize());
    final int keysEntryA = RowGroupDescriptor.entryIndexOf(ea.descriptor(), 0);
    final int keysEntryC = RowGroupDescriptor.entryIndexOf(ec.descriptor(), 0);
    assertNotEquals(RowGroupDescriptor.entryContentHash(ea.descriptor(), keysEntryA),
        RowGroupDescriptor.entryContentHash(ec.descriptor(), keysEntryC),
        "shifted record keys must change the KEYS hash");
  }

  @Test
  void columnCapEnforced() {
    final byte[] tooMany = new byte[RowGroupDescriptor.MAX_COLUMNS + 1];
    Arrays.fill(tooMany, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG);
    assertThrows(IllegalArgumentException.class, () -> RowGroupDescriptor.serialize(0, 0L, 0L, tooMany, 0, new int[0],
        new int[0], new long[0], new byte[0], new long[0], new long[0]));
  }

  @Test
  void doubleColumnsRoundTripInTransformDomain() {
    // NUMERIC_DOUBLE cells store the order-preserving transform; at the codec layer the
    // column is byte-identical to NUMERIC_LONG (FOR bit-packing over transformed longs).
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[2];
    final boolean[] bools = new boolean[2];
    final String[] strings = new String[2];
    final boolean[] present = new boolean[2];
    final boolean[] unrep = new boolean[2];
    final boolean[] nonIntegral = new boolean[2];
    final double[] doubles = {-1.0e300, -2.25, -0.0, 0.0, 0.5, 3.1415926535, 8.0, 1.0e300};
    for (int i = 0; i < doubles.length; i++) {
      longs[0] = ProjectionDoubleEncoding.encode(doubles[i]);
      longs[1] = i * 10L;
      present[0] = true;
      present[1] = i % 2 == 0;
      nonIntegral[0] = i == 3; // a lossy-conversion mark must survive (value-exactness bit)
      assertTrue(page.appendRow(100L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    // Zone maps live in the transform domain: min/max mirror = encode(min double)/encode(max).
    final int body0 =
        RowGroupDescriptor.entryIndexOf(encoded.descriptor(), ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    assertEquals(ProjectionDoubleEncoding.encode(-1.0e300), RowGroupDescriptor.entryMin(encoded.descriptor(), body0));
    assertEquals(ProjectionDoubleEncoding.encode(1.0e300), RowGroupDescriptor.entryMax(encoded.descriptor(), body0));
    assertTrue(
        (RowGroupDescriptor.entryColFlags(encoded.descriptor(), body0)
            & ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL) != 0,
        "the value-exactness bit must survive the codec");
  }

  /** Build a single-string-column leaf whose dictionary holds {@code values}. */
  private static ProjectionIndexRowGroupPage stringLeaf(final String[] values) {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    for (int i = 0; i < values.length; i++) {
      strings[0] = values[i];
      assertTrue(page.appendRow(1000L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  @Test
  void fsstCompressedDictionaryRoundTripsAndShrinks() {
    // High-cardinality repetitive-prefix dictionary — the FSST target shape (P7, doc §2.7).
    final String[] values = new String[600];
    for (int i = 0; i < values.length; i++) {
      values[i] = "https://sirix.example.com/api/v1/resources/customer-records/region-europe/" + "tenant-" + (i % 37)
          + "/entity-" + i;
    }
    final ProjectionIndexRowGroupPage page = stringLeaf(values);
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    // Byte-identity is the load-bearing contract — FSST must be invisible above the codec.
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    // And it must actually compress: the DICT segment holds ~48KB of URLs.
    final int dictIdx =
        RowGroupDescriptor.entryIndexOf(encoded.descriptor(), ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0));
    final int dictLen = RowGroupDescriptor.entryByteLen(encoded.descriptor(), dictIdx);
    int rawDictBytes = 0;
    for (final String v : values) {
      rawDictBytes += v.getBytes(StandardCharsets.UTF_8).length;
    }
    assertTrue(dictLen * 2 < rawDictBytes,
        "FSST should compress repetitive URLs >2x, got " + rawDictBytes + " -> " + dictLen);
  }

  @Test
  void fsstEncodingIsDeterministicAcrossIdenticalReencodes() {
    // The write-path no-op comparator hashes segment bytes: identical dictionaries must
    // encode to identical bytes (deterministic training over interning order) — 5.2-n.
    final String[] values = new String[300];
    for (int i = 0; i < values.length; i++) {
      values[i] = "prefix-common-part-shared/suffix-" + i + "/tail-" + (i % 7);
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup a =
        ProjectionIndexColumnSegmentCodec.encode(stringLeaf(values).serialize());
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup b =
        ProjectionIndexColumnSegmentCodec.encode(stringLeaf(values).serialize());
    assertArrayEquals(a.descriptor(), b.descriptor());
    for (int i = 0; i < a.segments().length; i++) {
      assertArrayEquals(a.segments()[i], b.segments()[i]);
    }
  }

  @Test
  void ownerWorkspaceSurvivesFsstRawFsstAndFailureReuse() {
    final String[] compressible = new String[300];
    for (int i = 0; i < compressible.length; i++) {
      compressible[i] =
          "https://sirix.example/api/owner-workspace/tenant-" + (i % 23) + "/entity-" + i + "/shared-tail";
    }
    final byte[] fsstRaw = stringLeaf(compressible).serialize();
    final byte[] smallRaw = stringLeaf(new String[] {"one", "two", "three"}).serialize();
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup first =
        ProjectionIndexColumnSegmentCodec.encode(fsstRaw, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup firstSnapshot = snapshot(first);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup middle =
        ProjectionIndexColumnSegmentCodec.encode(smallRaw, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup middleSnapshot = snapshot(middle);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup second =
        ProjectionIndexColumnSegmentCodec.encode(fsstRaw, workspace);

    assertEquals(1, segmentOf(first, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        0))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "first dictionary must use FSST");
    assertEquals(0, segmentOf(middle, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        0))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "middle dictionary must use RAW");
    assertEncodedEquals(firstSnapshot, first);
    assertEncodedEquals(middleSnapshot, middle);
    assertEncodedEquals(firstSnapshot, second);
    assertOutputsDetached(first, middle);
    assertOutputsDetached(first, second);
    assertOutputsDetached(middle, second);
    assertArrayEquals(fsstRaw,
        ProjectionIndexColumnSegmentCodec.assembleRaw(first.descriptor().clone(), coldResolverOf(first)),
        "detached descriptor + segment copies must cold-read byte-identically");
    assertArrayEquals(smallRaw,
        ProjectionIndexColumnSegmentCodec.assembleRaw(middle.descriptor().clone(), coldResolverOf(middle)));

    assertThrows(RuntimeException.class,
        () -> ProjectionIndexColumnSegmentCodec.encode(new byte[] {1, 2, 3}, workspace));
    assertEncodedEquals(firstSnapshot, first);
    assertEncodedEquals(middleSnapshot, middle);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup afterFailure =
        ProjectionIndexColumnSegmentCodec.encode(fsstRaw, workspace);
    assertEncodedEquals(firstSnapshot, afterFailure);
    assertEncodedEquals(firstSnapshot, first);
    assertEncodedEquals(middleSnapshot, middle);
    assertOutputsDetached(second, afterFailure);
  }

  @Test
  void sameWorkspaceDoubleClaimFailsClosedWithoutStealingTheOwner() {
    final byte[] raw = benchLeaf(32, 1_000L).serialize();
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();

    workspace.claim();
    try {
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> ProjectionIndexColumnSegmentCodec.encode(raw, workspace));
      assertEquals("Projection encode workspace is already in use", failure.getMessage());
      assertThrows(IllegalStateException.class,
          () -> ProjectionIndexColumnSegmentCodec.encode(raw, workspace),
          "a rejected claimant must not release the active owner's claim");
    } finally {
      workspace.release();
    }

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup recovered =
        ProjectionIndexColumnSegmentCodec.encode(raw, workspace);
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(recovered.descriptor(), resolverOf(recovered)),
        "the owning thread's release must make the workspace reusable");
  }

  @Test
  void borrowedLivePagesEncodeByteIdenticallyAcrossMixedSetAndFsstToRawShapes() {
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace rawWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace liveWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();

    // Numeric + boolean + scalar string dictionary, including missing cells and provenance flags.
    assertLiveEncodingEqualsRaw(benchLeaf(257, 8_000L), rawWorkspace, liveWorkspace);

    // Variable-width STRING_SET body, dictionary, per-value counts and bloom segments.
    assertLiveEncodingEqualsRaw(setLeaf(), rawWorkspace, liveWorkspace);

    // Exercise workspace state transition from a trained FSST dictionary to a small RAW one on
    // both sides. Equality covers descriptor hashes, segment ids and every segment byte.
    final String[] compressible = new String[300];
    for (int i = 0; i < compressible.length; i++) {
      compressible[i] = "https://sirix.example/live-page/tenant-" + (i % 23) + "/entity-" + i + "/shared-tail";
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup fsst =
        assertLiveEncodingEqualsRaw(stringLeaf(compressible), rawWorkspace, liveWorkspace);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup raw =
        assertLiveEncodingEqualsRaw(stringLeaf(new String[] {"one", "two", "three"}), rawWorkspace, liveWorkspace);
    assertEquals(1, segmentOf(fsst, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        0))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "large dictionary must use FSST");
    assertEquals(0, segmentOf(raw, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        0))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "small dictionary must fall back to RAW");
  }

  @Test
  void spliceColumnsBatchesNonAdjacentChangesAndExcludesSelectedNoop() {
    final ProjectionIndexRowGroupPage prior = new ProjectionIndexRowGroupPage(KINDS);
    final ProjectionIndexRowGroupPage expected = new ProjectionIndexRowGroupPage(KINDS);
    final ProjectionIndexRowGroupPage replacement0 = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final ProjectionIndexRowGroupPage replacement2 = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
    final ProjectionIndexRowGroupPage replacement3 = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final boolean[] allPresent = {true, true, true, true};

    for (int row = 0; row < 32; row++) {
      final long recordKey = 10_000L + row * 3L;
      final long[] priorLongs = {100L + row, 0L, 0L, 900L + row};
      final boolean[] bools = {false, (row & 1) == 0, false, false};
      final String[] priorStrings = {"", "", DEPTS[row % DEPTS.length], ""};
      assertTrue(prior.appendRow(recordKey, priorLongs, bools, priorStrings, allPresent, new boolean[4],
          new boolean[4]));

      final long[] nextLongs = priorLongs.clone();
      final String[] nextStrings = priorStrings.clone();
      if (row == 5) {
        nextLongs[0] += 1_000L;
      }
      if (row == 10) {
        nextStrings[2] = DEPTS[3];
      }
      assertTrue(expected.appendRow(recordKey, nextLongs, bools, nextStrings, allPresent, new boolean[4],
          new boolean[4]));
      assertTrue(replacement0.appendRow(recordKey, new long[] {nextLongs[0]}, new boolean[1], new String[1],
          new boolean[] {true}, new boolean[1], new boolean[1]));
      assertTrue(replacement2.appendRow(recordKey, new long[1], new boolean[1], new String[] {nextStrings[2]},
          new boolean[] {true}, new boolean[1], new boolean[1]));
      assertTrue(replacement3.appendRow(recordKey, new long[] {nextLongs[3]}, new boolean[1], new String[1],
          new boolean[] {true}, new boolean[1], new boolean[1]));
    }

    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedPrior =
        ProjectionIndexColumnSegmentCodec.encode(prior, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn encoded0 =
        ProjectionIndexColumnSegmentCodec.encodeColumn(replacement0, 0, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn encoded2 =
        ProjectionIndexColumnSegmentCodec.encodeColumn(replacement2, 2, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn encoded3 =
        ProjectionIndexColumnSegmentCodec.encodeColumn(replacement3, 3, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn[] replacements = {encoded0, encoded2, encoded3};
    final long[] actuallyChanged = {-1L, -1L};

    final byte[] patched = ProjectionIndexColumnSegmentCodec.spliceColumns(encodedPrior.descriptor(), replacements,
        replacements.length, actuallyChanged);

    assertArrayEquals(new long[] {(1L << 0) | (1L << 2), 0L}, actuallyChanged,
        "the selected but byte-identical column 3 must not enter the changed bitmap");
    RowGroupDescriptor.validate(patched);
    final int[] expectedIds = {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(2),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(3),
        ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(2)};
    final int[] patchedIds = new int[RowGroupDescriptor.columnSegmentCount(patched)];
    for (int entry = 0; entry < patchedIds.length; entry++) {
      patchedIds[entry] = RowGroupDescriptor.entryColumnSegmentId(patched, entry);
    }
    assertArrayEquals(expectedIds, encodedPrior.columnSegmentIds(), "test premise: canonical prior segment set");
    assertArrayEquals(expectedIds, patchedIds, "the batched splice must retain the canonical segment set");

    final int changedBody0 = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    final int changedBody2 = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2);
    for (final int id : expectedIds) {
      final int priorEntry = RowGroupDescriptor.entryIndexOf(encodedPrior.descriptor(), id);
      final int patchedEntry = RowGroupDescriptor.entryIndexOf(patched, id);
      if (id == changedBody0 || id == changedBody2) {
        assertNotEquals(RowGroupDescriptor.entryContentHash(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryContentHash(patched, patchedEntry), "changed BODY " + id);
      } else {
        assertEquals(RowGroupDescriptor.entryByteLen(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryByteLen(patched, patchedEntry), "carried length " + id);
        assertEquals(RowGroupDescriptor.entryContentHash(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryContentHash(patched, patchedEntry), "carried hash " + id);
        assertEquals(RowGroupDescriptor.entryColFlags(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryColFlags(patched, patchedEntry), "carried flags " + id);
        assertEquals(RowGroupDescriptor.entryMin(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryMin(patched, patchedEntry), "carried min " + id);
        assertEquals(RowGroupDescriptor.entryMax(encodedPrior.descriptor(), priorEntry),
            RowGroupDescriptor.entryMax(patched, patchedEntry), "carried max " + id);
      }
    }

    final Map<Integer, byte[]> segmentById = new HashMap<>();
    for (int i = 0; i < encodedPrior.columnSegmentIds().length; i++) {
      segmentById.put(encodedPrior.columnSegmentIds()[i], encodedPrior.segments()[i]);
    }
    for (final ProjectionIndexColumnSegmentCodec.EncodedColumn replacement : replacements) {
      for (int i = 0; i < replacement.columnSegmentIds().length; i++) {
        segmentById.put(replacement.columnSegmentIds()[i], replacement.segments()[i]);
      }
    }
    assertArrayEquals(expected.serialize(), ProjectionIndexColumnSegmentCodec.assembleRaw(patched, segmentById::get),
        "the canonical batched segment set must assemble to the fully updated row group");
  }

  @Test
  void slabAndColdReopenedLegacyDictionaryHaveGoldenWireSegmentAndHashParity() {
    final String[] values = new String[132];
    for (int i = 0; i < 128; i++) {
      values[i] = "https://sirix.example/Grüße/世界/🦄/tenant-" + (i % 17) + "/entity-" + i + "/shared-repetitive-tail";
    }
    values[128] = "";
    values[129] = values[7];
    values[130] = "emoji-🎯-mix";
    values[131] = values[7];
    final ProjectionIndexRowGroupPage slab = stringLeaf(values);
    assertTrue(slab.stringDictionaryIsSlabBacked(0));
    final byte[] raw = slab.serialize();
    final ProjectionIndexRowGroupPage reopened = ProjectionIndexRowGroupPage.deserialize(raw.clone());
    assertTrue(!reopened.stringDictionaryIsSlabBacked(0),
        "cold raw deserialisation must retain the historical byte[][] representation");

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup slabEncoded =
        ProjectionIndexColumnSegmentCodec.encode(slab,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup legacyEncoded =
        ProjectionIndexColumnSegmentCodec.encode(reopened,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
    assertEncodedEquals(legacyEncoded, slabEncoded);
    assertEquals(1, segmentOf(slabEncoded, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        0))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "golden corpus must exercise flat FSST");
    assertArrayEquals(raw,
        ProjectionIndexColumnSegmentCodec.assembleRaw(slabEncoded.descriptor().clone(), coldResolverOf(slabEncoded)),
        "detached cold segments must reopen to the exact raw leaf bytes");
  }

  @Test
  void borrowedEncoderOutputsStayDetachedWhenTheBuilderPageMutatesLater() {
    final ProjectionIndexRowGroupPage page = benchLeaf(32, 90_000L);
    final byte[] beforeMutation = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expected =
        ProjectionIndexColumnSegmentCodec.encode(beforeMutation,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup borrowed =
        ProjectionIndexColumnSegmentCodec.encode(page,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());

    final long[] longs = {41L, 0L, 0L, 0L};
    final boolean[] bools = {false, true, false, false};
    final String[] strings = {"", "", "after-encode", ""};
    final boolean[] present = {true, true, true, false};
    assertTrue(page.appendRow(100_000L, longs, bools, strings, present, new boolean[4], new boolean[4]));

    // Any alias to page-owned columns/dictionaries would make this differ after appendRow.
    assertEncodedEquals(expected, borrowed);
    assertArrayEquals(beforeMutation,
        ProjectionIndexColumnSegmentCodec.assembleRaw(borrowed.descriptor(), resolverOf(borrowed)));
  }

  @Test
  void distinctOwnerWorkspacesAreIsolatedDuringConcurrentEncoding() throws Exception {
    final String[] valuesA = new String[256];
    final String[] valuesB = new String[256];
    for (int i = 0; i < valuesA.length; i++) {
      valuesA[i] = "aaaaaaaa/catalog/tenant-" + (i % 17) + "/entity-" + i + "/tail-alpha";
      valuesB[i] = "zzzzzzzz/archive/group-" + (i % 19) + "/object-" + i + "/tail-omega";
    }
    final byte[] rawA = stringLeaf(valuesA).serialize();
    final byte[] rawB = stringLeaf(valuesB).serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expectedA =
        ProjectionIndexColumnSegmentCodec.encode(rawA);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expectedB =
        ProjectionIndexColumnSegmentCodec.encode(rawB);
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<ProjectionIndexColumnSegmentCodec.EncodedRowGroup> futureA = executor.submit(() -> {
        start.await();
        return ProjectionIndexColumnSegmentCodec.encode(rawA,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
      });
      final Future<ProjectionIndexColumnSegmentCodec.EncodedRowGroup> futureB = executor.submit(() -> {
        start.await();
        return ProjectionIndexColumnSegmentCodec.encode(rawB,
            new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
      });
      start.countDown();
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup actualA = futureA.get(30, TimeUnit.SECONDS);
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup actualB = futureB.get(30, TimeUnit.SECONDS);
      assertEncodedEquals(expectedA, actualA);
      assertEncodedEquals(expectedB, actualB);
      assertArrayEquals(rawA,
          ProjectionIndexColumnSegmentCodec.assembleRaw(actualA.descriptor().clone(), coldResolverOf(actualA)));
      assertArrayEquals(rawB,
          ProjectionIndexColumnSegmentCodec.assembleRaw(actualB.descriptor().clone(), coldResolverOf(actualB)));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void escapeHeavyAndSmallDictionariesTakeTheRawPathAndRoundTrip() {
    // Escape-heavy: bytes ≥ 0x80 and 0xFF everywhere — FSST's escape coding worst case; the
    // beneficial-gate must refuse and fall back to RAW, and either way bytes round-trip.
    final String[] hostile = new String[80];
    for (int i = 0; i < hostile.length; i++) {
      hostile[i] = "\u00ff\u00fe\u30c6\u30b9\u30c8-" + i + "-\u00ff\u00ff";
    }
    assertRoundTrip(stringLeaf(hostile));
    // Small dictionary: below the table gates — RAW mode, still byte-identical.
    assertRoundTrip(stringLeaf(new String[] {"a", "b", "c"}));
  }

  @Test
  void utf8DictionaryBytesSurviveExactly() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    final String[] values = {"ascii", "umläut-Straße", "日本語テキスト", "emoji-🎯-mix", ""};
    for (int i = 0; i < values.length; i++) {
      strings[0] = values[i];
      assertTrue(page.appendRow(500L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    // Sanity that multi-byte UTF-8 really is in play.
    assertTrue(values[2].getBytes(StandardCharsets.UTF_8).length > values[2].length());
    assertRoundTrip(page);
  }

  // ==================== sole descriptor / segment-slot format ====================

  @Test
  void canonicalWriterEmitsTheExactSegmentSchemaForEveryColumnKindAndRowCount() {
    final byte[] bodyOnlyKinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL};
    for (final byte kind : bodyOnlyKinds) {
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup populated =
          ProjectionIndexColumnSegmentCodec.encode(singleKindLeaf(kind).serialize());
      assertArrayEquals(new int[] {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
          ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)}, populated.columnSegmentIds(),
          "populated kind " + kind);
      RowGroupDescriptor.validate(populated.descriptor());
    }

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup localString = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT).serialize());
    assertArrayEquals(new int[] {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(0)}, localString.columnSegmentIds());
    RowGroupDescriptor.validate(localString.descriptor());

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup stringSet = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET).serialize());
    assertArrayEquals(new int[] {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0)}, stringSet.columnSegmentIds());
    RowGroupDescriptor.validate(stringSet.descriptor());

    final byte[] allKinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL};
    for (final byte kind : allKinds) {
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup empty = ProjectionIndexColumnSegmentCodec.encode(
          new ProjectionIndexRowGroupPage(new byte[] {kind}).serialize());
      assertArrayEquals(new int[] {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
          ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)}, empty.columnSegmentIds(), "empty kind " + kind);
      RowGroupDescriptor.validate(empty.descriptor());
    }
  }

  @Test
  void canonicalSchemaRejectsEveryMissingMandatorySegment() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup localString = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT).serialize());
    final int[] mandatoryLocalString = {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(0)};
    for (final int missing : mandatoryLocalString) {
      assertThrows(IllegalStateException.class,
          () -> RowGroupDescriptor.validate(descriptorWithoutId(localString.descriptor(), missing)),
          "missing mandatory segment " + missing + " was accepted");
    }

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup stringSet = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET).serialize());
    for (final int missing : new int[] {ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0)}) {
      assertThrows(IllegalStateException.class,
          () -> RowGroupDescriptor.validate(descriptorWithoutId(stringSet.descriptor(), missing)),
          "missing mandatory STRING_SET segment " + missing + " was accepted");
    }

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup empty = ProjectionIndexColumnSegmentCodec.encode(
        new ProjectionIndexRowGroupPage(
            new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT}).serialize());
    for (final int missing : empty.columnSegmentIds()) {
      assertThrows(IllegalStateException.class,
          () -> RowGroupDescriptor.validate(descriptorWithoutId(empty.descriptor(), missing)),
          "empty leaf accepted missing mandatory segment " + missing);
    }
  }

  @Test
  void canonicalSchemaRejectsExtraUnknownAndDuplicateSegmentIds() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup numeric = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG).serialize());
    assertThrows(IllegalStateException.class,
        () -> RowGroupDescriptor.validate(descriptorWithIds(numeric.descriptor(), 0, 1, 2)),
        "a numeric column cannot declare a DICT segment");

    final int unknownId = ProjectionIndexColumnSegmentCodec.DICT_HASH_SEGMENT_BASE - 1;
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexColumnSegmentCodec.expectedSegmentKind(unknownId));
    assertThrows(IllegalStateException.class,
        () -> RowGroupDescriptor.validate(descriptorWithIds(numeric.descriptor(), 0, 1, unknownId)));

    final byte[] duplicate = numeric.descriptor().clone();
    putSegmentId(duplicate, 1, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId());
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(duplicate));

    final byte[] unknownKind = numeric.descriptor().clone();
    unknownKind[27] = (byte) 0x7F;
    assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(unknownKind));

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup stringSet = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET).serialize());
    final int[] setIdsWithForbiddenHash = Arrays.copyOf(stringSet.columnSegmentIds(),
        stringSet.columnSegmentIds().length + 1);
    setIdsWithForbiddenHash[setIdsWithForbiddenHash.length - 1] =
        ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(0);
    assertThrows(IllegalStateException.class,
        () -> RowGroupDescriptor.validate(descriptorWithIds(stringSet.descriptor(), setIdsWithForbiddenHash)),
        "STRING_SET must not carry the scalar STRING_DICT hash segment");
  }

  @Test
  void nonBodySegmentsRejectEveryBodyOnlyMirrorField() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup localString = ProjectionIndexColumnSegmentCodec.encode(
        singleKindLeaf(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT).serialize());
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup stringSet = ProjectionIndexColumnSegmentCodec.encode(
        setLeaf().serialize());
    final int[] localStringNonBodies = {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0),
        ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(0)};
    for (final int id : localStringNonBodies) {
      assertNonBodyMirrorsRejected(localString, id);
    }
    assertNonBodyMirrorsRejected(stringSet, ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(0));
  }

  @Test
  void descriptorMirrorsMustEqualKeysAndBodySegmentTruth() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(64, 1_000L).serialize());

    final byte[] wrongFirstKey = encoded.descriptor().clone();
    RowGroupDescriptor.putLongLE(wrongFirstKey, 11, RowGroupDescriptor.firstRecordKey(wrongFirstKey) + 1);
    RowGroupDescriptor.validate(wrongFirstKey);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(wrongFirstKey, resolverOf(encoded)));

    final int bodyEntry = RowGroupDescriptor.entryIndexOf(encoded.descriptor(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    final int bodyOffset = descriptorEntryOffset(encoded.descriptor(), bodyEntry);
    final byte[] wrongFlags = encoded.descriptor().clone();
    wrongFlags[bodyOffset + 14] ^= ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL;
    RowGroupDescriptor.validate(wrongFlags);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(wrongFlags, resolverOf(encoded)));

    final byte[] wrongMin = encoded.descriptor().clone();
    RowGroupDescriptor.putLongLE(wrongMin, bodyOffset + 15, RowGroupDescriptor.entryMin(wrongMin, bodyEntry) + 1);
    RowGroupDescriptor.validate(wrongMin);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(wrongMin, resolverOf(encoded)));

    final byte[] wrongMax = encoded.descriptor().clone();
    RowGroupDescriptor.putLongLE(wrongMax, bodyOffset + 23, RowGroupDescriptor.entryMax(wrongMax, bodyEntry) - 1);
    RowGroupDescriptor.validate(wrongMax);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexColumnSegmentCodec.assembleRaw(wrongMax, resolverOf(encoded)));
  }

  @Test
  void lazyDirectoryRejectsUndeclaredAndDuplicateSegmentSlots() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(benchLeaf(64, 10L).serialize());
    final int count = encoded.columnSegmentIds().length;
    final long[] offsets = new long[count];
    Arrays.fill(offsets, 1L);
    new ProjectionIndexHOTStorage.RowGroupDirectory(1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
        new byte[count][]);

    final int[] duplicate = encoded.columnSegmentIds().clone();
    duplicate[duplicate.length - 1] = duplicate[duplicate.length - 2];
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexHOTStorage.RowGroupDirectory(1L, encoded.descriptor(), duplicate, offsets,
            new byte[count][]));

    final int[] undeclared = Arrays.copyOf(encoded.columnSegmentIds(), count + 1);
    undeclared[count] = ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(3);
    final long[] extraOffsets = Arrays.copyOf(offsets, count + 1);
    extraOffsets[count] = 2L;
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexHOTStorage.RowGroupDirectory(1L, encoded.descriptor(), undeclared, extraOffsets,
            new byte[count + 1][]));
  }

  @Test
  void everyEmittedSetCountsSegmentFitsCompletelyInsideIts512ByteSlot() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup small =
        ProjectionIndexColumnSegmentCodec.encode(setLeaf().serialize());
    final byte[] smallCounts = segmentOf(small, ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(0));
    assertNotNull(smallCounts, "the compact fixture must emit SET_COUNTS");
    assertTrue(smallCounts.length <= ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES);

    final String[] boundaryValues = new String[21];
    for (int value = 0; value < boundaryValues.length; value++) {
      boundaryValues[value] = String.format("%020d", value);
    }
    boundaryValues[0] += "xx"; // descriptor payload fits 512; the six-byte PIXS header does not.
    final ProjectionIndexRowGroupPage boundary =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET});
    appendSet(boundary, 1L, boundaryValues);
    final byte[] boundaryCounts = segmentOf(ProjectionIndexColumnSegmentCodec.encode(boundary.serialize()),
        ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(0));
    assertTrue(boundaryCounts == null || boundaryCounts.length <= ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "SET_COUNTS eligibility must include the complete PIXS header, not just its payload");
  }

  @Test
  void encoderEmitsExactZoneMapOnlyDescriptor() {
    final byte[] raw = benchLeaf(300, 5L).serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final byte[] descriptor = encoded.descriptor();
    RowGroupDescriptor.validate(descriptor);
    final int expectedLength = 27 + RowGroupDescriptor.columnCount(descriptor) + 2
        + RowGroupDescriptor.columnSegmentCount(descriptor) * RowGroupDescriptor.ENTRY_BYTES;
    assertEquals(expectedLength, descriptor.length,
        "the descriptor must end at its entry table and contain no payload region");
    assertArrayEquals(raw, ProjectionIndexColumnSegmentCodec.assembleRaw(descriptor, resolverOf(encoded)));
  }

  @Test
  void descriptorRejectsTrailingPayloadBytes() {
    final byte[] descriptor = ProjectionIndexColumnSegmentCodec.encode(benchLeaf(64, 9L).serialize()).descriptor();
    final byte[] withTrailingPayload = Arrays.copyOf(descriptor, descriptor.length + 1);
    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(withTrailingPayload));
    assertTrue(failure.getMessage().contains("length"), failure.getMessage());
  }

  @Test
  void descriptorRejectsFormerInlineMarker() {
    final byte[] descriptor = ProjectionIndexColumnSegmentCodec.encode(benchLeaf(64, 9L).serialize())
                                                                  .descriptor()
                                                                  .clone();
    final int firstEntryByteLen = 27 + RowGroupDescriptor.columnCount(descriptor) + 2 + 2;
    descriptor[firstEntryByteLen + 3] |= (byte) 0x80;
    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> RowGroupDescriptor.validate(descriptor));
    assertTrue(failure.getMessage().contains("byteLen"), failure.getMessage());
  }

  @Test
  void serializeAllowsWideZoneMapDescriptorPastTheU16Length() {
    final int columns = 2_200;
    final int entries = columns + 1;
    final byte[] kinds = new byte[columns];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG);
    final int[] ids = new int[entries];
    final int[] byteLens = new int[entries];
    final long[] hashes = new long[entries];
    final byte[] flags = new byte[entries];
    final long[] mins = new long[entries];
    final long[] maxs = new long[entries];
    ids[0] = ProjectionIndexColumnSegmentCodec.keysColumnSegmentId();
    byteLens[0] = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 2 * Long.BYTES + 1
        + 2 * Integer.BYTES;
    for (int column = 0; column < columns; column++) {
      final int entry = column + 1;
      ids[entry] = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column);
      byteLens[entry] = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 1;
      mins[entry] = Long.MAX_VALUE;
      maxs[entry] = Long.MIN_VALUE;
    }
    final byte[] descriptor = RowGroupDescriptor.serialize(0, Long.MAX_VALUE, Long.MIN_VALUE, kinds, entries, ids,
        byteLens, hashes, flags, mins, maxs);
    RowGroupDescriptor.validate(descriptor);
    assertTrue(descriptor.length > 0xFFFF,
        "a wide canonical descriptor must remain serializable for OverflowPage-backed blob storage");
  }

  @Test
  void overflowPageImposesNoSizeCeiling() {
    // OverflowPage is SHARED with node-record spill, which is unbounded: a single large string or
    // binary value legitimately produces a >16 MB page. A ceiling here would reject valid user data
    // at commit AND make already-committed pages of that size unreadable, so the page must accept
    // any length. The projection's own limit lives on RowGroupDescriptor instead.
    assertEquals(RowGroupDescriptor.MAX_SEGMENT_BYTES + 1,
        new OverflowPage(new byte[RowGroupDescriptor.MAX_SEGMENT_BYTES + 1]).getDataBytes().length);
    assertEquals(16, new OverflowPage(new byte[16]).getDataBytes().length);
    assertThrows(IllegalArgumentException.class, () -> new OverflowPage(null));
  }

  @Test
  void descriptorSerializeRejectsOutOfRangeSegmentLength() {
    final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> RowGroupDescriptor.serialize(0, 0L, 0L, new byte[0], 1, new int[] {0},
            new int[] {RowGroupDescriptor.MAX_SEGMENT_BYTES + 1}, new long[] {0L}, new byte[] {0}, new long[] {0L},
            new long[] {0L}));
    assertTrue(failure.getMessage().contains("byteLen"), failure.getMessage());
  }

  // ==================== flat dictionary slices ====================

  /** One STRING_DICT leaf whose column {@code 0} holds {@code values} (null = MISSING row). */
  private static ProjectionIndexRowGroupPage sparseStringLeaf(final String[] values) {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = new boolean[1];
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    for (int i = 0; i < values.length; i++) {
      strings[0] = values[i] == null
          ? ""
          : values[i];
      present[0] = values[i] != null;
      assertTrue(page.appendRow(1000L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  /**
   * Every present row of a STRING_DICT leaf, read back through the flat dictionary — the property the
   * per-entry {@code byte[][]} form used to give for free and the flat form has to earn from its
   * offsets.
   */
  private static void assertFlatDictReproduces(final String[] values, final boolean expectAliasedSegment) {
    final byte[] raw = sparseStringLeaf(values).serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final ProjectionIndexColumnSegmentCodec.SegmentResolver resolver = resolverOf(encoded);
    final byte[] body = resolver.segment(ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    final byte[] dict = resolver.segment(ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0));
    final ProjectionColumnStore.ColumnSlice slice =
        ProjectionIndexColumnSegmentCodec.decodeStringSlice(encoded.descriptor(), body, dict, 0);
    assertEquals(values.length, slice.rowCount());
    // Which of the two decoders ran, read off the shape of the flat run rather than a mode byte: a
    // RAW dictionary is already concatenated on the wire, so the run ALIASES the segment and its
    // offsets start past the header; an FSST one must be expanded into an exactly-sized buffer.
    final int end = slice.dictOffset(slice.dictSize());
    if (expectAliasedSegment) {
      assertTrue(slice.dictOffset(0) > 0, "a raw dictionary must alias the segment past its header, not be copied out");
    } else {
      assertEquals(0, slice.dictOffset(0), "an expanded dictionary starts at 0");
      assertEquals(end, slice.dictBytes().length, "an expanded dictionary must be exactly sized");
    }
    for (int r = 0; r < values.length; r++) {
      final boolean present = (slice.presenceWords()[r >>> 6] & 1L << (r & 63)) != 0L;
      assertEquals(values[r] != null, present, "presence of row " + r);
      if (!present) {
        continue;
      }
      final int id = slice.stringDictIds()[r];
      assertTrue(id >= 0 && id < slice.dictSize(), "dict id " + id + " out of range at row " + r);
      assertEquals(values[r], slice.dictString(id), "row " + r);
      assertArrayEquals(values[r].getBytes(StandardCharsets.UTF_8),
          Arrays.copyOfRange(slice.dictBytes(), slice.dictOffset(id), slice.dictOffset(id) + slice.dictLength(id)),
          "row " + r + " bytes");
    }
  }

  @Test
  void rawDictionarySliceAliasesTheSegmentAndReadsExactly() {
    final String[] values = new String[256];
    for (int i = 0; i < values.length; i++) {
      // Few distinct values and few total bytes: below the FSST gates, so the RAW mode is taken.
      values[i] = i % 7 == 0
          ? null
          : DEPTS[i % DEPTS.length];
    }
    assertFlatDictReproduces(values, true);
  }

  @Test
  void fsstDictionarySliceExpandsEveryEntryExactly() {
    // The shape that made the flat form worth building: a near-unique column whose per-leaf
    // dictionary is nearly as large as the leaf, well past FSST's sample and byte gates.
    final String[] values = new String[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = "did:plc:" + String.format("%024x", i * 0x9E3779B97F4A7C15L);
    }
    assertFlatDictReproduces(values, false);
  }

  @Test
  void flatDictionaryHandlesSupplementaryAndEmptyEntries() {
    // Multi-byte and 4-byte UTF-8 beside a genuine empty string: the offsets are BYTE offsets, so a
    // codepoint-vs-byte confusion anywhere in the decode shows up as a wrong value here.
    final String[] values = {"", "\u00e4\u00f6\u00fc", "\uD83D\uDE00 emoji", null, "plain", "", "\uD83D\uDE00"};
    assertFlatDictReproduces(values, true);
  }
}
