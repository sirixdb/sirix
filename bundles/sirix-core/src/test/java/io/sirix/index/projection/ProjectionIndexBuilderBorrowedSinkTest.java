/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ownership and compatibility coverage for the builder's synchronous borrowed-page boundary. */
final class ProjectionIndexBuilderBorrowedSinkTest {

  private static final int SAMPLE_LEAVES = 16;
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static final byte[] REUSE_KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
          ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};

  @Test
  void sixteenPageSampleDrainsSynchronouslyAndMatchesTheRawAdapterByteForByte() {
    final List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(SAMPLE_LEAVES);
    for (int i = 0; i < SAMPLE_LEAVES; i++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      assertTrue(page.appendRow(10_000L + i, new long[] {i, 0L}, new boolean[2], new String[] {"", "sample-" + i}));
      assertTrue(page.stringDictionaryIsSlabBacked(1));
      sample.add(page);
    }

    final GlobalValueDictionaryWriter[] dictionaries = new GlobalValueDictionaryWriter[KINDS.length];
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup[] direct =
        new ProjectionIndexColumnSegmentCodec.EncodedRowGroup[SAMPLE_LEAVES];
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace directWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final int[] callbackIndex = {0};
    ProjectionIndexBuilder.emitBorrowedSample(sample, dictionaries, page -> {
      final int index = callbackIndex[0]++;
      assertSame(sample.get(index), page, "the drain must borrow the exact converted sample page in order");
      direct[index] = ProjectionIndexColumnSegmentCodec.encode(page, directWorkspace);
    });
    assertEquals(SAMPLE_LEAVES, callbackIndex[0]);

    // The unchanged public Consumer<byte[]> surface serialises synchronously at the same boundary.
    final List<byte[]> rawPayloads = new ArrayList<>(SAMPLE_LEAVES);
    ProjectionIndexBuilder.emitBorrowedSample(sample, dictionaries,
        ProjectionIndexBuilder.serializingLeafSink(rawPayloads::add));
    assertEquals(SAMPLE_LEAVES, rawPayloads.size());

    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace rawWorkspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    for (int i = 0; i < SAMPLE_LEAVES; i++) {
      assertArrayEquals(sample.get(i).serialize(), rawPayloads.get(i), "raw adapter reordered sample page " + i);
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup viaRaw =
          ProjectionIndexColumnSegmentCodec.encode(rawPayloads.get(i), rawWorkspace);
      assertEncodedEquals(direct[i], viaRaw, i);
    }
  }

  @Test
  void sixteenPageSampleSurvivesElectionAndBorrowBeforeOnlyItsFinalPageBecomesReusable() {
    final List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(SAMPLE_LEAVES);
    final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
    final GlobalValueDictionaryWriter[] dictionaries = {null, dictionary};
    final byte[][] convertedWire = new byte[SAMPLE_LEAVES][];
    for (int i = 0; i < SAMPLE_LEAVES; i++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      assertTrue(page.appendRow(20_000L + i, new long[] {i, 0L}, new boolean[2], new String[] {"", "elected-" + i}));
      assertTrue(page.stringDictionaryIsSlabBacked(1), "every leading sample leaf must convert from slab ranges");
      page.convertStringDictColumnToGlobal(1, dictionary);
      assertFalse(page.stringDictionaryIsSlabBacked(1));
      convertedWire[i] = page.serialize();
      sample.add(page);
    }

    final int[] callbackIndex = {0};
    final ProjectionIndexRowGroupPage reusable =
        ProjectionIndexBuilder.emitBorrowedSampleForReuse(sample, dictionaries, page -> {
          final int index = callbackIndex[0]++;
          assertSame(sample.get(index), page);
          assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, page.columnKind(1));
          assertArrayEquals(convertedWire[index], page.serialize(),
              "sample page was reset before its synchronous borrower returned");
        });

    assertEquals(SAMPLE_LEAVES, callbackIndex[0]);
    assertSame(sample.get(SAMPLE_LEAVES - 1), reusable,
        "the last sample page should seed the one-page steady-state reuse slot");
    for (int i = 0; i < SAMPLE_LEAVES - 1; i++) {
      assertArrayEquals(convertedWire[i], sample.get(i).serialize(),
          "earlier sample pages must not be recycled while the sample is being drained");
    }
    assertEquals(0, reusable.getRowCount());
    assertEquals(Long.MAX_VALUE, reusable.firstRecordKey());
    assertEquals(Long.MIN_VALUE, reusable.lastRecordKey());
    assertTrue(reusable.appendRow(30_000L, new long[] {7L, 0L}, new boolean[2], new String[] {"", "after-sample"}),
        "the returned page must already be ready for its next leaf");
  }

  @Test
  void exactSampleSeedingDeduplicatesRepeatedValuesAtTheLocalEntryCap() {
    final int distinctValues = ProjectionIndexRowGroupPage.MAX_ROWS;
    final List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(SAMPLE_LEAVES);
    long repeatedPerLeafDictionaryEntries = 0L;
    for (int leafIndex = 0; leafIndex < SAMPLE_LEAVES; leafIndex++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      for (int value = 0; value < distinctValues; value++) {
        assertTrue(page.appendRow(100_000L + (long) leafIndex * distinctValues + value, new long[] {value, 0L},
            new boolean[2], new String[] {"", "shared-" + value}));
      }
      repeatedPerLeafDictionaryEntries += page.stringDictionarySize(1);
      sample.add(page);
    }
    assertEquals(GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND, repeatedPerLeafDictionaryEntries,
        "the bounded sample must reach the interner's exact structural admission cap");

    final GlobalValueDictionaryWriter dictionary =
        new GlobalValueDictionaryWriter(1, Long.MAX_VALUE, GlobalValueDictionaryWriter.AdmissionPolicy.DECLINE);
    try {
      ProjectionIndexBuilder.seedGlobalDictionaryFromSample(sample, 1, dictionary);
      assertEquals(distinctValues, dictionary.entryCount(),
          "sample election must cap the exact resource-wide distinct set, not repeated local ids");
    } finally {
      dictionary.release();
    }
  }

  @Test
  void globallyUniqueSampleExhaustsHeadroomBeforeAnyPageIsConverted() {
    final List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(SAMPLE_LEAVES);
    for (int leafIndex = 0; leafIndex < SAMPLE_LEAVES; leafIndex++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      for (int row = 0; row < ProjectionIndexRowGroupPage.MAX_ROWS; row++) {
        final long ordinal = (long) leafIndex * ProjectionIndexRowGroupPage.MAX_ROWS + row;
        assertTrue(page.appendRow(200_000L + ordinal, new long[] {ordinal, 0L}, new boolean[2],
            new String[] {"", "unique-" + ordinal}));
      }
      sample.add(page);
    }

    final GlobalValueDictionaryWriter dictionary =
        new GlobalValueDictionaryWriter(1, Long.MAX_VALUE, GlobalValueDictionaryWriter.AdmissionPolicy.DECLINE);
    try {
      ProjectionIndexBuilder.seedGlobalDictionaryFromSample(sample, 1, dictionary);
      assertEquals(GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND, dictionary.entryCount(),
          "AUTO must see that exact seeding left no structural slot for a later novel value");
      for (final ProjectionIndexRowGroupPage page : sample) {
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, page.columnKind(1));
        assertTrue(page.stringDictionaryIsSlabBacked(1),
            "election refusal must precede every local-to-global page mutation");
      }
    } finally {
      dictionary.release();
    }
  }

  @Test
  void autoHeadroomBoundaryReservesExactlyOneFullyDistinctLeaf() {
    final int admissionCeiling =
        GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND - ProjectionIndexRowGroupPage.MAX_ROWS;
    assertTrue(ProjectionIndexBuilder.globalDictionarySampleHasHeadroom(admissionCeiling));
    assertFalse(ProjectionIndexBuilder.globalDictionarySampleHasHeadroom(admissionCeiling + 1));
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexBuilder.globalDictionarySampleHasHeadroom(-1));
  }

  @Test
  void borrowedPageIsNeverResetWhenItsSinkThrows() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(REUSE_KINDS);
    populateSmallRawShape(page);
    final byte[] before = page.serialize();
    final RuntimeException sinkFailure = new RuntimeException("sink failed");

    final RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> ProjectionIndexBuilder.emitBorrowedLeafForReuse(page,
            new GlobalValueDictionaryWriter[REUSE_KINDS.length], borrowed -> {
              assertSame(page, borrowed);
              assertArrayEquals(before, borrowed.serialize());
              throw sinkFailure;
            }));

    assertSame(sinkFailure, thrown);
    assertArrayEquals(before, page.serialize(),
        "a throwing borrower must retain the full live page for failure handling");
    assertEquals(3, page.getRowCount());
  }

  @Test
  void oneBuilderOwnedPageRetainsCapacityAcrossFsstRawFsstAndStringSetShapes() {
    final GlobalValueDictionaryWriter[] dictionaries = new GlobalValueDictionaryWriter[REUSE_KINDS.length];
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(REUSE_KINDS);
    final ProjectionIndexRowGroupPage expectedLarge = new ProjectionIndexRowGroupPage(REUSE_KINDS);
    final ProjectionIndexRowGroupPage expectedSmall = new ProjectionIndexRowGroupPage(REUSE_KINDS);
    populateLargeFsstShape(page);
    populateLargeFsstShape(expectedLarge);
    populateSmallRawShape(expectedSmall);

    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup[] emitted =
        new ProjectionIndexColumnSegmentCodec.EncodedRowGroup[3];
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionIndexRowGroupPage afterFirst =
        ProjectionIndexBuilder.emitBorrowedLeafForReuse(page, dictionaries, borrowed -> {
          assertSame(page, borrowed);
          assertArrayEquals(expectedLarge.serialize(), borrowed.serialize());
          emitted[0] = ProjectionIndexColumnSegmentCodec.encode(borrowed, workspace);
        });
    assertSame(page, afterFirst);
    assertEquals(0, page.getRowCount());

    populateSmallRawShape(afterFirst);
    assertFalse(afterFirst.columnUnrepresentable(2), "sticky flags from the first generation must be cleared");
    assertFalse(afterFirst.columnNumericNonIntegral(0), "numeric provenance must be generation-local");
    final ProjectionIndexRowGroupPage afterSecond =
        ProjectionIndexBuilder.emitBorrowedLeafForReuse(afterFirst, dictionaries, borrowed -> {
          assertSame(page, borrowed);
          assertArrayEquals(expectedSmall.serialize(), borrowed.serialize(),
              "short RAW leaf must not expose dirty FSST-generation tails");
          emitted[1] = ProjectionIndexColumnSegmentCodec.encode(borrowed, workspace);
        });
    assertSame(page, afterSecond);

    populateLargeFsstShape(afterSecond);
    final ProjectionIndexRowGroupPage afterThird =
        ProjectionIndexBuilder.emitBorrowedLeafForReuse(afterSecond, dictionaries, borrowed -> {
          assertSame(page, borrowed);
          assertArrayEquals(expectedLarge.serialize(), borrowed.serialize(),
              "A/B/A reuse must restore the original wire representation exactly");
          emitted[2] = ProjectionIndexColumnSegmentCodec.encode(borrowed, workspace);
        });
    assertSame(page, afterThird);
    assertEquals(0, afterThird.getRowCount());

    assertEncodedEquals(emitted[0], emitted[2], 2);
    assertEquals(1, segmentOf(emitted[0], ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        2))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "large dictionary must use FSST");
    assertEquals(0, segmentOf(emitted[1], ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(
        2))[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES], "short dictionary must use RAW");
    assertArrayEquals(expectedSmall.serialize(),
        ProjectionIndexColumnSegmentCodec.assembleRaw(emitted[1].descriptor(), resolverOf(emitted[1])),
        "STRING_SET counts/elements and RAW scalar strings must cold-assemble byte-identically");
  }

  private static void populateLargeFsstShape(final ProjectionIndexRowGroupPage page) {
    final long[] longs = new long[REUSE_KINDS.length];
    final boolean[] bools = new boolean[REUSE_KINDS.length];
    final String[] strings = new String[REUSE_KINDS.length];
    final String[][] sets = new String[REUSE_KINDS.length][];
    final boolean[] present = {true, true, true, true};
    final boolean[] unrepresentable = new boolean[REUSE_KINDS.length];
    final boolean[] nonIntegral = new boolean[REUSE_KINDS.length];
    final boolean[] nonDoubleSource = new boolean[REUSE_KINDS.length];
    for (int row = 0; row < 300; row++) {
      longs[0] = 100_000L + row;
      bools[1] = (row & 1) == 0;
      strings[2] = "https://sirix.example/reuse/tenant-" + (row % 23) + "/entity-" + row + "/shared-tail";
      sets[3] = row % 5 == 0
          ? new String[] {"Drama", "Drama", "Short"}
          : new String[] {"Comedy", "Feature"};
      unrepresentable[2] = row == 17;
      nonIntegral[0] = row == 19;
      assertTrue(page.appendRow(1_000_000L + row, longs, bools, strings, sets, present, unrepresentable, nonIntegral,
          nonDoubleSource));
      unrepresentable[2] = false;
      nonIntegral[0] = false;
    }
  }

  private static void populateSmallRawShape(final ProjectionIndexRowGroupPage page) {
    final String[] scalar = {"one", "two", "three"};
    final String[][] setValues = {{"solo"}, {}, {"duo", "solo"}};
    for (int row = 0; row < scalar.length; row++) {
      final long[] longs = {-100L + row, 0L, 0L, 0L};
      final boolean[] bools = new boolean[REUSE_KINDS.length];
      final String[] strings = {"", "", scalar[row], ""};
      final String[][] sets = new String[REUSE_KINDS.length][];
      sets[3] = setValues[row];
      assertTrue(page.appendRow(9_000_000L + row, longs, bools, strings, sets, new boolean[] {true, true, true, true},
          new boolean[REUSE_KINDS.length], new boolean[REUSE_KINDS.length], new boolean[REUSE_KINDS.length]));
    }
  }

  private static byte[] segmentOf(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded,
      final int segmentId) {
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == segmentId) {
        return encoded.segments()[i];
      }
    }
    throw new AssertionError("missing segment " + segmentId);
  }

  private static ProjectionIndexColumnSegmentCodec.SegmentResolver resolverOf(
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    return segmentId -> segmentOf(encoded, segmentId);
  }

  private static void assertEncodedEquals(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expected,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup actual, final int sampleIndex) {
    assertArrayEquals(expected.descriptor(), actual.descriptor(), "descriptor differs at sample page " + sampleIndex);
    assertArrayEquals(expected.columnSegmentIds(), actual.columnSegmentIds(),
        "segment ids differ at sample page " + sampleIndex);
    assertEquals(expected.segments().length, actual.segments().length);
    for (int i = 0; i < expected.segments().length; i++) {
      assertArrayEquals(expected.segments()[i], actual.segments()[i],
          "segment " + i + " differs at sample page " + sampleIndex);
    }
  }
}
