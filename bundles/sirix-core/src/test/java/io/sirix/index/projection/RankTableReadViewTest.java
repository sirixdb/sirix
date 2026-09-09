/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.AbstractForwardingStorageEngineWriter;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.ValueDictionaryRankTableNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sealed segment dictionary stores its values in collation order but the pages carry
 * arrival-order MINTS, mapped to storage positions by a rank table. This drives the read side
 * through a real table over a real dictionary — one that spans several 256-value blocks, several
 * 16384-mint table records are not needed for that, and a spilled oversized value — and checks
 * every route that must translate: decode ({@code valueAsString}, the static
 * {@code valueBytes}/{@code value}), the encode probe (a POSITION found by binary search must come
 * back as the MINT), id comparison, and the two refusals (verdict sweeps, and a second block index
 * or an append on top of the table).
 *
 * <p>
 * <b>Mutations this must fail:</b> {@code ReadView#positionOf} returning the id untranslated (every
 * decode reads the wrong value); {@code probe} returning the search's position instead of
 * {@code mintAtPosition} (the probe answers a position that is a different mint); the static
 * {@code valueBytes} skipping the table (wrong bytes); {@code compareIds} comparing mints as ints
 * (the order inverts for the reversed permutation); {@code attachRankTable} accepting a repeated
 * rank.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class RankTableReadViewTest {

  private static final String RESOURCE_NAME = "rankTableResource";

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Several 256-value blocks and reverse buckets, and more than one separator range. */
  private static final int FILLER_VALUES = 1_100;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("every read route translates mints through the rank table; every write route refuses to lose it")
  void readRoutesTranslateThroughTheTable() {
    final List<byte[]> sorted = buildSortedValueSet();
    final int prefix = sorted.size();
    // rankByMint[m] = the storage position (1-based index into `sorted`) of mint m. A random
    // permutation with the two extremes pinned: the FIRST mint holds the LAST value and vice versa,
    // so an untranslated route is wrong at both ends, not just somewhere in the middle.
    final int[] rankByMint = shuffledPermutation(prefix, 0xC0FFEEL);
    swapToPin(rankByMint, 1, prefix);
    swapToPin(rankByMint, prefix, 1);
    final int[] mintByRank = new int[prefix + 1];
    for (int mint = 1; mint <= prefix; mint++) {
      mintByRank[rankByMint[mint]] = mint;
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      final long tableKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        headerKey = flushRankOrdered(sorted, namePage, writer);
        GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, writer, writer.getLog());

        // The permutation is checked ONCE, at attach: a repeated rank or a hole never reaches a page,
        // and a refused attach reserves no key (the counter moves by exactly the two probes below).
        final long keyBefore = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1);
        final int[] repeated = rankByMint.clone();
        repeated[2] = repeated[3];
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(headerKey, repeated,
            namePage, DatabaseType.JSON, writer, writer.getLog()), "a rank assigned to two mints");
        final int[] outOfRange = rankByMint.clone();
        outOfRange[5] = prefix + 1;
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(headerKey, outOfRange,
            namePage, DatabaseType.JSON, writer, writer.getLog()), "a rank beyond the prefix");
        final int[] zero = rankByMint.clone();
        zero[7] = 0;
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(headerKey, zero,
            namePage, DatabaseType.JSON, writer, writer.getLog()), "rank 0 is not a position");
        assertThrows(
            IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(headerKey,
                Arrays.copyOf(rankByMint, prefix), namePage, DatabaseType.JSON, writer, writer.getLog()),
            "one mint short");
        final ValueDictionaryHeaderNode untouched = GlobalValueDictionary.header(headerKey, writer);
        assertNotNull(untouched);
        assertFalse(untouched.hasRankTable(), "a refused attach leaves the header alone");
        assertEquals(keyBefore + 1, namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1),
            "four refused attaches reserved no keys");

        tableKey = GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, writer,
            writer.getLog());
        assertTrue(tableKey > 0L);
        assertEquals(1, ValueDictionaryRankTableNode.recordCountFor(prefix),
            "the fixture fits one record per run; a wider table is the seal test's job");
        assertEquals(tableKey + 2, namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1),
            "a table over one record reserves exactly two keys: the forward run and the inverse run");

        // Once tabled, the routes that would silently drop the table refuse instead.
        final IllegalStateException secondIndex =
            assertThrows(IllegalStateException.class, () -> GlobalValueDictionary.buildBlockIndex(headerKey, namePage,
                DatabaseType.JSON, writer, writer.getLog()));
        assertTrue(secondIndex.getMessage().contains("rank table"), secondIndex.getMessage());
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(headerKey, rankByMint,
            namePage, DatabaseType.JSON, writer, writer.getLog()), "a second table on the same dictionary");
        final GlobalValueDictionaryWriter appender = new GlobalValueDictionaryWriter();
        final byte[] extra = utf8("zz-appended-after-seal");
        appender.intern(extra, 0, extra.length);
        final ValueDictionaryHeaderNode tabled = GlobalValueDictionary.header(headerKey, writer);
        assertNotNull(tabled);
        assertTrue(tabled.hasRankTable());
        final IllegalArgumentException append = assertThrows(IllegalArgumentException.class,
            () -> appender.flushAppend(tabled, namePage, DatabaseType.JSON, writer, writer.getLog()));
        assertTrue(append.getMessage().contains("rank table"), append.getMessage());
        appender.release();
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, reader);
        assertNotNull(header);
        assertEquals(tableKey, header.getRankTableKey());
        assertTrue(header.hasRankTable());
        assertTrue(header.isFullyOrdered(), "the STORAGE is in collation order");
        assertFalse(header.idsAreCollationOrdered(), "the IDS are mints");
        assertEquals(prefix, header.getEntryCount());
        assertEquals(prefix, header.getOrderedPrefixCount());
        assertNotEquals(0L, header.getBlockIndexKey(), "the separator array built before the table must survive it");
        assertEquals(0L, header.getForwardRootKey());

        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertFalse(view.fullyOrdered(), "no arm may compare mints as values");
        assertTrue(view.hasRankTable());
        assertEquals(prefix, view.entryCount());

        // TRANSLATION: the forward run answers positions, the inverse run answers mints, and the two
        // compose to the identity in both orders. Outside 1..entryCount neither direction guesses.
        for (int mint = 1; mint <= prefix; mint++) {
          assertEquals(rankByMint[mint], view.positionOf(mint));
          assertEquals(mintByRank[mint], view.mintAtPosition(mint));
          assertEquals(mint, view.mintAtPosition(view.positionOf(mint)));
          assertEquals(mint, view.positionOf(view.mintAtPosition(mint)));
        }
        for (final int outside : new int[] {0, -1, prefix + 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
          assertThrows(IllegalStateException.class, () -> view.positionOf(outside), "positionOf(" + outside + ")");
          assertThrows(IllegalStateException.class, () -> view.mintAtPosition(outside),
              "mintAtPosition(" + outside + ")");
        }

        // DECODE: every mint yields the value stored at its position, by both the view and the static
        // routes. Ascending mints hit storage in permuted order, which is the table's whole point.
        for (int mint = 1; mint <= prefix; mint++) {
          final byte[] expected = sorted.get(rankByMint[mint] - 1);
          final int m = mint;
          assertEquals(new String(expected, StandardCharsets.UTF_8), view.valueAsString(mint),
              () -> "view decode of mint " + m);
          assertArrayEquals(expected, GlobalValueDictionary.valueBytes(headerKey, mint, reader),
              () -> "static decode of mint " + m);
        }
        assertNull(GlobalValueDictionary.valueBytes(headerKey, prefix + 1, reader));
        assertNull(GlobalValueDictionary.valueBytes(headerKey, 0, reader));
        assertEquals(new String(sorted.get(prefix - 1), StandardCharsets.UTF_8), view.valueAsString(1),
            "the pinned extreme: the first mint holds the LAST value");
        assertEquals(new String(sorted.get(0), StandardCharsets.UTF_8), view.valueAsString(prefix),
            "and the last mint the FIRST value");
        // The bulk route walks storage order but answers in the caller's order; a dead id (0, negative,
        // past the count) is a null in its slot and a repeated id is answered twice.
        final int[] wanted = {prefix, 1, 2, prefix + 1, 0, -7, 2, Integer.MAX_VALUE, prefix / 2};
        final String[] bulk = GlobalValueDictionary.values(headerKey, wanted, reader);
        assertNotNull(bulk);
        assertEquals(wanted.length, bulk.length);
        assertEquals(view.valueAsString(prefix), bulk[0]);
        assertEquals(view.valueAsString(1), bulk[1]);
        assertEquals(view.valueAsString(2), bulk[2]);
        assertNull(bulk[3]);
        assertNull(bulk[4]);
        assertNull(bulk[5]);
        assertEquals(view.valueAsString(2), bulk[6]);
        assertNull(bulk[7]);
        assertEquals(view.valueAsString(prefix / 2), bulk[8]);
        assertArrayEquals(new int[] {prefix, 1, 2, prefix + 1, 0, -7, 2, Integer.MAX_VALUE, prefix / 2}, wanted,
            "the ids handed in are not reordered");
        assertThrows(NullPointerException.class, () -> GlobalValueDictionary.values(headerKey, null, reader));
        assertEquals(0, GlobalValueDictionary.values(headerKey, new int[0], reader).length);

        // SWEEP: the sequential verdict route walks STORAGE, one block at a time, and must land each
        // bit on the MINT that position belongs to. An untranslated sweep sets the bit for the value
        // at position p at id p — which this permutation makes wrong at both pinned extremes and at
        // every mint whose rank differs from it.
        int permuted = 0;
        for (int mint = 1; mint <= prefix; mint++) {
          if (rankByMint[mint] != mint) {
            permuted++;
          }
        }
        assertTrue(permuted > prefix / 2,
            "the sweep trap is not armed: " + permuted + " of " + prefix + " mints differ from their rank");

        final int oversizedPosition = positionOfOversized(sorted);
        final int[] sweptMints = {1, 2, prefix / 2, prefix - 1, prefix, mintByRank[oversizedPosition]};
        for (final int mint : sweptMints) {
          final byte[] value = sorted.get(rankByMint[mint] - 1);
          final long[] swept = view.stringOpVerdictByMint(ProjectionIndexScan.Op.EQ, value);
          // The fixture is strictly ascending, so a value equals exactly one entry: one bit, at its
          // own mint. This needs no reference implementation of the op to be wrong-sensitive.
          assertEquals(1, cardinality(swept),
              () -> "EQ against the value of mint " + mint + " must match exactly one entry");
          assertTrue(isSet(swept, mint),
              () -> "EQ set bit " + onlySetBit(swept) + ", but that value is held by mint " + mint);
        }
        assertEquals(ValueDictionaryEntryNode.MAX_VALUE_LENGTH, sorted.get(oversizedPosition - 1).length,
            "the spilled value must still be oversized, or the sweep's spill arm is untested");

        // And a multi-match op, so the walk is exercised across blocks rather than at one entry.
        final String contained = "value-00000";
        final long[] containing = view.stringOpVerdictByMint(ProjectionIndexScan.Op.STR_CONTAINS, utf8(contained));
        int expectedContaining = 0;
        for (int mint = 1; mint <= prefix; mint++) {
          final boolean expected =
              new String(sorted.get(rankByMint[mint] - 1), StandardCharsets.UTF_8).contains(contained);
          if (expected) {
            expectedContaining++;
          }
          final int m = mint;
          assertEquals(expected, isSet(containing, mint),
              () -> "STR_CONTAINS(" + contained + ") disagrees at mint " + m + " (position " + rankByMint[m] + ")");
        }
        assertEquals(10, expectedContaining, "the fixture must contain value-000000..value-000009");

        // ENCODE: the binary search finds a POSITION; the answer must be the MINT stored there — by
        // the static route, by the view, and by the view over a value embedded in a larger buffer.
        for (int position = 1; position <= prefix; position++) {
          final byte[] value = sorted.get(position - 1);
          final int p = position;
          assertEquals(mintByRank[position], GlobalValueDictionary.probe(headerKey, value, reader),
              () -> "probe of the value at position " + p + " must answer its mint");
          assertEquals(mintByRank[position], view.probe(value, 0, value.length),
              () -> "view probe of the value at position " + p);
          final byte[] embedded = new byte[value.length + 7];
          Arrays.fill(embedded, (byte) 'X');
          System.arraycopy(value, 0, embedded, 3, value.length);
          assertEquals(mintByRank[position], view.probe(embedded, 3, value.length),
              () -> "view probe of the value at position " + p + " from offset 3");
        }
        for (final byte[] value : buildAbsentValues(sorted)) {
          assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(headerKey, value, reader),
              () -> "absent under a table must still be ABSENT for " + describe(value));
          assertEquals(GlobalValueDictionary.ID_ABSENT, view.probe(value, 0, value.length),
              () -> "absent under a table must still be ABSENT for the view for " + describe(value));
        }

        // ORDER: comparing two mints compares their values, whatever their arrival order.
        final SplittableRandom random = new SplittableRandom(42L);
        for (int i = 0; i < 4_000; i++) {
          final int left = 1 + random.nextInt(prefix);
          final int right = 1 + random.nextInt(prefix);
          final int expected =
              Integer.signum(compareCollation(sorted.get(rankByMint[left] - 1), sorted.get(rankByMint[right] - 1)));
          assertEquals(expected, Integer.signum(view.compareIds(left, right)),
              () -> "compareIds(" + left + ", " + right + ")");
        }
        assertTrue(view.compareIds(1, prefix) > 0, "mint 1 holds the last value, so it orders AFTER mint P");
        assertEquals(0, view.compareIds(17, 17));
        // The spilled value (MAX_VALUE_LENGTH bytes, behind its own record rather than in a block)
        // compares through the table like any other: it sorts after every "value-" and before nothing
        // that starts later in the alphabet, whatever mint it was given.
        final int spilledPosition = 1 + indexOfSpilled(sorted);
        final int spilledMint = mintByRank[spilledPosition];
        assertEquals(sorted.get(spilledPosition - 1).length, ValueDictionaryEntryNode.MAX_VALUE_LENGTH);
        assertEquals(new String(sorted.get(spilledPosition - 1), StandardCharsets.UTF_8),
            view.valueAsString(spilledMint), "the spilled value decodes through its mint");
        for (int mint = 1; mint <= prefix; mint++) {
          if (mint == spilledMint) {
            continue;
          }
          final int expected =
              Integer.signum(compareCollation(sorted.get(spilledPosition - 1), sorted.get(rankByMint[mint] - 1)));
          assertEquals(expected, Integer.signum(view.compareIds(spilledMint, mint)),
              "compareIds(spilled, " + mint + ")");
          assertEquals(-expected, Integer.signum(view.compareIds(mint, spilledMint)),
              "compareIds(" + mint + ", spilled)");
        }

        // The verdict sweep sets bits by storage position while rows carry mints: refused, not wrong.
        final byte[] literal = sorted.get(10);
        assertThrows(UnsupportedOperationException.class,
            () -> view.stringOpVerdict(ProjectionIndexScan.Op.EQ, literal));
        assertThrows(UnsupportedOperationException.class, () -> view.fillStringOpVerdict(ProjectionIndexScan.Op.STR_LT,
            literal, 0, view.verdictBucketCount(), view.newVerdict(), 0));

        // The two runs are reachable at the header's key: the forward record at tableKey covers the
        // prefix in mints, the inverse record right behind it covers the prefix in positions.
        final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
        final ValueDictionaryRankTableNode forward =
            (ValueDictionaryRankTableNode) namePage.getProjectionValueDictionaryRecord(tableKey, DatabaseType.JSON,
                reader);
        assertNotNull(forward);
        assertEquals(1, forward.firstKey());
        assertEquals(prefix, forward.size());
        assertEquals(ValueDictionaryRankTableNode.bitsFor(prefix), forward.bitsPerEntry());
        final ValueDictionaryRankTableNode inverse =
            (ValueDictionaryRankTableNode) namePage.getProjectionValueDictionaryRecord(tableKey + 1, DatabaseType.JSON,
                reader);
        assertNotNull(inverse);
        assertEquals(1, inverse.firstKey());
        assertEquals(prefix, inverse.size());
        assertEquals(ValueDictionaryRankTableNode.bitsFor(prefix), inverse.bitsPerEntry());
        for (int mint = 1; mint <= prefix; mint++) {
          assertEquals(rankByMint[mint], forward.entryOf(mint));
          assertEquals(mintByRank[mint], inverse.entryOf(mint));
        }
        assertNull(namePage.getProjectionValueDictionaryRecord(tableKey + 2, DatabaseType.JSON, reader),
            "nothing is written past the inverse run");
      }
    }
  }

  private static int indexOfSpilled(final List<byte[]> sorted) {
    for (int i = 0; i < sorted.size(); i++) {
      if (sorted.get(i).length == ValueDictionaryEntryNode.MAX_VALUE_LENGTH) {
        return i;
      }
    }
    throw new IllegalStateException("the fixture lost its spilled value");
  }

  /**
   * {@code valueLengthOfCell} is the per-cell twin of {@code fillLengthTable}: the segment-scoped
   * length table is derived per canonical id by asking for one cell at a time. Both modes, every
   * storage shape the fixture has (the empty value, block values, supplementary code points, the
   * spilled oversized value), both view shapes (a plain view addressing mints, a segment union
   * addressing packed cells) — and the refusals: an id outside the dictionary is {@code -1}, a cell
   * naming a segment that sealed nothing is refused, a non-length mode is refused.
   */
  @Test
  @DisplayName("valueLengthOfCell reads every length off the stored bytes, in both modes and view shapes")
  void valueLengthsReadOffTheStoredBytes() {
    // The shared fixture's spilled value is ASCII, where the two modes agree; a second spilled value
    // made of supplementary code points makes the spill arm answer differently per mode.
    final String emoji = new String(Character.toChars(0x1F600));
    final byte[] multibyteSpill = utf8("p" + emoji.repeat(ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES / 4 + 1));
    assertTrue(multibyteSpill.length > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES, "it must not fit a block");
    final List<byte[]> sorted = buildSortedValueSet();
    sorted.add(multibyteSpill);
    sorted.sort(RankTableReadViewTest::compareCollation);
    final int prefix = sorted.size();
    final int[] rankByMint = shuffledPermutation(prefix, 0x5EED5L);
    swapToPin(rankByMint, 1, prefix);
    swapToPin(rankByMint, prefix, 1);
    final int[] mintByRank = new int[prefix + 1];
    for (int mint = 1; mint <= prefix; mint++) {
      mintByRank[rankByMint[mint]] = mint;
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        headerKey = flushRankOrdered(sorted, namePage, writer);
        GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, writer, writer.getLog());
        GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, writer,
            writer.getLog());
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertFalse(view.isSegmentUnion());
        // The same dictionary as segment 2 of a three-segment column whose other segments sealed none.
        final int segment = 2;
        final GlobalValueDictionary.ReadView union =
            GlobalValueDictionary.segmentUnionReadView(new long[] {0L, 0L, headerKey}, reader);
        assertNotNull(union);
        assertTrue(union.isSegmentUnion());
        final int[] byteTable = view.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES);
        final int[] codePointTable = view.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS);

        int modesDiffer = 0;
        for (int mint = 1; mint <= prefix; mint++) {
          final byte[] stored = sorted.get(rankByMint[mint] - 1);
          final String decoded = new String(stored, StandardCharsets.UTF_8);
          final int codePoints = decoded.codePointCount(0, decoded.length());
          final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, mint);
          final String at = "mint " + mint + " at position " + rankByMint[mint];
          assertEquals(stored.length, view.valueLengthOfCell(mint, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES),
              () -> "bytes of " + at);
          assertEquals(codePoints, view.valueLengthOfCell(mint, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
              () -> "code points of " + at);
          assertEquals(stored.length, union.valueLengthOfCell(cell, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES),
              () -> "bytes of " + at + " through the union");
          assertEquals(codePoints, union.valueLengthOfCell(cell, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
              () -> "code points of " + at + " through the union");
          assertEquals(byteTable[mint], stored.length, () -> "the table's bytes of " + at);
          assertEquals(codePointTable[mint], codePoints, () -> "the table's code points of " + at);
          if (stored.length != codePoints) {
            modesDiffer++;
          }
        }
        assertEquals(5, modesDiffer, "the four non-ASCII collate- values and the multibyte spill");

        // The shapes the loop must have covered, named: the empty value has length 0 in both modes,
        // the spilled value answers from its own record, and a supplementary code point is four bytes.
        final int emptyMint = mintByRank[1];
        assertEquals(0, sorted.get(0).length, "the fixture's first value is the empty one");
        assertEquals(0, view.valueLengthOfCell(emptyMint, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES));
        assertEquals(0, view.valueLengthOfCell(emptyMint, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS));
        final int spilledMint = mintByRank[1 + indexOfSpilled(sorted)];
        assertEquals(ValueDictionaryEntryNode.MAX_VALUE_LENGTH,
            view.valueLengthOfCell(spilledMint, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES));
        assertEquals(ValueDictionaryEntryNode.MAX_VALUE_LENGTH,
            view.valueLengthOfCell(spilledMint, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
            "the spilled value is ASCII: as many code points as bytes");
        final int multibyteSpillMint = mintByRank[1 + indexOf(sorted, multibyteSpill)];
        assertEquals(multibyteSpill.length,
            view.valueLengthOfCell(multibyteSpillMint, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES));
        assertEquals(1 + ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES / 4 + 1,
            view.valueLengthOfCell(multibyteSpillMint, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
            "the multibyte spill answers code points from its own record");
        final int supplementaryMint =
            mintByRank[1 + indexOf(sorted, utf8("collate-" + new String(Character.toChars(0x1F600))))];
        assertEquals("collate-".length() + 4,
            view.valueLengthOfCell(supplementaryMint, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES));
        assertEquals("collate-".length() + 1,
            view.valueLengthOfCell(supplementaryMint, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS));

        // Outside 1..entryCount is "no entry", not a guess — in both view shapes.
        for (final int outside : new int[] {0, -1, prefix + 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
          assertEquals(-1, view.valueLengthOfCell(outside, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES),
              "bytes of id " + outside);
          assertEquals(-1, view.valueLengthOfCell(outside, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
              "code points of id " + outside);
          final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, outside);
          assertEquals(-1, union.valueLengthOfCell(cell, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES),
              "bytes of cell (2, " + outside + ")");
          assertEquals(-1, union.valueLengthOfCell(cell, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS),
              "code points of cell (2, " + outside + ")");
        }
        // A cell of a segment that sealed no dictionary is refused, never resolved against segment 2.
        for (final int other : new int[] {0, 1, 3, -1}) {
          final long cell = ProjectionIndexRowGroupPage.packSegmentCell(other, 1);
          assertThrows(IllegalStateException.class,
              () -> union.valueLengthOfCell(cell, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES),
              "a cell of segment " + other);
        }
        // Only the two length modes are lengths.
        assertThrows(IllegalArgumentException.class,
            () -> view.valueLengthOfCell(1, ProjectionIndexByteScan.STRING_LENGTH_NONE));
        assertThrows(IllegalArgumentException.class,
            () -> union.valueLengthOfCell(ProjectionIndexRowGroupPage.packSegmentCell(segment, 1), (byte) 7));
      }
    }
  }

  @Test
  @DisplayName("a position cursor walks the stored order block by block, answering the table's bytes and mints")
  void positionCursorWalksTheStoredOrder() {
    final List<byte[]> sorted = buildSortedValueSet();
    final int prefix = sorted.size();
    final int[] rankByMint = shuffledPermutation(prefix, 0xC0FFEEL);
    final int[] mintByRank = new int[prefix + 1];
    for (int mint = 1; mint <= prefix; mint++) {
      mintByRank[rankByMint[mint]] = mint;
    }
    final int spilledPosition = positionOfOversized(sorted);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      final long hashedKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        headerKey = flushRankOrdered(sorted, namePage, writer);
        GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, writer, writer.getLog());
        GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, writer,
            writer.getLog());
        // A streaming (hashed) dictionary stores in intern order: it has no position space to walk.
        final GlobalValueDictionaryWriter hashed = new GlobalValueDictionaryWriter();
        for (final byte[] value : sorted) {
          hashed.intern(value, 0, value.length);
        }
        hashedKey = hashed.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
        hashed.release();
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        final int segment = 2;
        final GlobalValueDictionary.ReadView union =
            GlobalValueDictionary.segmentUnionReadView(new long[] {0L, 0L, headerKey}, reader);
        assertNotNull(union);
        final GlobalValueDictionary.ReadView hashedView = GlobalValueDictionary.readView(hashedKey, reader);
        assertNotNull(hashedView);
        assertNull(hashedView.positionCursorOfCell(1), "a hashed dictionary has no stored order to walk");

        final SegmentRunCursor cursor = view.positionCursorOfCell(1);
        assertNotNull(cursor);
        final SegmentRunCursor viaUnion =
            union.positionCursorOfCell(ProjectionIndexRowGroupPage.packSegmentCell(segment, 7));
        assertNotNull(viaUnion);
        assertEquals(0L, cursor.loads(), "a fresh cursor holds nothing");

        // THE WALK: every position in order, on both view shapes, against the fixture's sorted bytes.
        int spills = 0;
        for (int position = 1; position <= prefix; position++) {
          final byte[] stored = sorted.get(position - 1);
          final String at = "position " + position;
          cursor.seek(position);
          viaUnion.seek(position);
          assertArrayEquals(stored, cursor.copyValue(), at);
          assertArrayEquals(stored, viaUnion.copyValue(), at + " through the union");
          assertEquals(0, SegmentRunCursor.compareToRange(cursor, stored, 0, stored.length), at);
          assertEquals(new String(stored, StandardCharsets.UTF_8), cursor.valueAsString(), at);
          assertEquals(0, SegmentRunCursor.compare(cursor, viaUnion), at + ": two cursors at one position are equal");
          if (position > 1) {
            final byte[] previous = sorted.get(position - 2);
            assertTrue(SegmentRunCursor.compareToRange(cursor, previous, 0, previous.length) > 0,
                at + " orders after the position before it");
          }
          if (position < prefix) {
            final byte[] next = sorted.get(position);
            assertTrue(SegmentRunCursor.compareToRange(cursor, next, 0, next.length) < 0,
                at + " orders before the position after it");
          }
          if (cursor.spill != null) {
            spills++;
            assertEquals(spilledPosition, position, "only the oversized value comes off its own record");
            assertNull(cursor.backing, "a spilled position holds no block slice");
          } else {
            assertNotNull(cursor.backing, at + " holds a block slice");
          }
          assertEquals(mintByRank[position], cursor.mintAt(position), "mint at " + at);
          assertEquals(mintByRank[position], viaUnion.mintAt(position), "mint at " + at + " through the union");
        }
        assertEquals(1, spills, "the fixture's one oversized value was met as a spill");

        // EACH READ ONCE: an ascending walk fetches every bucket, block and inverse record one time.
        // The block count is the dictionary's own (at most one per bucket plus one per spill split),
        // so the bound is what the walk may fetch; a per-seek fetch would be a thousand.
        final int buckets = (prefix + 255) >>> 8;
        final int records = ValueDictionaryRankTableNode.recordCountFor(prefix);
        final long walked = cursor.loads();
        assertTrue(walked >= buckets + records + 1, "buckets, records and at least one block: " + walked);
        assertTrue(walked <= 2L * buckets + 2L * spills + records + 1,
            "each read once: " + walked + " loads for " + prefix + " positions");
        assertEquals(walked, viaUnion.loads(), "the union's cursor is the segment's cursor");

        // Seeks inside the held block, and mints inside the held record, fetch nothing more.
        cursor.seek(1);
        final long held = cursor.loads();
        cursor.seek(2);
        cursor.seek(1);
        cursor.seek(2);
        cursor.mintAt(1);
        cursor.mintAt(2);
        assertEquals(held, cursor.loads(), "a seek inside the held block is a slice, not a fetch");
        assertArrayEquals(sorted.get(1), cursor.copyValue());

        // The same positions in RANDOM order is what the per-cell route amounts to: a fetch per seek.
        final SegmentRunCursor random = view.positionCursorOfCell(1);
        assertNotNull(random);
        final int[] order = shuffledPermutation(prefix, 0xDEADL);
        for (int i = 1; i <= prefix; i++) {
          random.seek(order[i]);
          assertArrayEquals(sorted.get(order[i] - 1), random.copyValue(), "position " + order[i] + " out of order");
        }
        assertTrue(random.loads() > 10 * walked, "random seeks re-fetch: " + random.loads() + " vs " + walked);

        // Outside 1..entries is refused, never a guess.
        for (final int outside : new int[] {0, -1, prefix + 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
          assertThrows(IllegalStateException.class, () -> cursor.seek(outside), "seek " + outside);
          assertEquals(-1, cursor.mintAt(outside), "mint at " + outside);
        }
        // A cell of a segment that sealed no dictionary has no cursor, and is not resolved elsewhere.
        for (final int other : new int[] {0, 1, 3, -1}) {
          assertThrows(IllegalStateException.class,
              () -> union.positionCursorOfCell(ProjectionIndexRowGroupPage.packSegmentCell(other, 1)),
              "a cell of segment " + other);
        }
      }
    }
  }

  /**
   * {@code fillLengthTableByPosition} derives the per-mint length table by ONE walk of the stored
   * order — what a length lane over a segment column indexes by the row's own mint, with no canonical
   * id space. It must equal the id walk's table entry for entry, in both modes, over a rank-tabled
   * dictionary (the pinned extremes put a non-empty value's length at mint 1 and the empty value's 0
   * at the last mint, so a fill that lands lengths on POSITIONS is wrong at both ends), over two
   * spilled values (the ASCII one and a multibyte one, whose length per mode comes off its own
   * record) and over an intern-ordered dictionary (no position space: the id walk by another name).
   * The cursor's {@code valueLength} reads the length off the slice the seek left. Refusals: a union
   * view, a short table, a foreign mode, a null table.
   */
  @Test
  @DisplayName("a position-order fill lands every length on its mint, in both modes, over every storage shape")
  void positionFillLandsLengthsOnMints() {
    final String emoji = new String(Character.toChars(0x1F600));
    final byte[] multibyteSpill = utf8("p" + emoji.repeat(ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES / 4 + 1));
    final List<byte[]> sorted = buildSortedValueSet();
    sorted.add(multibyteSpill);
    sorted.sort(RankTableReadViewTest::compareCollation);
    final int prefix = sorted.size();
    final int[] rankByMint = shuffledPermutation(prefix, 0xBEEF5L);
    swapToPin(rankByMint, 1, prefix);
    swapToPin(rankByMint, prefix, 1);
    final byte[] modes =
        {ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS};

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      final long hashedKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        headerKey = flushRankOrdered(sorted, namePage, writer);
        GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, writer, writer.getLog());
        GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, writer,
            writer.getLog());
        final GlobalValueDictionaryWriter hashed = new GlobalValueDictionaryWriter();
        for (final byte[] value : sorted) {
          hashed.intern(value, 0, value.length);
        }
        hashedKey = hashed.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
        hashed.release();
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertTrue(view.hasRankTable());
        final GlobalValueDictionary.ReadView union =
            GlobalValueDictionary.segmentUnionReadView(new long[] {0L, 0L, headerKey}, reader);
        assertNotNull(union);
        final GlobalValueDictionary.ReadView hashedView = GlobalValueDictionary.readView(hashedKey, reader);
        assertNotNull(hashedView);
        assertFalse(hashedView.hasRankTable());
        assertNull(hashedView.positionCursorOfCell(1), "intern-ordered storage has no position space to walk");

        for (final byte mode : modes) {
          final int[] byPosition = new int[prefix + 1];
          final int[] expectedByMint = new int[prefix + 1];
          for (int position = 1; position <= prefix; position++) {
            byPosition[position] = lengthOf(sorted.get(position - 1), mode);
          }
          for (int mint = 1; mint <= prefix; mint++) {
            expectedByMint[mint] = byPosition[rankByMint[mint]];
          }
          // The trap is armed: at both pinned extremes a positional fill answers the wrong length.
          assertEquals(0, byPosition[1], "the fixture's first value is the empty one");
          assertEquals(0, expectedByMint[prefix], "the last mint holds the empty value");
          assertNotEquals(0, expectedByMint[1], "the first mint holds the last value, which is not empty");
          assertNotEquals(byPosition[prefix], expectedByMint[prefix], "mode " + mode + ": the last slot differs");

          final int[] filled = new int[prefix + 1];
          view.fillLengthTableByPosition(mode, filled);
          assertArrayEquals(expectedByMint, filled, "mode " + mode + ": every length on its mint");
          assertArrayEquals(view.lengthTable(mode), filled,
              "mode " + mode + ": the id walk and the position walk derive one table");
          // A table with room to spare is filled in 1..entryCount and untouched past it.
          final int[] roomy = new int[prefix + 3];
          roomy[prefix + 1] = -7;
          roomy[prefix + 2] = -9;
          view.fillLengthTableByPosition(mode, roomy);
          assertArrayEquals(expectedByMint, Arrays.copyOf(roomy, prefix + 1), "mode " + mode + " into a roomy table");
          assertEquals(-7, roomy[prefix + 1]);
          assertEquals(-9, roomy[prefix + 2]);

          // The cursor's own read is the fill's per-position ingredient: the slice's length, or the
          // spilled record's, in the mode asked for.
          final SegmentRunCursor cursor = view.positionCursorOfCell(1);
          assertNotNull(cursor);
          int spills = 0;
          for (int position = 1; position <= prefix; position++) {
            cursor.seek(position);
            assertEquals(byPosition[position], cursor.valueLength(mode), "mode " + mode + " at position " + position);
            if (cursor.spill != null) {
              spills++;
            }
          }
          assertEquals(2, spills, "the ASCII oversized value and the multibyte spill both come off their records");

          // Intern-ordered storage: ids are positions, and the position fill is the id walk.
          final int[] hashedFilled = new int[prefix + 1];
          hashedView.fillLengthTableByPosition(mode, hashedFilled);
          assertArrayEquals(hashedView.lengthTable(mode), hashedFilled, "mode " + mode + " over intern order");
          assertArrayEquals(byPosition, hashedFilled, "mode " + mode + ": interned in sorted order, id = position");
        }
        // The two modes disagree on the multibyte values, so both lanes were exercised above.
        final int[] bytes = new int[prefix + 1];
        final int[] codePoints = new int[prefix + 1];
        view.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, bytes);
        view.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS, codePoints);
        int modesDiffer = 0;
        for (int mint = 1; mint <= prefix; mint++) {
          if (bytes[mint] != codePoints[mint]) {
            modesDiffer++;
          }
        }
        assertEquals(5, modesDiffer, "the four non-ASCII collate- values and the multibyte spill");

        // Refusals.
        assertThrows(IllegalStateException.class,
            () -> union.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES,
                new int[prefix + 1]),
            "a union has no dense id space");
        assertThrows(IllegalArgumentException.class,
            () -> view.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, new int[prefix]),
            "one slot short");
        assertThrows(IllegalArgumentException.class,
            () -> view.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_NONE, new int[prefix + 1]),
            "not a length mode");
        assertThrows(IllegalArgumentException.class,
            () -> view.fillLengthTableByPosition((byte) 7, new int[prefix + 1]), "not a length mode");
        assertThrows(NullPointerException.class,
            () -> view.fillLengthTableByPosition(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null));
        final SegmentRunCursor cursor = view.positionCursorOfCell(1);
        assertNotNull(cursor);
        cursor.seek(1);
        assertThrows(IllegalArgumentException.class,
            () -> cursor.valueLength(ProjectionIndexByteScan.STRING_LENGTH_NONE));
        assertThrows(IllegalArgumentException.class, () -> cursor.valueLength((byte) 7));
      }
    }
  }

  private static int lengthOf(final byte[] value, final byte mode) {
    if (mode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES) {
      return value.length;
    }
    final String text = new String(value, StandardCharsets.UTF_8);
    return text.codePointCount(0, text.length());
  }

  private static int indexOf(final List<byte[]> sorted, final byte[] value) {
    for (int i = 0; i < sorted.size(); i++) {
      if (Arrays.equals(sorted.get(i), value)) {
        return i;
      }
    }
    throw new IllegalStateException("the fixture lost " + new String(value, StandardCharsets.UTF_8));
  }

  /**
   * A permutation the seal would never produce — identity — is still a legal table; and a table over
   * a dictionary that is NOT fully ordered, or that has a forward index, is refused before anything
   * is written.
   */
  @Test
  @DisplayName("attachRankTable refuses a hashed or empty dictionary and accepts an identity permutation")
  void attachPreconditions() {
    final List<byte[]> sorted = buildSortedValueSet().subList(0, 300);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        final int[] identity = new int[sorted.size() + 1];
        for (int i = 1; i <= sorted.size(); i++) {
          identity[i] = i;
        }

        // A streaming (hashed, intern-ordered) dictionary has a forward index and no ordered prefix.
        final GlobalValueDictionaryWriter hashed = new GlobalValueDictionaryWriter();
        for (final byte[] value : sorted) {
          hashed.intern(value, 0, value.length);
        }
        final long hashedKey = hashed.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
        hashed.release();
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(hashedKey, identity,
            namePage, DatabaseType.JSON, writer, writer.getLog()));
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(hashedKey + 100_000L,
            identity, namePage, DatabaseType.JSON, writer, writer.getLog()), "no header at that key");

        // An ordered base extended by an intern-ordered generation keeps its ordered PREFIX and gains a
        // forward index over the tail. A table on top would drop that index (the header admits no
        // forward root beside a table), so the attach must refuse it — before reserving a single key.
        final long tailedKey = flushRankOrdered(sorted, namePage, writer);
        final GlobalValueDictionaryWriter tail = new GlobalValueDictionaryWriter();
        final byte[] appended = utf8("zz-appended-tail-value");
        tail.intern(appended, 0, appended.length);
        final ValueDictionaryHeaderNode tailedBase = GlobalValueDictionary.header(tailedKey, writer);
        assertNotNull(tailedBase);
        assertEquals(tailedKey, tail.flushAppend(tailedBase, namePage, DatabaseType.JSON, writer, writer.getLog()));
        tail.release();
        final ValueDictionaryHeaderNode tailed = GlobalValueDictionary.header(tailedKey, writer);
        assertNotNull(tailed);
        assertEquals(sorted.size(), tailed.getOrderedPrefixCount());
        assertEquals(sorted.size() + 1, tailed.getEntryCount());
        assertNotEquals(0L, tailed.getForwardRootKey(), "the appended generation is probed through a forward index");
        final long keyBefore = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1);
        assertThrows(IllegalArgumentException.class, () -> GlobalValueDictionary.attachRankTable(tailedKey, identity,
            namePage, DatabaseType.JSON, writer, writer.getLog()));
        assertEquals(keyBefore + 1, namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1),
            "a refused attach reserves no keys");
        final ValueDictionaryHeaderNode stillTailed = GlobalValueDictionary.header(tailedKey, writer);
        assertNotNull(stillTailed);
        assertEquals(tailed, stillTailed, "a refused attach leaves the header untouched");

        final long orderedKey = flushRankOrdered(sorted, namePage, writer);
        final long tableKey = GlobalValueDictionary.attachRankTable(orderedKey, identity, namePage, DatabaseType.JSON,
            writer, writer.getLog());
        assertTrue(tableKey > 0L);
        wtx.commit();

        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = rtx.getStorageEngineReader();
          final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(orderedKey, reader);
          assertNotNull(view);
          assertFalse(view.fullyOrdered(), "an identity table is still a table: the header, not the data, decides");
          for (int mint = 1; mint <= sorted.size(); mint++) {
            assertEquals(new String(sorted.get(mint - 1), StandardCharsets.UTF_8), view.valueAsString(mint));
            assertEquals(mint, GlobalValueDictionary.probe(orderedKey, sorted.get(mint - 1), reader));
          }
          // No block index was built for this one (300 values), so the search took the whole-prefix
          // route; the hashed control still answers through its forward index.
          assertEquals(0L, GlobalValueDictionary.header(orderedKey, reader).getBlockIndexKey());
          assertEquals(3, GlobalValueDictionary.probe(hashedKey, sorted.get(2), reader));
          // The tailed dictionary kept both directions: its prefix by binary search, its tail by index.
          assertEquals(3, GlobalValueDictionary.probe(tailedKey, sorted.get(2), reader));
          assertEquals(sorted.size() + 1,
              GlobalValueDictionary.probe(tailedKey, utf8("zz-appended-tail-value"), reader));
        }
      }
    }
  }

  /**
   * A rank table record of the wrong shape at the table's key is corruption, and translating through
   * it would decode wrong values without a trace. Every field of the shape is corrupted ONE AT A TIME
   * — first key, count, width, kind — on the forward record, then the count of the inverse record
   * alone, so that each check is shown to fire on its own and the inverse run is shown to be
   * validated independently of the forward run. Each corruption is its own revision; the final
   * revision restores both records and everything reads again, which pins the corruption as the only
   * cause.
   */
  @Test
  @DisplayName("a rank table record of the wrong shape refuses, field by field, on both runs")
  void wrongShapedRecordsRefuse() {
    final List<byte[]> sorted = buildSortedValueSet();
    final int prefix = sorted.size();
    final int[] rankByMint = shuffledPermutation(prefix, 0xBEEFL);
    final int[] mintByRank = new int[prefix + 1];
    for (int mint = 1; mint <= prefix; mint++) {
      mintByRank[rankByMint[mint]] = mint;
    }
    final int bits = ValueDictionaryRankTableNode.bitsFor(prefix);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      final long tableKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        headerKey = flushRankOrdered(sorted, namePage, writer);
        tableKey = GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, writer,
            writer.getLog());
        wtx.commit();
      }
      final byte[] firstValue = sorted.get(rankByMint[1] - 1);

      // A record whose first key says it is the SECOND record of a run, at the first record's key.
      final int[] shifted = new int[1 + ValueDictionaryRankTableNode.ENTRIES_PER_RECORD + prefix];
      Arrays.fill(shifted, 1);
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey,
          1 + ValueDictionaryRankTableNode.ENTRIES_PER_RECORD, shifted, prefix, bits));
      expectRefusal(session, headerKey, firstValue,
          "covers keys " + (1 + ValueDictionaryRankTableNode.ENTRIES_PER_RECORD) + "+" + prefix, true, false);

      // One entry short of the prefix.
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey, 1, rankByMint, prefix - 1, bits));
      expectRefusal(session, headerKey, firstValue, "covers keys 1+" + (prefix - 1), true, false);

      // The right entries at the wrong width.
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey, 1, rankByMint, prefix, bits + 1));
      expectRefusal(session, headerKey, firstValue, "at " + (bits + 1) + " bits", true, false);

      // Another kind of record altogether at the table's key.
      overwrite(session, tableKey,
          new ValueDictionaryHeaderNode(tableKey, ValueDictionaryHeaderNode.VERSION, 3, 7L, 9L, 1));
      expectRefusal(session, headerKey, firstValue, "is a VALUE_DICTIONARY_HEADER", true, false);

      // The forward record restored, the INVERSE record one entry short: decoding works again, the
      // probe — the only reader of the inverse run — refuses.
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey, 1, rankByMint, prefix, bits));
      overwrite(session, tableKey + 1,
          ValueDictionaryRankTableNode.pack(tableKey + 1, 1, mintByRank, prefix - 1, bits));
      expectRefusal(session, headerKey, firstValue, "record 1 of value dictionary " + headerKey, false, true);

      // Records of the RIGHT shape whose entries point outside the prefix. prefix + 1 fits the record's
      // width, so nothing but the range check on the translated value can see it: a forward entry
      // sending mint 1 to position prefix + 1 refuses on every decode route, an inverse entry sending
      // mint 1's position to id prefix + 1 refuses on the probe.
      assertTrue(prefix + 1 <= (1 << bits) - 1, "the fixture must leave room above the prefix at " + bits + " bits");
      final int[] forwardEscaping = rankByMint.clone();
      forwardEscaping[1] = prefix + 1;
      overwrite(session, tableKey + 1, ValueDictionaryRankTableNode.pack(tableKey + 1, 1, mintByRank, prefix, bits));
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey, 1, forwardEscaping, prefix, bits));
      expectRefusal(session, headerKey, firstValue,
          "maps id 1 to position " + (prefix + 1) + ", outside its ordered prefix of " + prefix, true, false);

      final int[] inverseEscaping = mintByRank.clone();
      inverseEscaping[rankByMint[1]] = prefix + 1;
      overwrite(session, tableKey, ValueDictionaryRankTableNode.pack(tableKey, 1, rankByMint, prefix, bits));
      overwrite(session, tableKey + 1,
          ValueDictionaryRankTableNode.pack(tableKey + 1, 1, inverseEscaping, prefix, bits));
      expectRefusal(session, headerKey, firstValue,
          "maps position " + rankByMint[1] + " to id " + (prefix + 1) + ", outside its ordered prefix of " + prefix,
          false, true);

      // Both restored: every route answers again.
      overwrite(session, tableKey + 1, ValueDictionaryRankTableNode.pack(tableKey + 1, 1, mintByRank, prefix, bits));
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertArrayEquals(firstValue, GlobalValueDictionary.valueBytes(headerKey, 1, reader));
        assertEquals(1, GlobalValueDictionary.probe(headerKey, firstValue, reader));
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        for (int mint = 1; mint <= prefix; mint++) {
          assertEquals(rankByMint[mint], view.positionOf(mint));
          assertEquals(mintByRank[mint], view.mintAtPosition(mint));
        }
      }
    }
  }

  /** Writes {@code record} under its own key in a revision of its own. */
  private static void overwrite(final JsonResourceSession session, final long key, final DataRecord record) {
    assertEquals(key, record.getNodeKey());
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final StorageEngineWriter writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      namePage.putProjectionValueDictionaryRecord(record, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
    }
  }

  /**
   * In the latest revision, the decode of mint 1 and the probe of its value each either answer or
   * refuse with {@link IllegalStateException} naming {@code fragment}, as the flags say.
   */
  private static void expectRefusal(final JsonResourceSession session, final long headerKey, final byte[] firstValue,
      final String fragment, final boolean decodeRefuses, final boolean probeRefuses) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      if (decodeRefuses) {
        final IllegalStateException staticDecode = assertThrows(IllegalStateException.class,
            () -> GlobalValueDictionary.valueBytes(headerKey, 1, reader), "static decode");
        assertTrue(staticDecode.getMessage().contains(fragment), staticDecode.getMessage());
        final IllegalStateException bulkDecode = assertThrows(IllegalStateException.class,
            () -> GlobalValueDictionary.values(headerKey, new int[] {1}, reader), "bulk decode");
        assertTrue(bulkDecode.getMessage().contains(fragment), bulkDecode.getMessage());
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        final IllegalStateException viewDecode =
            assertThrows(IllegalStateException.class, () -> view.valueAsString(1), "view decode");
        assertTrue(viewDecode.getMessage().contains(fragment), viewDecode.getMessage());
      } else {
        assertArrayEquals(firstValue, GlobalValueDictionary.valueBytes(headerKey, 1, reader));
      }
      if (probeRefuses) {
        final IllegalStateException probe = assertThrows(IllegalStateException.class,
            () -> GlobalValueDictionary.probe(headerKey, firstValue, reader), "probe");
        assertTrue(probe.getMessage().contains(fragment), probe.getMessage());
      } else {
        assertEquals(1, GlobalValueDictionary.probe(headerKey, firstValue, reader));
      }
    }
  }

  /**
   * A failure while WRITING the table or the separator array — after the arguments were validated,
   * after keys were reserved, with some records already on their pages — must leave the transaction
   * rollback-only: a commit would persist a header pointing at a half-written table, and a caller
   * that catches the exception must not be able to commit past it. The failure is injected at the
   * writer's {@code persistRecord}, first on the very first record and then on the header rewrite
   * that concludes the write, so that both the earliest and the latest failure point poison.
   */
  @Test
  @DisplayName("a failure while writing the table or the separator array poisons the transaction")
  void writeFailuresPoisonTheTransaction() {
    final List<byte[]> sorted = buildSortedValueSet();
    final int prefix = sorted.size();
    final int[] rankByMint = shuffledPermutation(prefix, 0xF00DL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      // attachRankTable writes forward record, inverse record, header: three persists.
      for (final int failAt : new int[] {1, 3}) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
          final StorageEngineWriter real = wtx.getStorageEngineWriter();
          final NamePage namePage = real.getNamePage(real.getActualRevisionRootPage());
          final long headerKey = flushRankOrdered(sorted, namePage, real);
          real.assertTransactionWritable();
          final FailingPersistWriter failing = new FailingPersistWriter(real, failAt);
          final IllegalStateException injected =
              assertThrows(IllegalStateException.class, () -> GlobalValueDictionary.attachRankTable(headerKey,
                  rankByMint, namePage, DatabaseType.JSON, failing, real.getLog()));
          assertTrue(injected.getMessage().startsWith("injected"), injected.getMessage());
          assertEquals(failAt, failing.calls(), "the failure fired at the intended persist");
          final SirixIOException poisoned = assertThrows(SirixIOException.class, real::assertTransactionWritable);
          assertSame(injected, poisoned.getCause(), "the transaction names the injected failure as its cause");
          assertCausedBy(assertThrows(RuntimeException.class, wtx::commit, "a poisoned transaction refuses to commit"),
              injected);
          wtx.rollback();
        }
      }
      // buildBlockIndex writes the separator array, then the header: two persists.
      for (final int failAt : new int[] {1, 2}) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
          final StorageEngineWriter real = wtx.getStorageEngineWriter();
          final NamePage namePage = real.getNamePage(real.getActualRevisionRootPage());
          final long headerKey = flushRankOrdered(sorted, namePage, real);
          final FailingPersistWriter failing = new FailingPersistWriter(real, failAt);
          final IllegalStateException injected =
              assertThrows(IllegalStateException.class, () -> GlobalValueDictionary.buildBlockIndex(headerKey, namePage,
                  DatabaseType.JSON, failing, real.getLog()));
          assertTrue(injected.getMessage().startsWith("injected"), injected.getMessage());
          assertEquals(failAt, failing.calls());
          final SirixIOException poisoned = assertThrows(SirixIOException.class, real::assertTransactionWritable);
          assertSame(injected, poisoned.getCause());
          assertCausedBy(assertThrows(RuntimeException.class, wtx::commit), injected);
          wtx.rollback();
        }
      }
      // The control: the same writes with no failure injected leave the transaction writable.
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter real = wtx.getStorageEngineWriter();
        final NamePage namePage = real.getNamePage(real.getActualRevisionRootPage());
        final long headerKey = flushRankOrdered(sorted, namePage, real);
        final FailingPersistWriter counting = new FailingPersistWriter(real, Integer.MAX_VALUE);
        GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, counting, real.getLog());
        assertEquals(2, counting.calls(), "separator array + header");
        GlobalValueDictionary.attachRankTable(headerKey, rankByMint, namePage, DatabaseType.JSON, counting,
            real.getLog());
        assertEquals(5, counting.calls(), "+ forward record + inverse record + header");
        real.assertTransactionWritable();
        wtx.commit();
      }
    }
  }

  /** {@code thrown} is {@code cause} or carries it somewhere down its cause chain. */
  private static void assertCausedBy(final Throwable thrown, final Throwable cause) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t == cause) {
        return;
      }
    }
    throw new AssertionError("expected the failure to be caused by " + cause, thrown);
  }

  /**
   * Forwards everything to the real writer; {@code persistRecord} number {@code failAtCall} throws.
   */
  private static final class FailingPersistWriter extends AbstractForwardingStorageEngineWriter {
    private final StorageEngineWriter delegate;
    private final int failAtCall;
    private int calls;

    FailingPersistWriter(final StorageEngineWriter delegate, final int failAtCall) {
      this.delegate = delegate;
      this.failAtCall = failAtCall;
    }

    @Override
    protected StorageEngineWriter delegate() {
      return delegate;
    }

    @Override
    public void persistRecord(final DataRecord record, final IndexType indexType, final int index) {
      if (++calls == failAtCall) {
        throw new IllegalStateException("injected persist failure at call " + calls + " (" + record.getKind() + ")");
      }
      delegate.persistRecord(record, indexType, index);
    }

    int calls() {
      return calls;
    }
  }

  private static long flushRankOrdered(final List<byte[]> sorted, final NamePage namePage,
      final StorageEngineWriter writer) {
    final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
    dictionary.markRankOrdered();
    for (int i = 0; i < sorted.size(); i++) {
      final byte[] value = sorted.get(i);
      assertEquals(i + 1, dictionary.intern(value, 0, value.length), "positions are minted densely from 1");
    }
    final long headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
    dictionary.release();
    return headerKey;
  }

  /** {@code result[1..n]} is a uniformly random permutation of {@code 1..n}; index 0 is unused. */
  private static int[] shuffledPermutation(final int n, final long seed) {
    final int[] permutation = new int[n + 1];
    for (int i = 1; i <= n; i++) {
      permutation[i] = i;
    }
    final SplittableRandom random = new SplittableRandom(seed);
    for (int i = n; i > 1; i--) {
      final int j = 1 + random.nextInt(i);
      final int swap = permutation[i];
      permutation[i] = permutation[j];
      permutation[j] = swap;
    }
    return permutation;
  }

  /** Makes {@code permutation[mint] == rank} by swapping, keeping it a permutation. */
  private static void swapToPin(final int[] permutation, final int mint, final int rank) {
    for (int other = 1; other < permutation.length; other++) {
      if (permutation[other] == rank) {
        permutation[other] = permutation[mint];
        permutation[mint] = rank;
        return;
      }
    }
    throw new IllegalStateException("rank " + rank + " missing from the permutation");
  }

  /**
   * The value set of {@code OrderedPrefixProbeEqualsHashProbeTest}: blocks, a spill, collation traps.
   */
  private static List<byte[]> buildSortedValueSet() {
    final List<byte[]> values = new ArrayList<>();
    values.add(new byte[0]);
    for (int i = 0; i < FILLER_VALUES; i++) {
      values.add(utf8(String.format("value-%06d", i)));
    }
    final String shared = "https://example.invalid/a/very/long/shared/prefix/that/keeps/going/for/a/while/";
    for (int i = 0; i < 40; i++) {
      values.add(utf8(shared + String.format("%04d", i)));
    }
    values.add(utf8("collate-"));
    values.add(utf8("collate-"));
    values.add(utf8("collate-" + new String(Character.toChars(0x10000))));
    values.add(utf8("collate-" + new String(Character.toChars(0x1F600))));
    final byte[] oversized = new byte[ValueDictionaryEntryNode.MAX_VALUE_LENGTH];
    Arrays.fill(oversized, (byte) 'z');
    oversized[0] = 'o';
    values.add(oversized);
    values.sort(RankTableReadViewTest::compareCollation);
    for (int i = 1; i < values.size(); i++) {
      assertNotEquals(0, compareCollation(values.get(i - 1), values.get(i)), "the fixture must be strictly ascending");
    }
    return values;
  }

  private static List<byte[]> buildAbsentValues(final List<byte[]> sorted) {
    final List<byte[]> absent = new ArrayList<>();
    absent.add(utf8("before-everything"));
    absent.add(utf8("zzzz-after-everything"));
    absent.add(utf8("value-000005-and-a-half"));
    absent.add(utf8("value-000255x"));
    absent.add(utf8("value-000256x"));
    for (final byte[] value : absent) {
      for (final byte[] present : sorted) {
        assertNotEquals(0, compareCollation(value, present), "an 'absent' fixture value is actually present");
      }
    }
    return absent;
  }

  private static int compareCollation(final byte[] left, final byte[] right) {
    return ValueDictionaryEntryNode.compareUtf16Range(left, 0, left.length, right, 0, right.length);
  }

  /** The 1-based storage position of the oversized (spilled) fixture value. */
  private static int positionOfOversized(final List<byte[]> sorted) {
    for (int position = 1; position <= sorted.size(); position++) {
      if (sorted.get(position - 1).length == ValueDictionaryEntryNode.MAX_VALUE_LENGTH) {
        return position;
      }
    }
    throw new IllegalStateException("the fixture no longer holds an oversized value");
  }

  private static boolean isSet(final long[] verdict, final int id) {
    return (verdict[id >>> 6] & 1L << (id & 63)) != 0L;
  }

  private static int cardinality(final long[] verdict) {
    int bits = 0;
    for (final long word : verdict) {
      bits += Long.bitCount(word);
    }
    return bits;
  }

  /**
   * The single set bit of {@code verdict}, for a failure message; {@code -1} when there is not one.
   */
  private static int onlySetBit(final long[] verdict) {
    int found = -1;
    for (int word = 0; word < verdict.length; word++) {
      long remaining = verdict[word];
      while (remaining != 0L) {
        if (found >= 0) {
          return -1;
        }
        found = (word << 6) + Long.numberOfTrailingZeros(remaining);
        remaining &= remaining - 1;
      }
    }
    return found;
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String describe(final byte[] value) {
    final String text = new String(value, StandardCharsets.UTF_8);
    return text.length() > 60
        ? text.substring(0, 60) + "…(" + value.length + " B)"
        : text;
  }
}
