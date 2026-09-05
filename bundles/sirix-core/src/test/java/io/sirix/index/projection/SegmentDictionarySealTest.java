/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.ValueDictionaryRankTableNode;
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
 * The seal takes a segment's values in MINT order and writes a dictionary whose storage is in
 * collation order, with a rank table from every mint to its position — or no table at all when the
 * mints already are the ranks. What must hold afterwards, through a real writer and a real reader:
 * every mint decodes to exactly the bytes the segment minted under it, every value probes back to its
 * mint, the storage is provably sorted (positions ascend in collation), the separator array exists
 * over a dictionary wide enough to need one and survives the table, a slot wider than one interner
 * generation chains and stays one ordered run, and a refused seal reserves no key.
 *
 * <p>
 * <b>Mutations this must fail:</b> the seal skipping {@code attachRankTable} (mints decode to the
 * value at their own position — the scrambled fixture pins mint 1 to the LAST value); building the
 * table before the block index (refused by {@code buildBlockIndex}); a comparator in byte order
 * rather than UTF-16 order (the astral trap inverts); the identity check inverted (a table where none
 * is needed, or none where one is); the strictness check dropped (a duplicate value seals); the
 * generation cut off by one (the interner refuses the 16385th value).
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class SegmentDictionarySealTest {

  private static final String RESOURCE_NAME = "segmentSealResource";

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Several 256-value blocks and more than one separator range. */
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
  @DisplayName("a scrambled segment seals to sorted storage under a rank table, and every mint reads back")
  void aScrambledSegmentSealsUnderATable() {
    final List<byte[]> sorted = buildSortedValueSet();
    final int count = sorted.size();
    // rankByMint[m] = the position of mint m; the extremes pinned so an untranslated route is wrong
    // at both ends.
    final int[] rankByMint = shuffledPermutation(count, 0x5EA1L);
    swapToPin(rankByMint, 1, count);
    swapToPin(rankByMint, count, 1);
    final byte[][] valuesById = new byte[count][];
    for (int mint = 1; mint <= count; mint++) {
      valuesById[mint - 1] = sorted.get(rankByMint[mint] - 1);
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final SegmentDictionarySeal.Sealed sealed;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        sealed = SegmentDictionarySeal.write(writer, 3, valuesById);
        assertFalse(sealed.wroteNothing());
        assertEquals(count, sealed.entryCount());
        assertTrue(sealed.rankTableKey() > 0L, "a scrambled segment needs a table");
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(sealed.headerKey(), writer);
        assertNotNull(header);
        assertEquals(sealed.rankTableKey(), header.getRankTableKey());
        assertTrue(header.isFullyOrdered(), "the STORAGE is in collation order");
        assertFalse(header.idsAreCollationOrdered(), "the IDS are mints");
        assertEquals(count, header.getEntryCount());
        assertEquals(count, header.getOrderedPrefixCount());
        assertNotEquals(0L, header.getBlockIndexKey(), "wide enough for a separator array, built before the table");
        assertEquals(0L, header.getForwardRootKey(), "a segment dictionary never carries a forward radix");
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final long headerKey = sealed.headerKey();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertFalse(view.fullyOrdered(), "no arm may compare mints as values");
        assertEquals(count, view.entryCount());
        // DECODE: every mint yields the bytes minted under it, by the view and the static route.
        for (int mint = 1; mint <= count; mint++) {
          final int m = mint;
          assertArrayEquals(valuesById[mint - 1], GlobalValueDictionary.valueBytes(headerKey, mint, reader),
              () -> "static decode of mint " + m);
          assertEquals(new String(valuesById[mint - 1], StandardCharsets.UTF_8), view.valueAsString(mint),
              () -> "view decode of mint " + m);
        }
        assertNull(GlobalValueDictionary.valueBytes(headerKey, count + 1, reader));
        // STORAGE ORDER: position p holds the p-th value in collation, whatever mint carries it.
        for (int position = 1; position <= count; position++) {
          assertEquals(position, view.positionOf(view.mintAtPosition(position)));
          assertArrayEquals(sorted.get(position - 1), valuesById[view.mintAtPosition(position) - 1],
              "position " + position + " must hold the value that sorts there");
        }
        // ENCODE: every value probes back to its MINT, not to the position the search found.
        for (int mint = 1; mint <= count; mint++) {
          final int m = mint;
          assertEquals(mint, GlobalValueDictionary.probe(headerKey, valuesById[mint - 1], reader),
              () -> "probe of mint " + m + "'s value");
        }
        assertEquals(GlobalValueDictionary.ID_ABSENT,
            GlobalValueDictionary.probe(headerKey, utf8("value-000005-and-a-half"),
            reader));
        // ORDER: comparing mints compares their values.
        final SplittableRandom random = new SplittableRandom(7L);
        for (int i = 0; i < 2_000; i++) {
          final int left = 1 + random.nextInt(count);
          final int right = 1 + random.nextInt(count);
          assertEquals(Integer.signum(compareCollation(valuesById[left - 1], valuesById[right - 1])),
              Integer.signum(view.compareIds(left, right)), () -> "compareIds(" + left + ", " + right + ")");
        }
        assertTrue(view.compareIds(1, count) > 0, "mint 1 holds the LAST value");
      }
    }
  }

  @Test
  @DisplayName("a segment whose mints arrived in collation order seals without a table")
  void anOrderedSegmentNeedsNoTable() {
    final List<byte[]> sorted = buildSortedValueSet().subList(0, 300);
    final byte[][] valuesById = sorted.toArray(new byte[0][]);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final SegmentDictionarySeal.Sealed sealed;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        sealed = SegmentDictionarySeal.write(wtx.getStorageEngineWriter(), 0, valuesById);
        assertEquals(0L, sealed.rankTableKey(), "identity: the mints ARE the positions, no table");
        assertEquals(valuesById.length, sealed.entryCount());
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(sealed.headerKey(), reader);
        assertNotNull(header);
        assertFalse(header.hasRankTable());
        assertTrue(header.idsAreCollationOrdered(), "ids are positions: the cheapest dictionary to serve");
        assertNotEquals(0L, header.getBlockIndexKey(),
            "300 values span two separator ranges: the array exists over a dictionary whose ids ARE positions");
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(sealed.headerKey(), reader);
        assertNotNull(view);
        assertTrue(view.fullyOrdered());
        for (int mint = 1; mint <= valuesById.length; mint++) {
          assertArrayEquals(valuesById[mint - 1], GlobalValueDictionary.valueBytes(sealed.headerKey(), mint, reader));
          assertEquals(mint, GlobalValueDictionary.probe(sealed.headerKey(), valuesById[mint - 1], reader));
        }
      }
    }
  }

  @Test
  @DisplayName("a slot wider than one interner generation chains into one ordered run under one table")
  void aWideSlotChainsGenerations() {
    final int count = 2 * SegmentDictionarySeal.GENERATION_ENTRIES + 5;
    final byte[][] sortedValues = new byte[count][];
    for (int i = 0; i < count; i++) {
      sortedValues[i] = utf8(String.format("w-%08d", i));
    }
    // Reverse arrival: mint m holds the value at position count + 1 - m, so every generation
    // boundary is crossed by the permutation and no mint is its own rank.
    final byte[][] valuesById = new byte[count][];
    for (int mint = 1; mint <= count; mint++) {
      valuesById[mint - 1] = sortedValues[count - mint];
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final SegmentDictionarySeal.Sealed sealed;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        sealed = SegmentDictionarySeal.write(wtx.getStorageEngineWriter(), 1, valuesById);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(sealed.headerKey(), reader);
        assertNotNull(header);
        assertEquals(count, header.getEntryCount());
        assertEquals(count, header.getOrderedPrefixCount(),
            "three chained rank-ordered generations stay ONE ordered run");
        assertEquals(2, header.getGeneration(), "two appends onto the first generation");
        assertTrue(header.hasRankTable());
        assertNotEquals(0L, header.getBlockIndexKey());
        for (int mint = 1; mint <= count; mint++) {
          final int m = mint;
          assertArrayEquals(valuesById[mint - 1], GlobalValueDictionary.valueBytes(sealed.headerKey(), mint, reader),
              () -> "decode of mint " + m + " across generations");
        }
        final SplittableRandom random = new SplittableRandom(11L);
        for (int i = 0; i < 1_000; i++) {
          final int mint = 1 + random.nextInt(count);
          assertEquals(mint, GlobalValueDictionary.probe(sealed.headerKey(), valuesById[mint - 1], reader));
        }
        // The generation seams in particular: the last mint of one interner and the first of the next.
        for (int seam = SegmentDictionarySeal.GENERATION_ENTRIES; seam <= count;
            seam += SegmentDictionarySeal.GENERATION_ENTRIES) {
          assertEquals(seam, GlobalValueDictionary.probe(sealed.headerKey(), valuesById[seam - 1], reader));
          assertEquals(seam + 1, GlobalValueDictionary.probe(sealed.headerKey(), valuesById[seam], reader));
        }

        // The table spans THREE records per run. Under the reverse arrival every mint's position is
        // count + 1 - mint, so a translation that picked the wrong record — or the forward run where
        // the inverse was meant — is off at every record seam, not only somewhere in the middle.
        final int records = ValueDictionaryRankTableNode.recordCountFor(count);
        assertEquals(3, records);
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(sealed.headerKey(), reader);
        assertNotNull(view);
        assertTrue(view.hasRankTable());
        final int[] probes = {1, 2, ValueDictionaryRankTableNode.ENTRIES_PER_RECORD,
            ValueDictionaryRankTableNode.ENTRIES_PER_RECORD + 1, 2 * ValueDictionaryRankTableNode.ENTRIES_PER_RECORD,
            2 * ValueDictionaryRankTableNode.ENTRIES_PER_RECORD + 1, count - 1, count};
        for (final int mint : probes) {
          assertEquals(count + 1 - mint, view.positionOf(mint), "positionOf(" + mint + ")");
          assertEquals(count + 1 - mint, view.mintAtPosition(mint), "mintAtPosition(" + mint + ")");
          assertEquals(mint, view.mintAtPosition(view.positionOf(mint)));
          assertEquals(new String(valuesById[mint - 1], StandardCharsets.UTF_8), view.valueAsString(mint),
              "view decode of mint " + mint);
          assertEquals(mint, view.probe(valuesById[mint - 1], 0, valuesById[mint - 1].length), "view probe " + mint);
        }
        assertThrows(IllegalStateException.class, () -> view.positionOf(count + 1));
        assertThrows(IllegalStateException.class, () -> view.mintAtPosition(count + 1));
        // The bulk route resolves a mixed handful across all three records in one call.
        final String[] bulk = GlobalValueDictionary.values(sealed.headerKey(), probes, reader);
        for (int i = 0; i < probes.length; i++) {
          assertEquals(new String(valuesById[probes[i] - 1], StandardCharsets.UTF_8), bulk[i], "bulk of " + probes[i]);
        }
        // Six records on disk: forward at tableKey + 0..2, inverse at tableKey + 3..5, nothing behind.
        final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
        final int bits = ValueDictionaryRankTableNode.bitsFor(count);
        for (int i = 0; i < 2 * records; i++) {
          final int withinRun = i < records
              ? i
              : i - records;
          final ValueDictionaryRankTableNode record = (ValueDictionaryRankTableNode) namePage
              .getProjectionValueDictionaryRecord(sealed.rankTableKey() + i, DatabaseType.JSON, reader);
          assertNotNull(record, "record " + i);
          assertEquals(1 + withinRun * ValueDictionaryRankTableNode.ENTRIES_PER_RECORD, record.firstKey(),
              "record " + i);
          assertEquals(Math.min(ValueDictionaryRankTableNode.ENTRIES_PER_RECORD,
              count - withinRun * ValueDictionaryRankTableNode.ENTRIES_PER_RECORD), record.size(), "record " + i);
          assertEquals(bits, record.bitsPerEntry(), "record " + i);
          // Both runs hold the SAME permutation here (it is an involution), which is exactly why the
          // view-level checks above are needed to tell them apart; the records themselves must agree.
          final int probeKey = record.firstKey();
          assertEquals(count + 1 - probeKey, record.entryOf(probeKey));
        }
        assertNull(namePage.getProjectionValueDictionaryRecord(sealed.rankTableKey() + 2L * records, DatabaseType.JSON,
            reader), "nothing is written behind the inverse run");
      }
    }
  }

  @Test
  @DisplayName("a refused seal reserves no key and writes no record")
  void refusalsReserveNoKey() {
    final List<byte[]> sorted = buildSortedValueSet().subList(0, 50);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      final StorageEngineWriter writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      namePage.createProjectionValueDictionaryTree(DatabaseType.JSON, writer, writer.getLog());
      final long keyBefore = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1);

      final byte[][] duplicated = sorted.toArray(new byte[0][]);
      duplicated[7] = duplicated[30].clone();
      final IllegalStateException duplicate = assertThrows(IllegalStateException.class,
          () -> SegmentDictionarySeal.write(writer, 0, duplicated));
      assertTrue(duplicate.getMessage().contains("two ids for one value"), duplicate.getMessage());

      final byte[][] holed = sorted.toArray(new byte[0][]);
      holed[12] = null;
      final IllegalStateException hole = assertThrows(IllegalStateException.class,
          () -> SegmentDictionarySeal.write(writer, 0, holed));
      assertTrue(hole.getMessage().contains("mint 13"), hole.getMessage());

      assertThrows(IllegalArgumentException.class, () -> SegmentDictionarySeal.write(writer, -1, duplicated));
      assertThrows(NullPointerException.class, () -> SegmentDictionarySeal.write(writer, 0, null));
      assertSame(SegmentDictionarySeal.Sealed.NOTHING, SegmentDictionarySeal.write(writer, 0, new byte[0][]));

      assertEquals(keyBefore + 1, namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON, 1),
          "no refused or empty seal reserved a key");
      wtx.rollback();
    }
  }

  @Test
  @DisplayName("mints are ranked in UTF-16 collation, strictly, and the identity is recognised")
  void rankingIsCollationNotByteOrder() {
    // U+FF61 is EF BD A1 in UTF-8 and a single BMP unit in UTF-16; U+10000 is F0 90 80 80 in UTF-8
    // and the surrogate pair D800 DC00 in UTF-16. Byte order puts U+FF61 first, UTF-16 order U+10000.
    final byte[] bmp = utf8("x" + new String(Character.toChars(0xFF61)));
    final byte[] astral = utf8("x" + new String(Character.toChars(0x10000)));
    assertTrue(Arrays.compareUnsigned(bmp, astral) < 0, "the fixture must invert between the two orders");
    final int[] mintsByRank = SegmentDictionarySeal.rankMints(new byte[][] {bmp, astral, utf8("a")});
    assertArrayEquals(new int[] {3, 2, 1}, mintsByRank, "a, then the astral value, then the BMP value");
    assertArrayEquals(new int[] {0, 3, 2, 1}, SegmentDictionarySeal.invert(mintsByRank));
    assertFalse(SegmentDictionarySeal.isIdentity(mintsByRank));
    assertTrue(SegmentDictionarySeal.isIdentity(SegmentDictionarySeal.rankMints(new byte[][] {utf8("a"), utf8("b")})));
    assertTrue(SegmentDictionarySeal.isIdentity(SegmentDictionarySeal.rankMints(new byte[0][])));
    assertFalse(SegmentDictionarySeal.isIdentity(SegmentDictionarySeal.rankMints(new byte[][] {utf8("b"), utf8("a")})));
    final IllegalStateException equal = assertThrows(IllegalStateException.class,
        () -> SegmentDictionarySeal.rankMints(new byte[][] {utf8("a"), utf8("b"), utf8("a")}));
    assertTrue(equal.getMessage().contains("mints 1 and 3"), equal.getMessage());
    assertThrows(IllegalStateException.class, () -> SegmentDictionarySeal.rankMints(new byte[][] {utf8("a"), null}));
    // The empty value is a value like any other and sorts first.
    assertArrayEquals(new int[] {2, 1}, SegmentDictionarySeal.rankMints(new byte[][] {utf8("a"), new byte[0]}));
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
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

  /** Blocks, a spill, collation traps: the value set of {@code RankTableReadViewTest}. */
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
    values.add(utf8("collate-" + new String(Character.toChars(0xF000))));
    values.add(utf8("collate-" + new String(Character.toChars(0xFEFF))));
    values.add(utf8("collate-" + new String(Character.toChars(0x10000))));
    values.add(utf8("collate-" + new String(Character.toChars(0x1F600))));
    final byte[] oversized = new byte[ValueDictionaryEntryNode.MAX_VALUE_LENGTH];
    Arrays.fill(oversized, (byte) 'z');
    oversized[0] = 'o';
    values.add(oversized);
    values.sort(SegmentDictionarySealTest::compareCollation);
    for (int i = 1; i < values.size(); i++) {
      assertNotEquals(0, compareCollation(values.get(i - 1), values.get(i)), "the fixture must be strictly ascending");
    }
    return values;
  }

  private static int compareCollation(final byte[] left, final byte[] right) {
    return ValueDictionaryEntryNode.compareUtf16Range(left, 0, left.length, right, 0, right.length);
  }
}
