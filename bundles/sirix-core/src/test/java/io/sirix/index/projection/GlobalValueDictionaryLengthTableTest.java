package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-id string-length table of a {@link GlobalValueDictionary.ReadView}: derived over disjoint
 * id ranges by several views (the executor's parallel lanes) it must equal the single-view table,
 * entry for entry, in both length modes — including the entries the dictionary SPILLS (over-long
 * values live in their own record, whose length is read from the record rather than the slice) and
 * the multi-byte values where code points and UTF-8 bytes disagree. And the
 * {@link ProjectionIndexRegistry.Handle} memo that keeps such a table across queries must keep it
 * within its byte bound only, first derivation winning.
 */
final class GlobalValueDictionaryLengthTableTest {

  private static final String RESOURCE_NAME = "lengthTableResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Enough ids to span many reverse buckets (256 ids each), so ranges cross bucket boundaries. */
  private static final int ENTRIES = 5_000;

  /** Two-byte code points enough to exceed a value block — the value then SPILLS to its own record. */
  private static final int SPILL_REPEATS = ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES / 2 + 1;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Values of varying byte length and code-point count: ASCII, two-, three- and four-byte code
   * points, and every 97th value over-long enough to spill into its own record.
   */
  private static String valueOf(final int id) {
    final StringBuilder sb = new StringBuilder(64);
    sb.append("v").append(id);
    switch (id % 5) {
      case 1 -> sb.append("é".repeat(id % 7));
      case 2 -> sb.append("€".repeat(1 + id % 3));
      case 3 -> sb.append("𐐀".repeat(1 + id % 2));
      default -> {
      }
    }
    if (id % 97 == 0) {
      sb.append("-").append("ü".repeat(SPILL_REPEATS));
    }
    return sb.toString();
  }

  private static long build(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (int i = 1; i <= ENTRIES; i++) {
        final byte[] utf8 = valueOf(i).getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
      return headerKey;
    }
  }

  @Test
  @DisplayName("ranged fills by several views equal the whole-view table in both modes, spills included")
  void rangedFillsEqualTheWholeTable() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey = build(session);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView whole = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(whole);
        assertEquals(ENTRIES, whole.entryCount());

        final int[] bytesTable = whole.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES);
        final int[] codePointTable = whole.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS);
        assertEquals(ENTRIES + 1, bytesTable.length);
        assertEquals(0, bytesTable[0], "slot 0 is unused");

        // Exactness against the materialised values, id by id — the two modes must DISAGREE on the
        // multi-byte ids or the code-point lane is not being exercised.
        int disagreements = 0;
        int spilled = 0;
        for (int id = 1; id <= ENTRIES; id++) {
          final String value = GlobalValueDictionary.value(headerKey, id, reader);
          final int utf8 = value.getBytes(StandardCharsets.UTF_8).length;
          final int codePoints = value.codePointCount(0, value.length());
          assertEquals(utf8, bytesTable[id], "utf8 length of id " + id);
          assertEquals(codePoints, codePointTable[id], "code points of id " + id);
          if (utf8 != codePoints) {
            disagreements++;
          }
          if (utf8 > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES) {
            spilled++;
          }
        }
        assertTrue(disagreements > ENTRIES / 2, "multi-byte values must be present: " + disagreements);
        assertTrue(spilled >= ENTRIES / 97 - 1, "over-long values must be present: " + spilled);

        // Disjoint ranges, one fresh view each, into one shared table — the executor's lanes.
        final int lanes = 7;
        for (final byte mode : new byte[] {ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES,
            ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS}) {
          final int[] ranged = new int[ENTRIES + 1];
          for (int lane = 0; lane < lanes; lane++) {
            final int lo = 1 + (int) ((long) ENTRIES * lane / lanes);
            final int hi = (int) ((long) ENTRIES * (lane + 1) / lanes);
            final GlobalValueDictionary.ReadView laneView = GlobalValueDictionary.readView(headerKey, reader);
            assertNotNull(laneView);
            laneView.fillLengthTable(mode, lo, hi, ranged);
          }
          assertArrayEquals(mode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
              ? bytesTable
              : codePointTable, ranged, "mode " + mode);
        }

        // Refusals: a foreign mode, a range outside the dictionary, a table too short.
        final int[] scratch = new int[ENTRIES + 1];
        assertThrows(IllegalArgumentException.class,
            () -> whole.fillLengthTable(ProjectionIndexByteScan.STRING_LENGTH_NONE, 1, ENTRIES, scratch));
        assertThrows(IllegalArgumentException.class,
            () -> whole.fillLengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, 0, 10, scratch));
        assertThrows(IllegalArgumentException.class,
            () -> whole.fillLengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, 1, ENTRIES + 1, scratch));
        assertThrows(IllegalArgumentException.class,
            () -> whole.fillLengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, 1, ENTRIES, new int[ENTRIES]));
      }
    }
  }

  @Test
  @DisplayName("the handle memo keeps tables within its byte bound, first derivation winning")
  void handleMemoKeepsTablesWithinItsBound() {
    final ProjectionIndexRegistry.Handle handle =
        new ProjectionIndexRegistry.Handle(new String[] {"a", "b"}, List.of());
    final long dictionaryA = 17L;
    final long dictionaryB = 23L;
    final byte bytesMode = ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES;
    final byte codePointsMode = ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS;

    assertNull(handle.stringLengthTable(dictionaryA, bytesMode));
    assertEquals(0L, handle.stringLengthTableBytes());

    final int[] first = new int[1001];
    assertTrue(handle.noteStringLengthTable(dictionaryA, bytesMode, first));
    assertSame(first, handle.stringLengthTable(dictionaryA, bytesMode));
    assertEquals(16L + 4L * 1001, handle.stringLengthTableBytes());
    // Same dictionary, other mode: a different table; same dictionary, same mode: the first stays.
    assertNull(handle.stringLengthTable(dictionaryA, codePointsMode));
    final int[] again = new int[1001];
    assertTrue(handle.noteStringLengthTable(dictionaryA, bytesMode, again), "an already-kept key reports kept");
    assertSame(first, handle.stringLengthTable(dictionaryA, bytesMode), "the first derivation wins");
    assertEquals(16L + 4L * 1001, handle.stringLengthTableBytes(), "a duplicate is not charged");

    // Past the bound a table is refused, uncharged and not retrievable; a bound of 0 keeps nothing.
    final long previousBound = ProjectionIndexRegistry.setStringLengthMemoBytesForTesting(16L + 4L * 1001 + 100L);
    try {
      assertFalse(handle.noteStringLengthTable(dictionaryB, bytesMode, new int[101]));
      assertNull(handle.stringLengthTable(dictionaryB, bytesMode));
      assertEquals(16L + 4L * 1001, handle.stringLengthTableBytes());
      assertTrue(handle.noteStringLengthTable(dictionaryB, bytesMode, new int[20]), "a table within the bound is kept");
      assertEquals(16L + 4L * 1001 + 16L + 4L * 20, handle.stringLengthTableBytes());
      ProjectionIndexRegistry.setStringLengthMemoBytesForTesting(0L);
      assertFalse(handle.noteStringLengthTable(dictionaryB, codePointsMode, new int[1]), "bound 0 disables retention");
      assertNull(handle.stringLengthTable(dictionaryB, codePointsMode));
    } finally {
      ProjectionIndexRegistry.setStringLengthMemoBytesForTesting(previousBound);
    }

    // Key refusals: a non-positive dictionary key and a foreign mode.
    assertThrows(IllegalArgumentException.class, () -> handle.stringLengthTable(0L, bytesMode));
    assertThrows(IllegalArgumentException.class, () -> handle.noteStringLengthTable(dictionaryA, (byte) 9, first));
  }
}
