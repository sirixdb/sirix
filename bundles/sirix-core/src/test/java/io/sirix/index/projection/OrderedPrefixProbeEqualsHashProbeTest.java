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
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * W17 — the binary-search probe over an ordered prefix answers IDENTICALLY to the hash probe.
 *
 * <p>
 * A rank-ordered dictionary carries no forward hash index, because "which id holds this value" is a
 * binary search over a reverse index that is already sorted by value. That is a large saving and it
 * is also a second implementation of the single most correctness-critical operation the dictionary
 * has, so it is proven equal rather than believed equal.
 * </p>
 *
 * <p>
 * <b>How the differential is constructed.</b> The same values are interned into two dictionaries in
 * the same ascending order, so both mint identical ids. One is declared rank-ordered and therefore
 * has {@code forwardRootKey == 0} and is probed by binary search; the other is not, so it builds
 * the forward index and is probed by hash. The test asserts that difference explicitly, because a
 * differential in which both sides ran the same code would pass while proving nothing.
 * </p>
 *
 * <p>
 * <b>THE SHARED PREFIX IN THE COLLATION CASES IS LOAD-BEARING. Do not "tidy" it.</b> The first
 * version of this fixture used {@code "private-use-\uE000"} against
 * {@code "supplementary-<U+10000>"}, and the comparator mutation PASSED — because the ASCII
 * prefixes differ, so every comparison was settled before it reached the character that
 * distinguishes UTF-8 byte order from UTF-16 code-unit order. The witness proved less than it
 * claimed, and only running the mutation revealed it. Any case added here to exercise the collation
 * must share its prefix with the case it is contrasted against.
 * </p>
 *
 * <p>
 * <b>Mutations this must fail:</b> dropping the final byte comparison after the search converges
 * (the collision pair then returns its neighbour's id); using {@code <} where {@code <=} is meant
 * at a block boundary (the first value of each block is reported absent); comparing with unsigned
 * byte order instead of {@link ValueDictionaryEntryNode#compareUtf16Range} (the
 * supplementary-character cases inverts against the U+E000..U+FFFF ones).
 * </p>
 *
 * <p>
 * <b>One honest limitation.</b> The "collision pair" here collides on the PRIMARY BUCKET
 * ({@code valueHash >>> 40}), which is what actually exercises the radix's secondary-hash and
 * collision-tree path. A full 64-bit FNV-1a collision is not brute-forceable and would have to be
 * built algebraically from the hash's invertibility; that is not done here, and the case this test
 * does cover is the one where the two probes could realistically diverge.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class OrderedPrefixProbeEqualsHashProbeTest {

  private static final String RESOURCE_NAME = "orderedProbeResource";

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Enough to span several 256-value blocks and several 256-id reverse buckets. */
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
  void binarySearchProbeAnswersExactlyWhatTheHashProbeAnswers() {
    final List<byte[]> sorted = buildSortedValueSet();
    final List<byte[]> absent = buildAbsentValues(sorted);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long orderedHeaderKey;
      final long hashedHeaderKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
        orderedHeaderKey = flushDictionary(wtx, sorted, true);
        hashedHeaderKey = flushDictionary(wtx, sorted, false);
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final ValueDictionaryHeaderNode ordered = GlobalValueDictionary.header(orderedHeaderKey, reader);
        final ValueDictionaryHeaderNode hashed = GlobalValueDictionary.header(hashedHeaderKey, reader);

        // The differential is only worth anything if the two sides really run different code.
        assertEquals(0L, ordered.getForwardRootKey(), "a rank-ordered dictionary must carry no forward index");
        assertTrue(ordered.isFullyOrdered(), "every id of the rank-ordered dictionary must be in collation order");
        assertEquals(sorted.size(), ordered.getOrderedPrefixCount());
        // Positive witness for the separator array: without this the search falls back to scanning
        // the whole ordered prefix, which is correct and therefore invisible to every assertion
        // below — the test would keep passing while proving nothing about the structure it exists
        // to guard.
        assertNotEquals(0L, ordered.getBlockIndexKey(), "the rank-ordered dictionary must carry a separator array");
        assertNotEquals(0L, hashed.getForwardRootKey(),
            "the control must build the forward index it is the control for");
        assertEquals(0, hashed.getOrderedPrefixCount(), "the control must not claim any ordered prefix");
        assertEquals(ordered.getEntryCount(), hashed.getEntryCount());

        for (int index = 0; index < sorted.size(); index++) {
          final byte[] value = sorted.get(index);
          final int expected = index + 1;
          final int viaSearch = GlobalValueDictionary.probe(orderedHeaderKey, value, reader);
          final int viaHash = GlobalValueDictionary.probe(hashedHeaderKey, value, reader);
          assertEquals(expected, viaHash, () -> "the hash probe lost id " + expected);
          assertEquals(viaHash, viaSearch, () -> "probes disagree on the value ranked " + expected);
        }

        for (final byte[] value : absent) {
          final int viaSearch = GlobalValueDictionary.probe(orderedHeaderKey, value, reader);
          final int viaHash = GlobalValueDictionary.probe(hashedHeaderKey, value, reader);
          assertEquals(GlobalValueDictionary.ID_ABSENT, viaHash,
              () -> "the hash probe must answer ABSENT, not UNKNOWN or a neighbour, for " + describe(value));
          assertEquals(GlobalValueDictionary.ID_ABSENT, viaSearch,
              () -> "the binary search must answer ABSENT, not UNKNOWN or a neighbour, for " + describe(value));
        }
      }
    }
  }

  /**
   * Every value the mechanism could get wrong, then sorted into the order the ranks are minted in.
   *
   * <p>
   * Block and bucket boundaries fall at multiples of 256 by construction
   * ({@link ValueDictionaryValueBlockNode#MAX_BLOCK_VALUES} and
   * {@link ValueDictionaryValueBucketNode#VALUES_PER_BUCKET}), and the filler is wide enough that
   * several of each are crossed; the specials below cover what the boundaries cannot.
   * </p>
   */
  private static List<byte[]> buildSortedValueSet() {
    final List<byte[]> values = new ArrayList<>();
    values.add(new byte[0]);
    for (int i = 0; i < FILLER_VALUES; i++) {
      values.add(utf8(String.format("value-%06d", i)));
    }
    // A neighbourhood sharing a long prefix: front coding's worst case for a mid-block landing, and
    // the case where a search that stops at the first differing byte lands on the wrong side.
    final String shared = "https://example.invalid/a/very/long/shared/prefix/that/keeps/going/for/a/while/";
    for (int i = 0; i < 40; i++) {
      values.add(utf8(shared + String.format("%04d", i)));
    }
    // Supplementary characters against the U+E000..U+FFFF range: the ONE place UTF-8 byte order and
    // UTF-16 code-unit order disagree. The prefix MUST be identical, or the comparison is decided
    // before it reaches the differing character and the case proves nothing — which is exactly what
    // the first version of this fixture did, and the mutation caught it.
    // UTF-8 bytes: 0xEE (U+E000) < 0xF0 (U+10000) -> private use first
    // UTF-16 units: 0xD800 (surrogate) < 0xE000 -> supplementary first
    values.add(utf8("collate-\uE000"));
    values.add(utf8("collate-\uF8FF"));
    values.add(utf8("collate-" + new String(Character.toChars(0x10000))));
    values.add(utf8("collate-" + new String(Character.toChars(0x1F600))));
    // Longest admitted value: above MAX_BLOCK_BYTES, so it SPILLS to its own entry node and the
    // search has to reach a value that is not in any block.
    final byte[] oversized = new byte[ValueDictionaryEntryNode.MAX_VALUE_LENGTH];
    java.util.Arrays.fill(oversized, (byte) 'z');
    oversized[0] = 'o';
    values.add(oversized);
    values.addAll(primaryBucketCollisionPair());

    values.sort(OrderedPrefixProbeEqualsHashProbeTest::compareCollation);
    // The rank pass may only be handed a strictly ascending stream, so a duplicate here would be a
    // defect in the fixture rather than in what it tests.
    for (int i = 1; i < values.size(); i++) {
      assertNotEquals(0, compareCollation(values.get(i - 1), values.get(i)), "the fixture must be strictly ascending");
    }
    return values;
  }

  /**
   * Two distinct values whose {@code valueHash} lands in the same primary bucket, so the forward
   * probe must separate them by secondary hash and byte comparison rather than by bucket alone.
   */
  private static List<byte[]> primaryBucketCollisionPair() {
    final Map<Integer, byte[]> byBucket = new HashMap<>();
    for (int i = 0; i < 400_000; i++) {
      final byte[] candidate = utf8("collide-" + i);
      final long hash = GlobalValueDictionary.valueHash(candidate, 0, candidate.length);
      final int bucket = (int) (hash >>> 40) & 0xFF_FFFF;
      final byte[] previous = byBucket.putIfAbsent(bucket, candidate);
      if (previous != null) {
        return List.of(previous, candidate);
      }
    }
    throw new IllegalStateException("no primary-bucket collision found; the bucket derivation changed");
  }

  /** Absent values placed before the first entry, after the last, and between two adjacent ones. */
  private static List<byte[]> buildAbsentValues(final List<byte[]> sorted) {
    final List<byte[]> absent = new ArrayList<>();
    absent.add(utf8("before-everything"));
    absent.add(utf8("zzzz-after-everything"));
    absent.add(utf8("value-000005-and-a-half"));
    absent.add(utf8("https://example.invalid/a/very/long/shared/prefix/that/keeps/going/for/a/while/0007x"));
    // Absent, and adjacent to a block boundary from both sides.
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

  /**
   * Interns {@code sorted} into one dictionary. When {@code rankOrdered}, the writer is told the
   * stream is in collation order, which is what suppresses the forward index and records the
   * boundary; otherwise this is the ordinary streaming shape and acts as the control.
   */
  private static long flushDictionary(final JsonNodeTrx wtx, final List<byte[]> sorted, final boolean rankOrdered) {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    if (rankOrdered) {
      writer.markRankOrdered();
    }
    for (int i = 0; i < sorted.size(); i++) {
      final byte[] value = sorted.get(i);
      final int minted = writer.intern(value, 0, value.length);
      assertEquals(i + 1, minted, "ids must be minted densely from 1 in call order");
    }
    final var storageEngineWriter = wtx.getStorageEngineWriter();
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final long headerKey = writer.flush(namePage, DatabaseType.JSON, storageEngineWriter, storageEngineWriter.getLog());
    writer.release();
    if (rankOrdered) {
      // The separator array must be BUILT here, or the search silently takes the whole-prefix
      // fallback and every mutation of the range logic passes. That is not hypothetical: both
      // separator mutations survived this test until this call was added.
      GlobalValueDictionary.buildBlockIndex(headerKey, namePage, DatabaseType.JSON, storageEngineWriter,
          storageEngineWriter.getLog());
    }
    return headerKey;
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
