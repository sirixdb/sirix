/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.GlobalValueDictionaryWriter;
import io.sirix.node.ValueDictionaryCollisionNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHashBucketNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.ValueDictionaryRadixNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The global projection value dictionary is a new sub-trie inside {@link NamePage}, and everything
 * downstream of it — integer group-by, integer distinct folds, integer equality — is only sound if
 * an id keeps meaning the same value for as long as any row group referring to it can be read.
 *
 * <p>
 * These tests are about that, not about the in-memory intern table. Four properties carry the
 * design:
 *
 * <ul>
 * <li><b>The offset run stays gapless.</b> {@link NamePage} serializes its bookkeeping
 * positionally, so occupying offset 2 on a resource whose offset 1 was never used would make the
 * next commit throw. A resource that never enabled FSST is therefore the interesting case, not the
 * boring one.</li>
 * <li><b>Keys are addressable.</b> The sub-trie's indirect-page traversal only grows a level when a
 * densely allocated page key crosses a power-of-two boundary, so a dictionary spanning many record
 * pages is the case that catches a key layout the trie cannot address — one that would otherwise
 * resolve every page to the root reference and let records overwrite each other silently.</li>
 * <li><b>Revisions are self-consistent.</b> A rebuild re-mints ids from 1; that is safe only
 * because copy-on-write keeps the earlier revision reading the dictionary it was built
 * against.</li>
 * <li><b>"Absent" and "cannot say" are different answers.</b> A probe that reported a value missing
 * when it merely could not see it would turn a fast path into a wrong answer.</li>
 * </ul>
 */
@DisplayName("the global projection value dictionary is a versioned sub-trie")
public final class GlobalValueDictionaryStoreTest {

  private static final String RESOURCE_NAME = "valueDictionaryResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * The gapless-run case. This resource stores no FSST table, so its NamePage holds offset 0 alone;
   * writing the dictionary at offset 2 has to root offset 1 as well or the commit throws.
   */
  @Test
  @DisplayName("a dictionary commits on a resource that never used FSST")
  void dictionaryCommitsWithoutAnFsstTable() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "alpha");
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals("alpha", GlobalValueDictionary.value(header, 1, rtx.getStorageEngineReader()));
      }
    }
  }

  /**
   * Enough values to span many record pages and many directory blocks. This is the test that fails
   * loudly if the key layout is one the indirect-page trie cannot address.
   */
  @Test
  @DisplayName("both directions round-trip across a commit at multi-page scale")
  void bothDirectionsRoundTripAcrossACommit() {
    final int count = 4000;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < count; i++) {
          assertEquals(i + 1, intern(writer, value(i)), "ids must be minted densely from 1 in first-seen order");
        }
        // Re-interning must return the same id rather than mint a second one.
        for (int i = 0; i < count; i += 97) {
          assertEquals(i + 1, intern(writer, value(i)));
        }
        assertEquals(count, writer.entryCount());
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        for (int i = 0; i < count; i++) {
          assertEquals(value(i), GlobalValueDictionary.value(header, i + 1, reader),
              "reverse lookup returned the wrong value for id " + (i + 1));
          assertEquals(i + 1, GlobalValueDictionary.probe(header, utf8(value(i)), reader),
              "forward probe returned the wrong id for " + value(i));
        }
      }
    }
  }

  @Test
  @DisplayName("a read view compares and transforms entry bytes without materializing rows")
  void readViewComparesAndTransformsEntryBytes() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
        intern(dictionary, "2013-07-14T20:38:47");
        // First-seen ids deliberately disagree with UTF-16 order. U+10400's high surrogate sorts
        // before U+FF01 even though unsigned UTF-8/scalar order puts U+10400 after it.
        intern(dictionary, "\uFF01");
        intern(dictionary, "\uD801\uDC00");
        intern(dictionary, "92233720368547758070");
        headerKey = flush(wtx, dictionary);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final GlobalValueDictionary.ReadView view =
            GlobalValueDictionary.readView(headerKey, rtx.getStorageEngineReader());
        assertNotNull(view);
        assertEquals(38L, view.xsIntegerOfSubstring(1, 15, 2));
        assertEquals(201307142038L + 1L, view.packIsoMinuteSubstring(1, 1, 16));
        assertEquals("2013-07-14T20:38", view.materializeIsoMinuteSubstring(1, 1, 16));
        assertTrue(view.compareIds(3, 2) < 0, "comparison must follow UTF-16, not first-seen or UTF-8 order");
        assertEquals(Integer.signum("\uD801\uDC00".compareTo("\uFF01")), Integer.signum(view.compareIds(3, 2)));
        assertEquals(Long.MIN_VALUE, view.xsIntegerOfSubstring(2, 1, 1), "a non-ASCII cast transform must fail closed");
        assertEquals(Long.MIN_VALUE, view.xsIntegerOfSubstring(4, 1, 20),
            "an out-of-range integer must decline instead of wrapping");
      }
    }
  }

  @Test
  @DisplayName("equal hashes spanning a large persistent bucket search the complete run")
  void collisionRunAcrossPersistentBucketIsComplete() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      final byte[] target = utf8("target-across-the-boundary");
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
        final int entries = 260;
        final int targetId = 130;
        for (int id = 1; id <= entries; id++) {
          final byte[] value = id == targetId
              ? target
              : utf8("fabricated-collision-" + id);
          dictionary.intern(value, 0, value.length);
        }
        headerKey = flush(wtx, dictionary);
        forceCollisionBucket(wtx, headerKey, target);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(130, GlobalValueDictionary.probe(headerKey, target, rtx.getStorageEngineReader()));
      }
    }
  }

  @Test
  @DisplayName("adversarial primary and secondary collisions round-trip through the bounded tree")
  void adversarialPrimaryAndSecondaryCollisionsRoundTrip() {
    final int entries = 512;
    final int targetId = 257;
    final byte[] target = utf8("adversarial-collision-" + targetId);
    final long primaryHash = GlobalValueDictionary.valueHash(target, 0, target.length);
    final long secondaryHash = LongHashFunction.xx3().hashBytes(target);
    final long headerKey;

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      seed(wtx);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (int id = 1; id <= entries; id++) {
        intern(dictionary, "adversarial-collision-" + id);
      }
      overrideAllWriterHashes(dictionary, entries, primaryHash, secondaryHash);
      headerKey = flush(wtx, dictionary);
      wtx.commit();
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, reader);
      assertNotNull(header);
      assertEquals(entries, header.getEntryCount());
      assertEquals(targetId, GlobalValueDictionary.probe(headerKey, target, reader));
      assertEquals("adversarial-collision-1", GlobalValueDictionary.value(headerKey, 1, reader));
      assertEquals("adversarial-collision-512", GlobalValueDictionary.value(headerKey, entries, reader));
    }
  }

  @Test
  @DisplayName("append segments preserve the anchor, ids, and historical mappings")
  void appendSegmentsPreserveStableIdentity() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter base = new GlobalValueDictionaryWriter();
        intern(base, "alpha");
        intern(base, "beta");
        headerKey = flush(wtx, base);
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ValueDictionaryHeaderNode baseHeader =
            GlobalValueDictionary.header(headerKey, wtx.getStorageEngineWriter());
        final GlobalValueDictionaryWriter additions = new GlobalValueDictionaryWriter();
        intern(additions, "gamma");
        intern(additions, "delta");
        final var writer = wtx.getStorageEngineWriter();
        assertEquals(headerKey, additions.flushAppend(baseHeader,
            writer.getNamePage(writer.getActualRevisionRootPage()), DatabaseType.JSON, writer, writer.getLog()));
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx revisionOne = session.beginNodeReadOnlyTrx(1)) {
        assertEquals("alpha", GlobalValueDictionary.value(headerKey, 1, revisionOne.getStorageEngineReader()));
        assertNull(GlobalValueDictionary.value(headerKey, 3, revisionOne.getStorageEngineReader()));
      }
      try (final JsonNodeReadOnlyTrx revisionTwo = session.beginNodeReadOnlyTrx(2)) {
        final var reader = revisionTwo.getStorageEngineReader();
        assertEquals("alpha", GlobalValueDictionary.value(headerKey, 1, reader));
        assertEquals("gamma", GlobalValueDictionary.value(headerKey, 3, reader));
        assertEquals(1, GlobalValueDictionary.probe(headerKey, utf8("alpha"), reader));
        assertEquals(3, GlobalValueDictionary.probe(headerKey, utf8("gamma"), reader));
      }
    }
  }

  @Test
  @DisplayName("collision runs remain complete across persistent generations")
  void collisionRunsAreSearchedAcrossGenerations() {
    final byte[] target = utf8("target-in-base");
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter base = new GlobalValueDictionaryWriter();
        base.intern(target, 0, target.length);
        headerKey = flush(wtx, base);
        wtx.commit();
      }
      for (int segmentIndex = 0; segmentIndex < 3; segmentIndex++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          final var writer = wtx.getStorageEngineWriter();
          final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
          final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, writer);
          final GlobalValueDictionaryWriter additions = new GlobalValueDictionaryWriter();
          for (int i = 0; i <= 128; i++) {
            intern(additions, "segment-" + segmentIndex + "-value-" + i);
          }
          additions.flushAppend(header, namePage, DatabaseType.JSON, writer, writer.getLog());
          forceCollisionBucket(wtx, headerKey, target);
          wtx.commit();
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      for (int revision = 1; revision <= session.getMostRecentRevisionNumber(); revision++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(1, GlobalValueDictionary.probe(headerKey, target, rtx.getStorageEngineReader()),
              "collision run failed at revision " + revision);
        }
      }
    }
  }

  @Test
  @DisplayName("cyclic secondary bucket chains fail closed within a bounded traversal")
  void cyclicSecondaryBucketChainFailsClosed() {
    final byte[] target = utf8("cycle-target");
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      seed(wtx);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      dictionary.intern(target, 0, target.length);
      intern(dictionary, "not-the-target");
      headerKey = flush(wtx, dictionary);
      forceSecondaryBucket(wtx, headerKey, target, 2, true);
      wtx.commit();
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertThrows(IllegalStateException.class,
          () -> GlobalValueDictionary.probe(headerKey, target, rtx.getStorageEngineReader()));
    }
  }

  @Test
  @DisplayName("forward bucket ids beyond the header cardinality fail closed")
  void forwardBucketIdBeyondHeaderFailsClosed() {
    final byte[] target = utf8("invalid-id-target");
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      seed(wtx);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      dictionary.intern(target, 0, target.length);
      headerKey = flush(wtx, dictionary);
      forceSecondaryBucket(wtx, headerKey, target, 2, false);
      wtx.commit();
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertThrows(IllegalStateException.class,
          () -> GlobalValueDictionary.probe(headerKey, target, rtx.getStorageEngineReader()));
    }
  }

  @Test
  @DisplayName("cyclic collision trees fail closed within a bounded traversal")
  void cyclicCollisionTreeFailsClosed() {
    final byte[] target = utf8("collision-cycle-target");
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      seed(wtx);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      dictionary.intern(target, 0, target.length);
      intern(dictionary, "collision-cycle-left");
      intern(dictionary, "collision-cycle-right");
      headerKey = flush(wtx, dictionary);
      forceCollisionTree(wtx, headerKey, target, 2, 3, true);
      wtx.commit();
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertThrows(IllegalStateException.class,
          () -> GlobalValueDictionary.probe(headerKey, target, rtx.getStorageEngineReader()));
    }
  }

  @Test
  @DisplayName("collision append reserves exactly the records reachable from the new roots")
  void collisionAppendConsumesItsExactKeyReservation() {
    final byte[] anchor = utf8("exact-reservation-anchor");
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
        dictionary.intern(anchor, 0, anchor.length);
        headerKey = flush(wtx, dictionary);
        forceCollisionTree(wtx, headerKey, anchor, 1, 0, false);
        wtx.commit();
      }

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var writer = wtx.getStorageEngineWriter();
        final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
        final int dictionaryOffset = NamePage.projectionValueDictionaryOffset(DatabaseType.JSON);
        final long maximumBefore = namePage.getMaxNodeKey(dictionaryOffset);
        final ValueDictionaryHeaderNode base = GlobalValueDictionary.header(headerKey, writer);
        assertNotNull(base);
        final GlobalValueDictionaryWriter additions = new GlobalValueDictionaryWriter();
        final byte[] added = utf8("exact-reservation-added");
        additions.intern(added, 0, added.length);
        overrideWriterHashes(additions, 1, GlobalValueDictionary.valueHash(anchor, 0, anchor.length),
            LongHashFunction.xx3().hashBytes(anchor));
        additions.flushAppend(base, namePage, DatabaseType.JSON, writer, writer.getLog());

        final long maximumAfter = namePage.getMaxNodeKey(dictionaryOffset);
        final ValueDictionaryHeaderNode updated = GlobalValueDictionary.header(headerKey, writer);
        assertNotNull(updated);
        final int newlyReachable = countReachableRecordsAfter(updated, maximumBefore, namePage, writer);
        assertEquals((long) newlyReachable * GlobalValueDictionary.PERSISTENT_RECORD_STRIDE,
            maximumAfter - maximumBefore, "reserved key groups must correspond one-for-one with reachable records");
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals("exact-reservation-added",
            GlobalValueDictionary.value(headerKey, 2, rtx.getStorageEngineReader()));
      }
    }
  }

  @Test
  @DisplayName("public reverse bytes cannot mutate the cached dictionary entry")
  void publicValueBytesAreOwnershipSafe() {
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
        intern(dictionary, "immutable-value");
        headerKey = flush(wtx, dictionary);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final byte[] exposed = GlobalValueDictionary.valueBytes(headerKey, 1, rtx.getStorageEngineReader());
        assertNotNull(exposed);
        exposed[0] ^= 0x7F;
        assertEquals("immutable-value", GlobalValueDictionary.value(headerKey, 1, rtx.getStorageEngineReader()));
        assertEquals(1, GlobalValueDictionary.probe(headerKey, utf8("immutable-value"), rtx.getStorageEngineReader()));
      }
    }
  }

  private static void forceCollisionBucket(final JsonNodeTrx wtx, final long headerKey, final byte[] target) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, writer);
    final long hash = GlobalValueDictionary.valueHash(target, 0, target.length);
    final int bucket = (int) (hash >>> 40) & 0xFF_FFFF;
    long key = header.getForwardRootKey();
    for (int depth = 0; depth < 3; depth++) {
      final ValueDictionaryRadixNode node =
          (ValueDictionaryRadixNode) namePage.getProjectionValueDictionaryRecord(key, DatabaseType.JSON, writer);
      key = node.childKey((bucket >>> (16 - depth * 8)) & 0xFF);
    }
    final int count = header.getEntryCount();
    final long secondary = LongHashFunction.xx3().hashBytes(target);
    final int bucketPages = (count + 127) / 128;
    long nextKey = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON,
        (bucketPages + 7L) * GlobalValueDictionary.PERSISTENT_RECORD_STRIDE);
    long nextBucketKey = 0L;
    for (int end = count; end > 0; end -= 128) {
      final int start = Math.max(0, end - 128);
      final long[] hashes = new long[end - start];
      final int[] ids = new int[end - start];
      Arrays.fill(hashes, hash);
      for (int i = start; i < end; i++)
        ids[i - start] = i + 1;
      final long bucketKey = nextKey;
      nextKey += GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;
      namePage.putProjectionValueDictionaryRecord(new ValueDictionaryHashBucketNode(bucketKey, bucket,
          (byte) Long.BYTES, secondary, nextBucketKey, hashes, ids), DatabaseType.JSON, writer, writer.getLog());
      nextBucketKey = bucketKey;
    }
    final int[] path = new int[Long.BYTES];
    for (int i = 0; i < path.length; i++) {
      path[i] = (int) (secondary >>> (56 - i * 8)) & 0xFF;
    }
    long childKey = nextBucketKey;
    for (int depth = 10; depth >= 4; depth--) {
      final long[] children = new long[ValueDictionaryRadixNode.FANOUT];
      children[path[depth - 3]] = childKey;
      final long radixKey = nextKey;
      nextKey += GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryRadixNode(radixKey, ValueDictionaryRadixNode.FORWARD, (byte) depth, children),
          DatabaseType.JSON, writer, writer.getLog());
      childKey = radixKey;
    }
    final long[] rootChildren = new long[ValueDictionaryRadixNode.FANOUT];
    rootChildren[path[0]] = childKey;
    namePage.putProjectionValueDictionaryRecord(
        new ValueDictionaryRadixNode(key, ValueDictionaryRadixNode.FORWARD, (byte) 3, rootChildren), DatabaseType.JSON,
        writer, writer.getLog());
  }

  private static void forceSecondaryBucket(final JsonNodeTrx wtx, final long headerKey, final byte[] target,
      final int wrongId, final boolean cyclic) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    final long primaryHash = GlobalValueDictionary.valueHash(target, 0, target.length);
    final long secondaryHash = LongHashFunction.xx3().hashBytes(target);
    final int primaryBucket = (int) (primaryHash >>> 40) & 0xFF_FFFF;
    final long first = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON,
        8L * GlobalValueDictionary.PERSISTENT_RECORD_STRIDE);
    final long bucketKey = first;
    namePage.putProjectionValueDictionaryRecord(new ValueDictionaryHashBucketNode(bucketKey, primaryBucket,
        (byte) Long.BYTES, secondaryHash, cyclic
            ? bucketKey
            : 0L,
        new long[] {primaryHash}, new int[] {wrongId}), DatabaseType.JSON, writer, writer.getLog());
    installSecondaryLeaf(wtx, headerKey, target, bucketKey, first + GlobalValueDictionary.PERSISTENT_RECORD_STRIDE);
  }

  private static void forceCollisionTree(final JsonNodeTrx wtx, final long headerKey, final byte[] target,
      final int rootId, final int secondId, final boolean cyclic) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    final int collisionRecords = cyclic
        ? 2
        : 1;
    final long first = namePage.reserveProjectionValueDictionaryKeys(DatabaseType.JSON,
        (collisionRecords + 7L) * GlobalValueDictionary.PERSISTENT_RECORD_STRIDE);
    final long rootKey = first;
    long next = first + GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;
    if (cyclic) {
      final long secondKey = next;
      next += GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryCollisionNode(rootKey, rootId, 2, secondKey, secondKey), DatabaseType.JSON, writer,
          writer.getLog());
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryCollisionNode(secondKey, secondId, 1, rootKey, rootKey), DatabaseType.JSON, writer,
          writer.getLog());
    } else {
      namePage.putProjectionValueDictionaryRecord(new ValueDictionaryCollisionNode(rootKey, rootId, 1, 0L, 0L),
          DatabaseType.JSON, writer, writer.getLog());
    }
    installSecondaryLeaf(wtx, headerKey, target, rootKey, next);
  }

  private static void installSecondaryLeaf(final JsonNodeTrx wtx, final long headerKey, final byte[] target,
      final long leafKey, long nextKey) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, writer);
    assertNotNull(header);
    final long primaryHash = GlobalValueDictionary.valueHash(target, 0, target.length);
    final int primaryBucket = (int) (primaryHash >>> 40) & 0xFF_FFFF;
    long primaryLeafKey = header.getForwardRootKey();
    for (int depth = 0; depth < 3; depth++) {
      final ValueDictionaryRadixNode node =
          (ValueDictionaryRadixNode) namePage.getProjectionValueDictionaryRecord(primaryLeafKey, DatabaseType.JSON,
              writer);
      primaryLeafKey = node.childKey((primaryBucket >>> (16 - depth * 8)) & 0xFF);
    }
    if (primaryLeafKey == 0L) {
      throw new AssertionError("target primary path is absent");
    }

    final long secondaryHash = LongHashFunction.xx3().hashBytes(target);
    final int[] path = new int[Long.BYTES];
    for (int index = 0; index < path.length; index++) {
      path[index] = (int) (secondaryHash >>> (56 - index * 8)) & 0xFF;
    }
    long childKey = leafKey;
    for (int depth = 10; depth >= 4; depth--) {
      final long[] children = new long[ValueDictionaryRadixNode.FANOUT];
      children[path[depth - 3]] = childKey;
      final long radixKey = nextKey;
      nextKey += GlobalValueDictionary.PERSISTENT_RECORD_STRIDE;
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryRadixNode(radixKey, ValueDictionaryRadixNode.FORWARD, (byte) depth, children),
          DatabaseType.JSON, writer, writer.getLog());
      childKey = radixKey;
    }
    final long[] rootChildren = new long[ValueDictionaryRadixNode.FANOUT];
    rootChildren[path[0]] = childKey;
    namePage.putProjectionValueDictionaryRecord(
        new ValueDictionaryRadixNode(primaryLeafKey, ValueDictionaryRadixNode.FORWARD, (byte) 3, rootChildren),
        DatabaseType.JSON, writer, writer.getLog());
  }

  private static void overrideWriterHashes(final GlobalValueDictionaryWriter writer, final int id,
      final long primaryHash, final long secondaryHash) {
    try {
      final var primaryField = GlobalValueDictionaryWriter.class.getDeclaredField("hashes");
      primaryField.setAccessible(true);
      ((long[]) primaryField.get(writer))[id] = primaryHash;
      final var secondaryField = GlobalValueDictionaryWriter.class.getDeclaredField("secondaryHashes");
      secondaryField.setAccessible(true);
      ((long[]) secondaryField.get(writer))[id] = secondaryHash;
    } catch (final ReflectiveOperationException exception) {
      throw new AssertionError("unable to install adversarial dictionary hashes", exception);
    }
  }

  private static void overrideAllWriterHashes(final GlobalValueDictionaryWriter writer, final int entryCount,
      final long primaryHash, final long secondaryHash) {
    try {
      final var primaryField = GlobalValueDictionaryWriter.class.getDeclaredField("hashes");
      primaryField.setAccessible(true);
      final long[] primaryHashes = (long[]) primaryField.get(writer);
      final var secondaryField = GlobalValueDictionaryWriter.class.getDeclaredField("secondaryHashes");
      secondaryField.setAccessible(true);
      final long[] secondaryHashes = (long[]) secondaryField.get(writer);
      Arrays.fill(primaryHashes, 1, entryCount + 1, primaryHash);
      Arrays.fill(secondaryHashes, 1, entryCount + 1, secondaryHash);
    } catch (final ReflectiveOperationException exception) {
      throw new AssertionError("unable to install adversarial dictionary hashes", exception);
    }
  }

  private static int countReachableRecordsAfter(final ValueDictionaryHeaderNode header, final long exclusiveFloor,
      final NamePage namePage, final StorageEngineReader reader) {
    final Set<Long> visited = new HashSet<>();
    collectReachableRecords(header.getForwardRootKey(), namePage, reader, visited);
    collectReachableRecords(header.getReverseRootKey(), namePage, reader, visited);
    int count = 0;
    for (final long key : visited) {
      if (key > exclusiveFloor) {
        count++;
      }
    }
    return count;
  }

  private static void collectReachableRecords(final long key, final NamePage namePage, final StorageEngineReader reader,
      final Set<Long> visited) {
    if (key == 0L || !visited.add(key)) {
      return;
    }
    final DataRecord record = namePage.getProjectionValueDictionaryRecord(key, DatabaseType.JSON, reader);
    if (record instanceof ValueDictionaryRadixNode radix) {
      for (final long childKey : radix.getSparseChildKeys()) {
        collectReachableRecords(childKey, namePage, reader, visited);
      }
    } else if (record instanceof ValueDictionaryHashBucketNode bucket) {
      collectReachableRecords(bucket.getNextBucketKey(), namePage, reader, visited);
    } else if (record instanceof ValueDictionaryCollisionNode collision) {
      collectReachableRecords(collision.getLeftKey(), namePage, reader, visited);
      collectReachableRecords(collision.getRightKey(), namePage, reader, visited);
    } else if (record instanceof ValueDictionaryValueBucketNode values) {
      // Both lanes of the sparse directory: packed sub-blocks and individually spilled entries.
      for (int i = 0; i < values.blockCount(); i++) {
        collectReachableRecords(values.blockKey(i), namePage, reader, visited);
      }
      for (int i = 0; i < values.spillCount(); i++) {
        collectReachableRecords(values.spillKeyAt(i), namePage, reader, visited);
      }
    } else if (!(record instanceof ValueDictionaryEntryNode) && !(record instanceof ValueDictionaryValueBlockNode)) {
      throw new AssertionError("unexpected dictionary record " + record);
    }
  }

  @ParameterizedTest(name = "append segments remain historical under {0}")
  @EnumSource(VersioningType.class)
  void appendSegmentsSurviveColdHistoricalReadsForEveryVersioningType(final VersioningType versioningType) {
    final String resourceName = "dictionary-" + versioningType.name().toLowerCase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(
          ResourceConfiguration.newBuilder(resourceName).versioningApproach(versioningType).build());
    }
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(resourceName)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter base = new GlobalValueDictionaryWriter();
        intern(base, "base");
        headerKey = flush(wtx, base);
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, wtx.getStorageEngineWriter());
        final GlobalValueDictionaryWriter additions = new GlobalValueDictionaryWriter();
        intern(additions, "added");
        final var writer = wtx.getStorageEngineWriter();
        additions.flushAppend(header, writer.getNamePage(writer.getActualRevisionRootPage()), DatabaseType.JSON, writer,
            writer.getLog());
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(resourceName);
        final JsonNodeReadOnlyTrx revisionOne = session.beginNodeReadOnlyTrx(1);
        final JsonNodeReadOnlyTrx revisionTwo = session.beginNodeReadOnlyTrx(2)) {
      assertEquals("base", GlobalValueDictionary.value(headerKey, 1, revisionOne.getStorageEngineReader()));
      assertNull(GlobalValueDictionary.value(headerKey, 2, revisionOne.getStorageEngineReader()));
      assertEquals("base", GlobalValueDictionary.value(headerKey, 1, revisionTwo.getStorageEngineReader()));
      assertEquals("added", GlobalValueDictionary.value(headerKey, 2, revisionTwo.getStorageEngineReader()));
      assertEquals(2, GlobalValueDictionary.probe(headerKey, utf8("added"), revisionTwo.getStorageEngineReader()));
      final GlobalValueDictionary.ReadView revisionOneView =
          GlobalValueDictionary.readView(headerKey, revisionOne.getStorageEngineReader());
      final GlobalValueDictionary.ReadView revisionTwoView =
          GlobalValueDictionary.readView(headerKey, revisionTwo.getStorageEngineReader());
      assertNotNull(revisionOneView);
      assertNotNull(revisionTwoView);
      assertEquals(1, revisionOneView.entryCount());
      assertEquals(2, revisionTwoView.entryCount());
      assertThrows(IllegalStateException.class, () -> revisionOneView.compareIds(1, 2),
          "a historical view must not resolve a later revision's id");
      assertTrue(revisionTwoView.compareIds(1, 2) > 0);
    }
  }

  @Test
  @DisplayName("a value the dictionary does not hold probes as absent, not as unknown")
  void absentValueProbesAsAbsent() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < 500; i++) {
          intern(writer, value(i));
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(GlobalValueDictionary.ID_ABSENT,
            GlobalValueDictionary.probe(header, utf8("nothing-like-this"), rtx.getStorageEngineReader()));
      }
    }
  }

  /** A dictionary nothing ever wrote must decline, never claim the value is absent. */
  @Test
  @DisplayName("an unwritten dictionary declines rather than reporting absence")
  void unwrittenDictionaryDeclines() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals(GlobalValueDictionary.ID_UNKNOWN, GlobalValueDictionary.probe(17L, utf8("alpha"), reader));
        assertNull(GlobalValueDictionary.value(17L, 1, reader));
        // "No dictionary at all" must also decline rather than throw.
        assertEquals(GlobalValueDictionary.ID_UNKNOWN, GlobalValueDictionary.probe(0L, utf8("alpha"), reader));
      }
    }
  }

  /**
   * The property the whole scheme rests on: a rebuild re-mints ids from 1, so id 1 means one thing in
   * revision 1 and another in revision 2. That is only safe because each revision reads the
   * dictionary it was built against.
   */
  @Test
  @DisplayName("a rebuild in a later revision leaves the earlier revision's mapping intact")
  void rebuildDoesNotDisturbEarlierRevisions() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerOne;
      final long headerTwo;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "first-revision-a");
        intern(writer, "first-revision-b");
        headerOne = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "second-revision-a");
        intern(writer, "second-revision-b");
        intern(writer, "second-revision-c");
        headerTwo = flush(wtx, writer);
        wtx.commit();
      }
      assertNotEquals(headerOne, headerTwo,
          "a rebuild reused the previous run's keys, so the two dictionaries share records");

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("first-revision-a", GlobalValueDictionary.value(headerOne, 1, reader),
            "revision 1's id 1 was rewritten by revision 2's rebuild — every row group committed in "
                + "revision 1 now means something else");
        assertEquals("first-revision-b", GlobalValueDictionary.value(headerOne, 2, reader));
        assertEquals(1, GlobalValueDictionary.probe(headerOne, utf8("first-revision-a"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT,
            GlobalValueDictionary.probe(headerOne, utf8("second-revision-a"), reader));
        assertNull(GlobalValueDictionary.value(headerTwo, 1, reader),
            "revision 1 can see a dictionary only revision 2 wrote, so the store is not going "
                + "through the versioned trie");
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("second-revision-a", GlobalValueDictionary.value(headerTwo, 1, reader));
        assertEquals("second-revision-c", GlobalValueDictionary.value(headerTwo, 3, reader));
        assertEquals(3, GlobalValueDictionary.probe(headerTwo, utf8("second-revision-c"), reader));
        // The earlier revision's dictionary is still readable from the later revision, unchanged.
        assertEquals("first-revision-a", GlobalValueDictionary.value(headerOne, 1, reader));
      }
    }
  }

  /** Two dictionaries written in one transaction must not see each other's values. */
  @Test
  @DisplayName("dictionaries are isolated inside the one sub-trie")
  void dictionariesAreIsolated() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerA;
      final long headerB;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter a = new GlobalValueDictionaryWriter();
        intern(a, "shared-value");
        intern(a, "only-in-a");
        headerA = flush(wtx, a);
        final GlobalValueDictionaryWriter b = new GlobalValueDictionaryWriter();
        intern(b, "only-in-b");
        intern(b, "shared-value");
        headerB = flush(wtx, b);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        // The same string holds a different id in each dictionary, and each answers only for itself.
        assertEquals(1, GlobalValueDictionary.probe(headerA, utf8("shared-value"), reader));
        assertEquals(2, GlobalValueDictionary.probe(headerB, utf8("shared-value"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(headerA, utf8("only-in-b"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(headerB, utf8("only-in-a"), reader));
        assertEquals("only-in-a", GlobalValueDictionary.value(headerA, 2, reader));
        assertEquals("only-in-b", GlobalValueDictionary.value(headerB, 1, reader));
      }
    }
  }

  /** The batch reverse lookup must agree with the single-id one, order preserved. */
  @Test
  @DisplayName("batch reverse lookup preserves the caller's order")
  void batchReverseLookupPreservesOrder() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < 1000; i++) {
          intern(writer, value(i));
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final int[] ids = {900, 3, 512, 1, 777, 0};
        final String[] values = GlobalValueDictionary.values(header, ids, reader);
        assertEquals(ids.length, values.length);
        for (int i = 0; i < ids.length; i++) {
          if (ids[i] == 0) {
            assertNull(values[i], "id 0 means 'no id' and must not resolve");
          } else {
            assertEquals(value(ids[i] - 1), values[i], "batch lookup disagreed at slot " + i);
          }
        }
      }
    }
  }

  /**
   * Long values must not overflow a record page's slotted buffer at the layout's density — the
   * ceiling is a hard failure at commit, not a slow path, so the layout has to leave headroom.
   */
  @Test
  @DisplayName("long values survive the record-page layout")
  void longValuesSurviveTheLayout() {
    final int count = 900;
    final String padding = "x".repeat(400);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < count; i++) {
          intern(writer, padding + "-" + i);
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        for (int i = 0; i < count; i += 41) {
          assertEquals(padding + "-" + i, GlobalValueDictionary.value(header, i + 1, reader));
        }
      }
    }
  }

  /** Values that differ only past a shared prefix, plus the empty string, which is a real value. */
  @Test
  @DisplayName("the empty string and shared-prefix values are distinct entries")
  void emptyAndSharedPrefixValues() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        assertEquals(1, intern(writer, ""));
        assertEquals(2, intern(writer, "prefix"));
        assertEquals(3, intern(writer, "prefixx"));
        assertEquals(1, intern(writer, ""), "the empty string must intern to one id, not a fresh one each time");
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("", GlobalValueDictionary.value(header, 1, reader));
        assertEquals(1, GlobalValueDictionary.probe(header, utf8(""), reader));
        assertEquals(2, GlobalValueDictionary.probe(header, utf8("prefix"), reader));
        assertEquals(3, GlobalValueDictionary.probe(header, utf8("prefixx"), reader));
      }
    }
  }

  /** An empty dictionary is legitimate; it must round-trip and decline every probe. */
  @Test
  @DisplayName("an empty dictionary round-trips and answers nothing")
  void emptyDictionaryRoundTrips() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        header = flush(wtx, new GlobalValueDictionaryWriter());
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertNull(GlobalValueDictionary.value(header, 1, reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(header, utf8("x"), reader));
      }
    }
  }

  /** Dictionary records must fill leaf slots densely and cover the worst-case radix shape. */
  @Test
  @DisplayName("the key layout reserves one smallest persistent unit per record")
  void keyLayoutIsDense() {
    final int entries = 1000;
    final long reserved = GlobalValueDictionary.maximumKeysToReserve(entries);
    assertTrue(reserved > (long) entries * GlobalValueDictionary.PERSISTENT_RECORD_STRIDE,
        "the reservation is short of the radix and bucket records used");
    assertEquals(1, GlobalValueDictionary.PERSISTENT_RECORD_STRIDE,
        "a dictionary record must consume exactly one key slot");
    assertEquals(io.sirix.settings.Constants.INP_REFERENCE_COUNT, GlobalValueDictionary.PERSISTENT_RECORDS_PER_PAGE,
        "dense dictionary records must use every record-page slot");
  }

  private static void seed(final JsonNodeTrx wtx) {
    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
  }

  private static int intern(final GlobalValueDictionaryWriter writer, final String value) {
    final byte[] bytes = utf8(value);
    return writer.intern(bytes, 0, bytes.length);
  }

  private static long flush(final JsonNodeTrx wtx, final GlobalValueDictionaryWriter writer) {
    final var storageEngineWriter = wtx.getStorageEngineWriter();
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    return writer.flush(namePage, DatabaseType.JSON, storageEngineWriter, storageEngineWriter.getLog());
  }

  /** A did-shaped value, spread over the hash space rather than sequential. */
  private static String value(final int i) {
    return "did:plc:" + Integer.toHexString(i * 31 + 7) + "abcdefghij";
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
