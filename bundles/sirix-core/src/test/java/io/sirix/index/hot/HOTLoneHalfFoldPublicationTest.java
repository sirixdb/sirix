/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A key folded into the lone indirect child of a 1:31 full-node split has to be published.
 *
 * <p>
 * A full node whose most significant bit tells one child from the other 31 splits into that child —
 * handed back bare, as the node's <em>own</em> child reference — and one compressed half. When a new
 * key forms a new combination of the node's existing bits on the lone child's side, the writer folds
 * the key's leaf into that child. The folded page is fresh, but the reference it would replace is
 * not: it already names the unfolded child in the transaction log. Swizzling the folded page onto
 * that reference publishes nothing, because registration stops at a reference that carries an
 * identity. A reader following the swizzle sees the key; the writer, which resolves the log first,
 * and the commit both keep the unfolded child. The key's posting is then lost with every invariant
 * intact: the committed trie is well-formed, it simply never contained the key. In a valid-time
 * index that is a record answered over part of its span only, by the store that kept its other
 * endpoint.
 * </p>
 *
 * <p>
 * The shape is built through the public writer from ordinary puts. 512 keys of fork {@code 0} fill
 * the root leaf; one key of a fork differing in a high bit splits it, and one more fork-{@code 0}
 * key splits the full leaf again. 29 keys of the other fork, one endpoint bit each, fill the
 * height-1 root, and a fresh posting chunk of a resident key overflows a full leaf under it: the
 * capacity cascade leaves a height-2 root over the fork-{@code 0} node and the other fork's. Of 32
 * more keys of the other fork, two fill its node and thirty become leaves of the root beside it. The
 * root is then full, and its most significant bit — the fork bit — has the fork-{@code 0} node alone
 * on its side. The next fork-{@code 0} key whose endpoint sets a bit the root's mask already carries
 * for the other fork is the fold.
 * </p>
 *
 * <p>
 * Nothing about the loss depends on how a leaf is versioned — the folded page never reaches the log
 * — so each scenario runs under every {@link VersioningType}, and the multi-revision one checks each
 * revision against exactly what it held, no more and no less. Every scenario asserts through
 * {@link AbstractHOTIndexWriter#FULL_EXISTING_BIT_LONE_HALF_FOLD} that the fold was really reached,
 * so a change in leaf geometry cannot let it pass without exercising anything.
 * </p>
 */
final class HOTLoneHalfFoldPublicationTest {

  private static final String RESOURCE = "lone-half-fold-publication";
  private static final int INDEX_NUMBER = 0;

  /** The node every put of the shape phase indexes. */
  private static final long SHAPE_NODE_KEY = 7L;

  /** The fork that differs from fork {@code 0} in one high bit: the root's most significant bit. */
  private static final long OTHER_FORK = 1L << 60;

  /** Entries that fill one leaf; the next one splits it. */
  private static final int LEAF_CAPACITY = 512;

  /** An endpoint bit the root's mask carries for the other fork only, above the fork-0 node's own. */
  private static final int FOLD_ENDPOINT_BIT = 41;

  /** Revisions the multi-revision scenario commits: enough to wrap every strategy's window twice. */
  private static final int REVISIONS = 7;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}: a key folded into a lone indirect half survives the commit")
  @EnumSource(VersioningType.class)
  void foldedKeySurvivesTheCommit(final VersioningType versioningType) {
    final Path databasePath = createDatabase(versioningType);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final Load load = new Load();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        load.attach(wtx);
        load.buildFullMixedRoot();
        load.putFoldedKey();
        wtx.commit();
      }
      load.snapshot();
      verifyRevision(session, 1, load.snapshots.get(0));
    }
  }

  @ParameterizedTest(name = "{0}: a key folded into a lone indirect half survives an insert beside it")
  @EnumSource(VersioningType.class)
  void foldedKeySurvivesAnInsertBesideIt(final VersioningType versioningType) {
    final Path databasePath = createDatabase(versioningType);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final Load load = new Load();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        load.attach(wtx);
        load.buildFullMixedRoot();
        final ValidTimeKey folded = load.putFoldedKey();

        // The next key of the same fork re-resolves the folded child through the transaction log and
        // rewrites it; whatever the log does not hold for that child is gone from here on.
        load.put(0L, 1L << (FOLD_ENDPOINT_BIT + 1), SHAPE_NODE_KEY);
        final NodeReferences postings = load.writer.get(folded, SearchMode.EQUAL);
        assertNotNull(postings, "the folded key must still be readable after an insert beside it");
        assertTrue(postings.isPresent(SHAPE_NODE_KEY), "the folded key must still hold its posting");
        wtx.commit();
      }
      load.snapshot();
      verifyRevision(session, 1, load.snapshots.get(0));
    }
  }

  @ParameterizedTest(name = "{0}: a lone indirect half with no room declines the fold and the key is still placed")
  @EnumSource(VersioningType.class)
  void fullLoneHalfDeclinesTheFoldAndTheKeyIsStillPlaced(final VersioningType versioningType) {
    final Path databasePath = createDatabase(versioningType);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final Load load = new Load();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        load.attach(wtx);
        load.buildFullMixedRoot();
        load.fillLoneChild();

        // A compressed half always has room for the key; the node's own child was sized by its own
        // inserts. Folding into it regardless would build a node of MAX_NODE_ENTRIES + 1 children,
        // which the page refuses — and with it the put, and the load.
        final long declinedBefore = AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FULL.get();
        final long foldsBefore = AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FOLD.get();
        load.put(0L, 1L << FOLD_ENDPOINT_BIT, SHAPE_NODE_KEY);
        assertEquals(declinedBefore + 1, AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FULL.get(),
            "the put must meet a full lone indirect half; without that the shape has drifted and this scenario"
                + " exercises nothing");
        assertEquals(foldsBefore, AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FOLD.get(),
            "a half with no room must not be folded into");
        load.put(0L, 1L << (FOLD_ENDPOINT_BIT + 1), SHAPE_NODE_KEY);
        wtx.commit();
      }
      load.snapshot();
      verifyRevision(session, 1, load.snapshots.get(0));
    }
  }

  @ParameterizedTest(name = "{0}: every revision holds exactly what it was given around the fold")
  @EnumSource(VersioningType.class)
  void everyRevisionStaysExactAroundTheFold(final VersioningType versioningType) {
    final Path databasePath = createDatabase(versioningType);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final Load load = new Load();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        load.attach(wtx);
        load.buildFullMixedRoot();
        wtx.commit();
      }
      load.snapshot();

      // The fold happens on a committed tree, as it did in the load that lost a posting: the lone
      // child is then a page of an earlier revision the descent has copied into this one's log.
      final ValidTimeKey folded;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        load.attach(wtx);
        folded = load.putFoldedKey();
        load.put(0L, 1L << (FOLD_ENDPOINT_BIT + 1), SHAPE_NODE_KEY);
        wtx.commit();
      }
      load.snapshot();

      // Keep writing through the folded child: its postings grow and shrink, and new keys join it.
      for (int revision = 3; revision <= REVISIONS; revision++) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          load.attach(wtx);
          load.put(folded.forkNode(), folded.endpoint(), SHAPE_NODE_KEY + revision);
          load.put(0L, 1L << (FOLD_ENDPOINT_BIT + revision), SHAPE_NODE_KEY);
          load.put(0L, LEAF_CAPACITY + revision, SHAPE_NODE_KEY);
          if (revision == 5) {
            load.remove(folded.forkNode(), folded.endpoint(), SHAPE_NODE_KEY);
          }
          wtx.commit();
        }
        load.snapshot();
      }

      assertEquals(REVISIONS, load.snapshots.size());
      for (int revision = 1; revision <= REVISIONS; revision++) {
        verifyRevision(session, revision, load.snapshots.get(revision - 1));
      }
      assertTrue(load.snapshots.get(0).get(folded) == null, "revision 1 predates the folded key");
      assertArrayEquals(new long[] {SHAPE_NODE_KEY + 3, SHAPE_NODE_KEY + 4, SHAPE_NODE_KEY + 5, SHAPE_NODE_KEY + 6,
          SHAPE_NODE_KEY + 7}, toArray(load.snapshots.get(REVISIONS - 1).get(folded)),
          "the last revision holds the folded key's grown posting without the removed node");
    }
  }

  // ===== Load =====

  /**
   * The put stream of one scenario, recording what every key must hold at every commit so each
   * revision can be checked entry by entry.
   */
  private static final class Load {
    private final Map<ValidTimeKey, TreeSet<Long>> expected = new LinkedHashMap<>();
    private final List<Map<ValidTimeKey, TreeSet<Long>>> snapshots = new ArrayList<>(REVISIONS);
    private HOTIndexWriter<ValidTimeKey> writer;

    /** A writer lives as long as its transaction; every revision gets its own. */
    private void attach(final JsonNodeTrx wtx) {
      writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(), ValidTimeKeySerializer.INSTANCE,
          IndexType.VALIDTIME, INDEX_NUMBER);
    }

    private void put(final long forkNode, final long endpoint, final long nodeKey) {
      final ValidTimeKey key = new ValidTimeKey(ValidTimeKey.STORE_LOWER, forkNode, endpoint);
      writer.indexNodeKey(key, nodeKey);
      expected.computeIfAbsent(key, ignored -> new TreeSet<>()).add(nodeKey);
    }

    private void remove(final long forkNode, final long endpoint, final long nodeKey) {
      final ValidTimeKey key = new ValidTimeKey(ValidTimeKey.STORE_LOWER, forkNode, endpoint);
      assertTrue(writer.remove(key, nodeKey), key + " must have held node " + nodeKey);
      final TreeSet<Long> postings = expected.get(key);
      postings.remove(nodeKey);
      if (postings.isEmpty()) {
        expected.remove(key);
      }
    }

    /** Record what the revision being committed holds. */
    private void snapshot() {
      final Map<ValidTimeKey, TreeSet<Long>> copy = new LinkedHashMap<>(expected.size() * 2);
      for (final Map.Entry<ValidTimeKey, TreeSet<Long>> entry : expected.entrySet()) {
        copy.put(entry.getKey(), new TreeSet<>(entry.getValue()));
      }
      snapshots.add(copy);
    }

    /**
     * A full height-2 root whose most significant bit has one indirect child alone on its side: the
     * node of the fork-{@code 0} leaves, beside 31 children of the other fork.
     */
    private void buildFullMixedRoot() {
      for (int endpoint = 0; endpoint < LEAF_CAPACITY; endpoint++) {
        put(0L, endpoint, SHAPE_NODE_KEY);
      }
      put(OTHER_FORK, 0L, SHAPE_NODE_KEY); // splits the root leaf at the fork bit
      put(0L, LEAF_CAPACITY, SHAPE_NODE_KEY); // splits the full fork-0 leaf
      for (int bit = 10; bit < 39; bit++) {
        put(OTHER_FORK, 1L << bit, SHAPE_NODE_KEY); // one leaf each: the height-1 root is full
      }
      // A fresh posting chunk is a fresh key of the full fork-0 leaf: its overflow splits the full
      // root, and the fork-0 leaves get a node of their own under a height-2 root.
      put(0L, 0L, (1L << 16) + SHAPE_NODE_KEY);
      for (int bit = 39; bit < 63; bit++) {
        put(OTHER_FORK, 1L << bit, SHAPE_NODE_KEY);
      }
      for (int bit = 0; bit < 8; bit++) {
        put(OTHER_FORK | (1L << bit), 0L, SHAPE_NODE_KEY);
      }
    }

    /**
     * Fill the lone fork-{@code 0} child without touching the full root above it: each key sets one
     * endpoint bit more significant than every bit that child discriminates on and absent from the
     * root's mask, so it joins the child as a new partition root — 29 of them fill its 3 children up
     * to {@code MAX_NODE_ENTRIES}.
     */
    private void fillLoneChild() {
      for (int bit = 10; bit < 39; bit++) {
        put(0L, 1L << bit, SHAPE_NODE_KEY);
      }
    }

    /**
     * The put that folds: a fork-{@code 0} key on an endpoint bit the full root already discriminates
     * on. Asserts the fold into the lone indirect half was reached, and by this put alone.
     */
    private ValidTimeKey putFoldedKey() {
      final long foldsBefore = AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FOLD.get();
      put(0L, 1L << FOLD_ENDPOINT_BIT, SHAPE_NODE_KEY);
      assertEquals(foldsBefore + 1, AbstractHOTIndexWriter.FULL_EXISTING_BIT_LONE_HALF_FOLD.get(),
          "the put must fold into the lone indirect half of a 1:31 split; without that the shape has drifted"
              + " and this scenario exercises nothing");
      return new ValidTimeKey(ValidTimeKey.STORE_LOWER, 0L, 1L << FOLD_ENDPOINT_BIT);
    }
  }

  // ===== Verification =====

  /** The revision's trie is well-formed and holds exactly {@code expected} — every key, every node. */
  private static void verifyRevision(final JsonResourceSession session, final int revision,
      final Map<ValidTimeKey, TreeSet<Long>> expected) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER).assertOk();
      final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
          ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
      for (final Map.Entry<ValidTimeKey, TreeSet<Long>> entry : expected.entrySet()) {
        final NodeReferences postings = reader.get(entry.getKey(), SearchMode.EQUAL);
        assertNotNull(postings, entry.getKey() + " must be readable in revision " + revision);
        assertArrayEquals(toArray(entry.getValue()), postings.toSortedArray(),
            "postings of " + entry.getKey() + " in revision " + revision);
      }
      // Exactly, not at least: a scan yields the expected entries in key order and nothing else. A
      // point lookup and a scan reach a leaf by different routes, and a stab query is a scan.
      final Iterator<Map.Entry<ValidTimeKey, NodeReferences>> stored = reader.iterator();
      for (final Map.Entry<ValidTimeKey, TreeSet<Long>> entry : new TreeMap<>(expected).entrySet()) {
        assertTrue(stored.hasNext(), "the scan of revision " + revision + " ends before " + entry.getKey());
        final Map.Entry<ValidTimeKey, NodeReferences> next = stored.next();
        assertEquals(entry.getKey(), next.getKey(), "scan order in revision " + revision);
        assertArrayEquals(toArray(entry.getValue()), next.getValue().toSortedArray(),
            "scanned postings of " + entry.getKey() + " in revision " + revision);
      }
      assertFalse(stored.hasNext(), "the scan of revision " + revision + " yields an entry it was never given");
    }
  }

  private static long[] toArray(final TreeSet<Long> postings) {
    final long[] nodeKeys = new long[postings.size()];
    int index = 0;
    for (final long nodeKey : postings) {
      nodeKeys[index++] = nodeKey;
    }
    return nodeKeys;
  }

  private Path createDatabase(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve("db-" + versioningType);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioningType).build()));
    }
    return databasePath;
  }
}
