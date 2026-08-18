/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageKind;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invariant I2 for the chunk-framed body: combining fragments produces the same page whatever the
 * fragments' bodies were framed as, under every versioning scheme.
 *
 * <p>
 * <b>How the claim is tested.</b> Two resources are built side by side from an identical, scripted
 * sequence of mutations — one written with chunk-framed bodies, one with monolith bodies — and then
 * every revision of both is walked node by node and compared. Because the scripts are identical,
 * the two resources agree on node keys, so the comparison can be exact rather than structural:
 * kind, parent, siblings, first child, name, value, child and descendant counts, at every node of
 * every revision. Nothing here inspects the format; the point is that nothing can tell which format
 * it is reading.
 *
 * <p>
 * <b>Why the mutations are shaped the way they are.</b> Revisions 2..N touch only the first page's
 * worth of records, so that page accumulates a fragment chain as long as the revision count while
 * the pages behind it keep none. That split is what makes the second half of this test possible: a
 * point lookup on the chained page must decline laziness (a combine reads every slot of every
 * fragment, so a lazy fragment would expand all of itself and pay the framing for it — plan
 * amendment A6), and a point lookup on an untouched page must still take it. Both are asserted per
 * page, from the page's own fragment count, not inferred from an aggregate counter.
 *
 * <p>
 * <b>The third assertion is the suite being green.</b> Every combine entry in
 * {@code VersioningType} asserts that no fragment reaching it is still holding chunks; those guards
 * throw. Deletes are in the script for the same reason — a tombstone is a record like any other, so
 * a chunk holding nothing but tombstones has to materialize like any other, and combine's shadowing
 * decisions have to keep reading it correctly.
 */
final class ChunkedBodyVersionedSweepTest {

  /**
   * Values seeded in revision 1. Comfortably past a page's 1024 slots, so the mutations in later
   * revisions can be confined to the first page and leave the pages behind it untouched.
   */
  private static final int SEED_VALUES = 2600;

  /** Revisions, counting the seed. Three or more is what makes a chain rather than a pair. */
  private static final int REVISIONS = 5;

  /** Records mutated per revision, all drawn from the front of the document — i.e. from one page. */
  private static final int MUTATIONS_PER_REVISION = 24;

  private boolean previouslyEnabled;
  private boolean previousDiag;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    previouslyEnabled = ChunkedBodyConfig.enabled();
    previousDiag = ChunkedBodyConfig.setDiagForTesting(true);
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setDiagForTesting(previousDiag);
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest(name = "[{0}] chunked and monolith resources agree at every revision")
  @EnumSource(VersioningType.class)
  void chunkedFragmentsCombineLikeMonolithOnes(final VersioningType versioning) {
    final String chunkedResource = "versionedSweep_" + versioning.name() + "_chunked";
    final String monolithResource = "versionedSweep_" + versioning.name() + "_monolith";
    PageKind.resetChunkedBodyStats();

    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(resourceConfig(chunkedResource, versioning));
      database.createResource(resourceConfig(monolithResource, versioning));

      try (final JsonResourceSession chunked = database.beginResourceSession(chunkedResource);
          final JsonResourceSession monolith = database.beginResourceSession(monolithResource)) {
        // The two resources run the same script, so their node keys line up and the comparison can
        // be by key rather than by position. The keys are read back from the first resource once
        // revision 1 exists and then drive the mutations on BOTH.
        final List<Long> valueKeys = new ArrayList<>(SEED_VALUES);
        seed(chunked, true);
        seed(monolith, false);
        collectValueKeys(chunked, valueKeys);
        assertEquals(SEED_VALUES, valueKeys.size(), "the seed did not produce the values the script expects");

        final Set<Long> removed = new LinkedHashSet<>();
        for (int revision = 2; revision <= REVISIONS; revision++) {
          // Applied to both in the same order, with only the body format differing. The removed
          // set is shared so both resources delete exactly the same keys.
          final Set<Long> removedThisRevision = mutate(chunked, true, revision, valueKeys, removed);
          mutate(monolith, false, revision, valueKeys, removed);
          removed.addAll(removedThisRevision);
        }

        assertEquals(REVISIONS, chunked.getMostRecentRevisionNumber(), "chunked resource: wrong revision count");
        assertEquals(REVISIONS, monolith.getMostRecentRevisionNumber(), "monolith resource: wrong revision count");
        assertTrue(PageKind.chunkedBodiesWritten() > 0,
            "no page was written with a chunked body — the writer never saw the flag, so this test compared"
                + " two monolith resources with each other");

        long nodesCompared = 0;
        for (int revision = 1; revision <= REVISIONS; revision++) {
          nodesCompared += assertRevisionsIdentical(chunked, monolith, revision, versioning);
        }

        final FragmentCensus census = probeLoadPolicy(chunked, versioning);
        System.out.println("[chunked-i2] " + versioning + ": " + REVISIONS + " revisions, " + nodesCompared
            + " nodes compared against the monolith twin; pages probed by point lookup — chained(eager)="
            + census.chained + " unchained(lazy)=" + census.unchained + " fragmentsPerPage=" + census.fragmentCounts);
        assertChainsMatchTheScheme(versioning, census);
      }
    }
  }

  /**
   * What each scheme is supposed to have built, and what the load policy must therefore have done.
   *
   * <p>
   * The chain lengths are not incidental — they are the schemes' definitions, and asserting them is
   * what keeps this test honest about whether it exercised a combine at all. FULL rewrites the whole
   * page every commit, so there is never anything to combine and every page must load lazily.
   * DIFFERENTIAL keeps exactly one delta against the last full dump, so its chains are one fragment
   * long however many revisions there are — a longer chain would mean the scheme had changed under
   * the test. INCREMENTAL and SLIDING_SNAPSHOT accumulate a delta per revision, so five revisions of
   * mutations to one page must leave several.
   */
  private static void assertChainsMatchTheScheme(final VersioningType versioning, final FragmentCensus census) {
    final int longestChain = census.fragmentCounts().lastKey();
    switch (versioning) {
      case FULL -> {
        assertEquals(0, census.chained(), "FULL stores complete pages, so no page can carry a chain");
        assertTrue(census.unchained() >= 3, "FULL: only " + census.unchained()
            + " pages were loaded lazily, so the point-lookup policy" + " barely fired");
      }
      case DIFFERENTIAL -> {
        assertEquals(1, longestChain, "DIFFERENTIAL keeps one delta over the last full dump; a chain of " + longestChain
            + " means the scheme, not this test, changed");
        assertTrue(census.chained() > 0,
            "DIFFERENTIAL: no probed page carried its delta, so A6's decline" + " went untested");
      }
      case INCREMENTAL, SLIDING_SNAPSHOT -> {
        assertTrue(longestChain >= 2, versioning + ": the longest chain reached was " + longestChain
            + " fragments; five revisions of mutations to one page were supposed to leave more than one");
        assertTrue(census.chained() > 0,
            versioning + ": no probed page carried a fragment chain, so A6's" + " decline went untested");
      }
    }
  }

  private static ResourceConfiguration resourceConfig(final String name, final VersioningType versioning) {
    return ResourceConfiguration.newBuilder(name)
                                .versioningApproach(versioning)
                                // Enough that a chain is kept rather than collapsed back to a full
                                // page on every commit, which would leave nothing to combine.
                                .maxNumberOfRevisionsToRestore(REVISIONS + 1)
                                .build();
  }

  /** Revision 1: one array of string values, wide enough to span several record pages. */
  private static void seed(final JsonResourceSession session, final boolean chunkedBody) {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunkedBody);
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild(value(1, 0));
      for (int i = 1; i < SEED_VALUES; i++) {
        wtx.insertStringValueAsRightSibling(value(1, i));
      }
      wtx.commit();
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }

  /**
   * One revision's mutations: updates, deletes and inserts, all against records at the front of the
   * document so the chain grows on one page and the pages behind it stay whole.
   *
   * @param removed keys deleted by earlier revisions, which this one must not touch again
   * @return the keys this revision deleted
   */
  private static Set<Long> mutate(final JsonResourceSession session, final boolean chunkedBody, final int revision,
      final List<Long> valueKeys, final Set<Long> removed) {
    final Set<Long> removedNow = new LinkedHashSet<>();
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunkedBody);
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      int mutated = 0;
      for (int i = 0; mutated < MUTATIONS_PER_REVISION && i < valueKeys.size(); i++) {
        // A stride rather than a run, so the mutated slots are scattered through the page's
        // directory and land in different chunks of it.
        final int index = (i * 7 + revision) % 512;
        final long key = valueKeys.get(index);
        if (removed.contains(key) || removedNow.contains(key)) {
          continue;
        }
        if (!wtx.moveTo(key)) {
          continue;
        }
        mutated++;
        if (index % 8 == 3) {
          // Delete: leaves a tombstone the combine has to keep shadowing in later revisions.
          wtx.remove();
          removedNow.add(key);
        } else if (index % 8 == 5) {
          // Insert: a brand new record, on the same page as its anchor.
          wtx.insertStringValueAsRightSibling(value(revision, index));
        } else {
          // Update: rewrites the slot in place, growing or shrinking it.
          wtx.setStringValue(value(revision, index));
        }
      }
      wtx.commit();
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
    return removedNow;
  }

  /** Node keys of every string value in revision 1, in document order. */
  private static void collectValueKeys(final JsonResourceSession session, final List<Long> out) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      if (rtx.moveToFirstChild()) {
        do {
          if (rtx.getKind() == NodeKind.STRING_VALUE) {
            out.add(rtx.getNodeKey());
          }
        } while (rtx.moveToRightSibling());
      }
    }
  }

  /**
   * Walk one revision of both resources in lockstep and compare every node.
   *
   * @return the nodes compared
   */
  private static long assertRevisionsIdentical(final JsonResourceSession chunked, final JsonResourceSession monolith,
      final int revision, final VersioningType versioning) {
    final String what = versioning + " revision " + revision;
    long compared = 0;
    try (final JsonNodeReadOnlyTrx chunkedRtx = chunked.beginNodeReadOnlyTrx(revision);
        final JsonNodeReadOnlyTrx monolithRtx = monolith.beginNodeReadOnlyTrx(revision)) {
      chunkedRtx.moveToDocumentRoot();
      monolithRtx.moveToDocumentRoot();
      final var chunkedAxis = new DescendantAxis(chunkedRtx, IncludeSelf.YES);
      final var monolithAxis = new DescendantAxis(monolithRtx, IncludeSelf.YES);
      while (chunkedAxis.hasNext()) {
        assertTrue(monolithAxis.hasNext(), what + ": the chunked resource has more nodes than the monolith twin");
        chunkedAxis.nextLong();
        monolithAxis.nextLong();
        assertEquals(describe(monolithRtx), describe(chunkedRtx), what + ": node mismatch");
        compared++;
      }
      assertFalse(monolithAxis.hasNext(), what + ": the monolith twin has more nodes than the chunked resource");
    }
    assertTrue(compared > SEED_VALUES, what + ": walked only " + compared + " nodes");
    return compared;
  }

  /**
   * Everything the cursor can say about the node it sits on.
   *
   * <p>
   * Compared as one string rather than field by field so a mismatch reports the whole node, which is
   * what tells you whether a record was shifted, truncated or reinjected wrongly.
   */
  private static String describe(final JsonNodeReadOnlyTrx rtx) {
    final StringBuilder out = new StringBuilder(128);
    out.append("key=")
       .append(rtx.getNodeKey())
       .append(" kind=")
       .append(rtx.getKind())
       .append(" parent=")
       .append(rtx.getParentKey())
       .append(" firstChild=")
       .append(rtx.getFirstChildKey())
       .append(" leftSibling=")
       .append(rtx.getLeftSiblingKey())
       .append(" rightSibling=")
       .append(rtx.getRightSiblingKey())
       .append(" children=")
       .append(rtx.getChildCount())
       .append(" descendants=")
       .append(rtx.getDescendantCount());
    if (rtx.isObjectKey()) {
      out.append(" name=").append(rtx.getName());
    } else if (rtx.isStringValue()) {
      out.append(" value=").append(rtx.getValue());
    } else if (rtx.isNumberValue()) {
      out.append(" number=").append(rtx.getNumberValue());
    } else if (rtx.isBooleanValue()) {
      out.append(" boolean=").append(rtx.getBooleanValue());
    }
    return out.toString();
  }

  /** What the point-lookup probe found: how many pages were chained, and how many went lazy. */
  private record FragmentCensus(int chained, int unchained, TreeMap<Integer, Integer> fragmentCounts) {
  }

  /**
   * Point-look up one record on each record page of the newest revision and check what the load
   * policy did with it.
   *
   * <p>
   * The move comes first and the page inspection second, on purpose: {@code moveTo} is the
   * point-lookup entry point that requests laziness, while asking the reader for the page directly
   * requests an eager load. By the time the second call runs the page is resident, so it hands back
   * the very instance the first call produced — the one whose policy is under test.
   */
  private static FragmentCensus probeLoadPolicy(final JsonResourceSession session, final VersioningType versioning) {
    int chained = 0;
    int unchained = 0;
    final TreeMap<Integer, Integer> fragmentCounts = new TreeMap<>();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long maxNodeKey = rtx.getMaxNodeKey();
      final int revision = rtx.getRevisionNumber();
      long probed = 0;
      for (long pageKey = 0; pageKey * 1024 <= maxNodeKey; pageKey++) {
        // One live record on this page, found by scanning forward from its first slot; a page whose
        // records were all deleted has none, and is skipped.
        long nodeKey = -1;
        for (long candidate = pageKey * 1024; candidate < (pageKey + 1) * 1024
            && candidate <= maxNodeKey; candidate++) {
          if (candidate > 0 && rtx.moveTo(candidate)) {
            nodeKey = candidate;
            break;
          }
        }
        if (nodeKey < 0) {
          continue;
        }
        probed++;

        final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision));
        assertNotNull(loaded, versioning + ": page " + pageKey + " vanished between the move and the probe");
        final int fragments = loaded.reference().getPageFragments().size();
        fragmentCounts.merge(fragments, 1, Integer::sum);
        final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
        final boolean loadedLazily = page.chunkCount() > 0;
        if (fragments > 0) {
          // A chain: the load went through combine, which reads every slot of every fragment.
          assertFalse(loadedLazily, versioning + ": page " + pageKey + " has " + fragments
              + " older fragments and was still loaded lazily — A6 says a combined page is loaded whole");
          chained++;
        } else {
          assertTrue(loadedLazily, versioning + ": page " + pageKey
              + " has no fragment chain and was loaded eagerly — the point-lookup policy did not fire");
          unchained++;
        }
      }
      assertTrue(probed >= 3, versioning + ": only " + probed + " record pages were probed");
    }
    return new FragmentCensus(chained, unchained, fragmentCounts);
  }

  /** A value long enough that an update meaningfully resizes the slot it rewrites. */
  private static String value(final int revision, final int index) {
    return "shared-prefix-for-dedup/rev=" + revision + "/idx=" + index + "/tail-" + "x".repeat(8 + (index % 23));
  }
}
