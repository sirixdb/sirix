/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * SHAPE-CANONICITY PROBE (task #76 phase 1): is a live sirix HOT trie's structure a function of its
 * key set alone, or of the insertion order?
 *
 * <p>
 * Feeds the SAME (key, nodeKey) set through the PRODUCTION per-entry insert path
 * ({@code AbstractHOTIndexWriter.doIndex} via {@link HOTLongIndexWriter#indexNodeKey}) in three
 * different orders — ascending, descending, shuffled — plus once through the bulk loader
 * ({@link HOTLongBulkIndexLoader} → {@link HOTBulkBuilder}), each into its own index number of the
 * same transaction. Then walks every resulting trie and compares full structural signatures (node
 * kinds, heights, discriminative bits, partials, leaf partitioning, leaf content hashes —
 * everything except allocator-dependent page keys).
 *
 * <p>
 * The probe ASSERTS only semantic equivalence (every key readable with an identical postings value
 * in all four tries). Shape equality is REPORTED, not asserted — measuring it is the purpose of the
 * probe.
 */
final class HOTInsertionOrderShapeProbe {

  private static final String RESOURCE_NAME = "hot-order-probe";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final int N = 20_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.SLIDING_SNAPSHOT).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void insertionOrderShapeDifferential() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final StorageEngineWriter sew = wtx.getStorageEngineWriter();

      probePattern(sew, "dense", denseKeys(), 0);
      probePattern(sew, "strided(rowGroup<<16|slot)", stridedKeys(), 4);
    }
  }

  /** Dense monotone keys 0..N-1 — the shape of order labels / locator ids. */
  private static long[] denseKeys() {
    final long[] keys = new long[N];
    for (int i = 0; i < N; i++) {
      keys[i] = i;
    }
    return keys;
  }

  /** Strided keys (i << 16) | 3 — the projection column-segment slot shape. */
  private static long[] stridedKeys() {
    final long[] keys = new long[N];
    for (int i = 0; i < N; i++) {
      keys[i] = ((long) i << 16) | 3L;
    }
    return keys;
  }

  private static void probePattern(final StorageEngineWriter sew, final String label, final long[] ascending,
      final int indexNumberBase) {
    final long[] descending = reverse(ascending);
    final long[] shuffled = shuffle(ascending, 42L);

    final HOTLongIndexWriter ascWriter = insertIncrementally(sew, indexNumberBase, ascending);
    final HOTLongIndexWriter descWriter = insertIncrementally(sew, indexNumberBase + 1, descending);
    final HOTLongIndexWriter shufWriter = insertIncrementally(sew, indexNumberBase + 2, shuffled);
    final HOTLongIndexWriter bulkWriter = insertViaBulkLoader(sew, indexNumberBase + 3, shuffled);
    // Second bulk build from a DIFFERENT feed order — the loader sorts, so if the builder is
    // deterministic the two bulk tries must be structurally identical.
    final HOTLongIndexWriter bulk2Writer = insertViaBulkLoader(sew, indexNumberBase + 100, descending);

    final String ascSig = signature(ascWriter, sew);
    final String descSig = signature(descWriter, sew);
    final String shufSig = signature(shufWriter, sew);
    final String bulkSig = signature(bulkWriter, sew);
    requireNonTrivial(ascSig);
    requireNonTrivial(descSig);
    requireNonTrivial(shufSig);
    requireNonTrivial(bulkSig);

    System.out.println("==== pattern " + label + " (N=" + N + ") ====");
    printStats("ascending  ", ascWriter, sew);
    printStats("descending ", descWriter, sew);
    printStats("shuffled   ", shufWriter, sew);
    printStats("bulk       ", bulkWriter, sew);
    System.out.println("shape asc==desc : " + ascSig.equals(descSig));
    System.out.println("shape asc==shuf : " + ascSig.equals(shufSig));
    System.out.println("shape desc==shuf: " + descSig.equals(shufSig));
    System.out.println("shape asc==bulk : " + ascSig.equals(bulkSig));
    System.out.println("shape desc==bulk: " + descSig.equals(bulkSig));
    System.out.println("shape shuf==bulk: " + shufSig.equals(bulkSig));
    final String bulk2Sig = signature(bulk2Writer, sew);
    requireNonTrivial(bulk2Sig);
    System.out.println("shape bulk==bulk2 (determinism): " + bulkSig.equals(bulk2Sig));
    System.out.println("-- descending tree head (height-consistency check) --");
    final String[] descLines = descSig.split("\n");
    for (int i = 0; i < Math.min(descLines.length, 12); i++) {
      System.out.println("  " + trimTo(descLines[i], 150));
    }
    if (!ascSig.equals(bulkSig)) {
      printFirstDivergence("asc-vs-bulk", ascSig, bulkSig);
    }
    if (!ascSig.equals(descSig)) {
      printFirstDivergence("asc-vs-desc", ascSig, descSig);
    }

    // The one hard gate: SEMANTIC equivalence. Every key must be present with the identical
    // postings payload in all four tries regardless of insertion order or build route.
    for (final long key : ascending) {
      final NodeReferences fromAsc = ascWriter.get(key, SearchMode.EQUAL);
      final NodeReferences fromDesc = descWriter.get(key, SearchMode.EQUAL);
      final NodeReferences fromShuf = shufWriter.get(key, SearchMode.EQUAL);
      final NodeReferences fromBulk = bulkWriter.get(key, SearchMode.EQUAL);
      assertNotNull(fromAsc, "asc missing key " + key);
      assertNotNull(fromDesc, "desc missing key " + key);
      assertNotNull(fromShuf, "shuf missing key " + key);
      assertNotNull(fromBulk, "bulk missing key " + key);
      assertEquals(fromAsc.getNodeKeys(), fromDesc.getNodeKeys(), "asc/desc postings differ at key " + key);
      assertEquals(fromAsc.getNodeKeys(), fromShuf.getNodeKeys(), "asc/shuf postings differ at key " + key);
      assertEquals(fromAsc.getNodeKeys(), fromBulk.getNodeKeys(), "asc/bulk postings differ at key " + key);
    }
    System.out.println("semantic equivalence (all 4 routes, " + N + " keys): OK");
  }

  private static HOTLongIndexWriter insertIncrementally(final StorageEngineWriter sew, final int indexNumber,
      final long[] keys) {
    final HOTLongIndexWriter writer = HOTLongIndexWriter.create(sew, IndexType.PATH, indexNumber);
    for (final long key : keys) {
      writer.indexNodeKey(key, key);
    }
    return writer;
  }

  private static HOTLongIndexWriter insertViaBulkLoader(final StorageEngineWriter sew, final int indexNumber,
      final long[] keys) {
    final HOTLongIndexWriter writer = HOTLongIndexWriter.create(sew, IndexType.PATH, indexNumber);
    final HOTLongBulkIndexLoader loader = writer.createBulkLoader();
    for (final long key : keys) {
      loader.add(key, key);
    }
    loader.flush();
    return writer;
  }

  // ====================================================================
  // Structural signature
  // ====================================================================

  /**
   * Full structural signature of the writer's trie: node kinds, heights, disc bits, partials, child
   * counts, leaf entry counts and leaf content hashes. Page keys are deliberately excluded (allocator
   * order differs across the four builds by construction).
   */
  private static String signature(final HOTLongIndexWriter writer, final StorageEngineWriter sew) {
    final StringBuilder sb = new StringBuilder(1 << 16);
    walk(writer.getRootReference(), sew.getLog(), sb, 0);
    return sb.toString();
  }

  private static void walk(final PageReference ref, final TransactionIntentLog log, final StringBuilder sb,
      final int depth) {
    final Page page = resolve(ref, log);
    if (page == null) {
      sb.append(indentOf(depth)).append("UNRESOLVED\n");
      return;
    }
    if (page instanceof HOTLeafPage leaf) {
      long contentHash = 1469598103934665603L;
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        contentHash = fnv(contentHash, leaf.getKey(i));
        contentHash = fnv(contentHash, leaf.getValue(i));
      }
      sb.append(indentOf(depth))
        .append("L{n=")
        .append(leaf.getEntryCount())
        .append(",first=")
        .append(leaf.getEntryCount() > 0
            ? HexFormat.of().formatHex(leaf.getKey(0))
            : "-")
        .append(",content=")
        .append(Long.toHexString(contentHash))
        .append("}\n");
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    sb.append(indentOf(depth))
      .append("I{type=")
      .append(indirect.getNodeType())
      .append(",h=")
      .append(indirect.getHeight())
      .append(",n=")
      .append(indirect.getNumChildren())
      .append(",bits=")
      .append(Arrays.toString(HOTIncrementalInsert.discriminativeBits(indirect)))
      .append(",partials=")
      .append(Arrays.toString(Arrays.copyOf(indirect.getPartialKeysRef(), indirect.getNumChildren())))
      .append("}\n");
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      walk(indirect.getChildReference(i), log, sb, depth + 1);
    }
  }

  /** In-transaction page resolution: swizzled page first, then the transaction-intent log. */
  private static Page resolve(final PageReference ref, final TransactionIntentLog log) {
    final Page page = ref.getPage();
    if (page != null) {
      return page;
    }
    final PageContainer container = log.get(ref);
    if (container != null) {
      final Page modified = container.getModified();
      if (modified != null) {
        ref.setPage(modified);
        return modified;
      }
    }
    return null;
  }

  private static void printStats(final String label, final HOTLongIndexWriter writer, final StorageEngineWriter sew) {
    final int[] stats = new int[4]; // {leaves, indirects, minFill, maxFill}
    stats[2] = Integer.MAX_VALUE;
    collectStats(writer.getRootReference(), sew.getLog(), stats);
    final Page root = resolve(writer.getRootReference(), sew.getLog());
    final int height = root instanceof HOTIndirectPage ind
        ? ind.getHeight()
        : 0;
    System.out.println(label + " leaves=" + stats[0] + " indirects=" + stats[1] + " height=" + height + " leafFill=["
        + (stats[2] == Integer.MAX_VALUE
            ? 0
            : stats[2])
        + ".." + stats[3] + "]/" + HOTLeafPage.MAX_ENTRIES);
  }

  private static void collectStats(final PageReference ref, final TransactionIntentLog log, final int[] stats) {
    final Page page = resolve(ref, log);
    if (page instanceof HOTLeafPage leaf) {
      stats[0]++;
      stats[2] = Math.min(stats[2], leaf.getEntryCount());
      stats[3] = Math.max(stats[3], leaf.getEntryCount());
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      stats[1]++;
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectStats(indirect.getChildReference(i), log, stats);
      }
    }
  }

  private static void printFirstDivergence(final String label, final String a, final String b) {
    final String[] linesA = a.split("\n");
    final String[] linesB = b.split("\n");
    final int limit = Math.min(linesA.length, linesB.length);
    for (int i = 0; i < limit; i++) {
      if (!linesA[i].equals(linesB[i])) {
        System.out.println("first divergence (" + label + ") at walk line " + i + ":");
        System.out.println("  A: " + trimTo(linesA[i], 160));
        System.out.println("  B: " + trimTo(linesB[i], 160));
        return;
      }
    }
    System.out.println("divergence (" + label + "): one walk is a prefix of the other (lines " + linesA.length + " vs "
        + linesB.length + ")");
  }

  private static String trimTo(final String s, final int max) {
    return s.length() <= max
        ? s
        : s.substring(0, max) + "…";
  }

  private static String indentOf(final int depth) {
    return "  ".repeat(depth);
  }

  private static long fnv(long hash, final byte[] bytes) {
    for (final byte b : bytes) {
      hash = (hash ^ (b & 0xFF)) * 1099511628211L;
    }
    return hash;
  }

  private static long[] reverse(final long[] keys) {
    final long[] out = keys.clone();
    for (int i = 0, j = out.length - 1; i < j; i++, j--) {
      final long tmp = out[i];
      out[i] = out[j];
      out[j] = tmp;
    }
    return out;
  }

  private static long[] shuffle(final long[] keys, final long seed) {
    final List<Long> list = new ArrayList<>(keys.length);
    for (final long key : keys) {
      list.add(key);
    }
    Collections.shuffle(list, new Random(seed));
    final long[] out = new long[keys.length];
    for (int i = 0; i < out.length; i++) {
      out[i] = list.get(i);
    }
    return out;
  }

  /** Guard against silently probing an empty tree (a vacuous signature comparison). */
  private static void requireNonTrivial(final String sig) {
    if (sig.isEmpty() || sig.startsWith("UNRESOLVED")) {
      fail("probe walked an empty or unresolvable trie — the comparison would be vacuous");
    }
  }
}
