/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bisection reproducer for the HOT trie scale bug with dense-common-prefix
 * keys. Walks the failure boundary at pure HOT-trie level using the
 * projection storage's exact key shape (8-byte sign-flipped BE of
 * {@code leafIndex << 8}) so the defect is isolated from the projection-
 * storage layer above.
 */
final class HOTDenseKeyScaleTest {

  private static final String RESOURCE_NAME = "hot-dense-scale";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * Reproduces the scale boundary by running the projection storage's
   * put/get cycle at increasing N and reporting the largest N where all
   * {@code N} keys are retrievable.
   */
  @Test
  void reproduceScaleBoundary_multiChunk() throws IOException {
    final int[] scales = { 50, 100, 200, 500, 1000, 5000 };
    final int payloadSize = 20 * 1024; // 5 chunks each
    for (final int n : scales) {
      JsonTestHelper.deleteEverything();
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
        db.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME)
                .versioningApproach(VersioningType.FULL).build());
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
            final byte[] payload = new byte[payloadSize];
            for (int i = 0; i < n; i++) {
              payload[0] = (byte) (i & 0xFF);
              payload[1] = (byte) ((i >>> 8) & 0xFF);
              storage.put(i, payload);
            }
            wtx.commit();
          }
          try (JsonNodeTrx probe = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(probe.getStorageEngineWriter(), 0);
            int found = 0, missing = 0;
            final StringBuilder missingSpans = new StringBuilder();
            int spanStart = -1;
            for (int i = 0; i < n; i++) {
              final boolean present = storage.get(i) != null;
              if (present) {
                found++;
                if (spanStart >= 0) {
                  if (missingSpans.length() > 0) missingSpans.append(',');
                  missingSpans.append(spanStart).append("..").append(i - 1);
                  spanStart = -1;
                }
              } else {
                missing++;
                if (spanStart < 0) spanStart = i;
              }
            }
            if (spanStart >= 0) {
              if (missingSpans.length() > 0) missingSpans.append(',');
              missingSpans.append(spanStart).append("..").append(n - 1);
            }
            System.out.println(String.format(
                "[scale.multi] n=%6d found=%6d missing=%6d (miss %.1f%%)  missingSpans=[%s]",
                n, found, missing, 100.0 * missing / n, missingSpans.toString()));
            if (n == 200 && missing > 0) {
              System.setProperty("sirix.hot.routing.diag", "true");
              final byte[] k = ProjectionIndexHOTStorage.encodeCompositeKey(96, 0);
              System.err.println("--- routing probe for leafIndex=96 chunkIdx=0 ---");
              storage.get(96L);
              System.clearProperty("sirix.hot.routing.diag");
            }
          }
        }
      }
    }
  }

  @Test
  void reproduceScaleBoundary() throws IOException {
    final int[] scales = { 50, 100, 200, 500, 1000, 5000, 10_000, 100_000 };
    for (final int n : scales) {
      JsonTestHelper.deleteEverything();
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
        db.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME)
                .versioningApproach(VersioningType.FULL).build());
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
            // Single-chunk writes to isolate scale behavior from multi-chunk interaction.
            final byte[] smallPayload = new byte[128];
            for (int i = 0; i < n; i++) {
              smallPayload[0] = (byte) (i & 0xFF);
              smallPayload[1] = (byte) ((i >>> 8) & 0xFF);
              storage.put(i, smallPayload);
            }
            wtx.commit();
          }
          try (JsonNodeTrx probe = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(probe.getStorageEngineWriter(), 0);
            int found = 0, missing = 0;
            final StringBuilder firstMissing = new StringBuilder();
            int spanStart = -1, spanEnd = -1;
            for (int i = 0; i < n; i++) {
              if (storage.get(i) != null) {
                if (spanStart >= 0) {
                  if (firstMissing.length() < 300) {
                    if (firstMissing.length() > 0) firstMissing.append(',');
                    firstMissing.append(spanStart).append("..").append(spanEnd);
                  }
                  spanStart = -1;
                }
                found++;
              } else {
                missing++;
                if (spanStart < 0) spanStart = i;
                spanEnd = i;
              }
            }
            if (spanStart >= 0 && firstMissing.length() < 300) {
              if (firstMissing.length() > 0) firstMissing.append(',');
              firstMissing.append(spanStart).append("..").append(spanEnd);
            }
            System.out.println(String.format("[scale] n=%6d found=%6d missing=%6d (miss %.1f%%)  spans=[%s]",
                n, found, missing, 100.0 * missing / n, firstMissing.toString()));
          }
        }
      }
    }
  }

  /**
   * Distinguishes WRITE-path bug (key never stored) from ROUTING bug
   * (key stored but not reachable via point lookup). Scans all leaves
   * and builds the set of stored keys, then point-looks-up each stored
   * key. Any stored-but-unreachable key implies a post-split routing
   * bug; any missing-from-storage implies a write-path bug.
   */
  /**
   * Diagnostic test: measure tree height across scales and report fan-out
   * distribution. The reference C++ HOT achieves height ≈ O(log_K N) where
   * K ~ 32. A depth well below the leaf count's log₂ indicates healthy
   * consolidation of disc bits into wide SpanNodes/MultiNodes.
   */
  @Test
  void measureHeightAcrossScales() throws IOException {
    final int[] scales = { 300, 1000, 5000 };
    final int payloadSize = 20 * 1024;
    for (final int n : scales) {
      JsonTestHelper.deleteEverything();
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
        db.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
            final byte[] payload = new byte[payloadSize];
            for (int i = 0; i < n; i++) {
              payload[0] = (byte) (i & 0xFF);
              payload[1] = (byte) ((i >>> 8) & 0xFF);
              storage.put(i, payload);
            }
            wtx.commit();
          }
          try (JsonNodeTrx probe = session.beginNodeTrx()) {
            final PageReference root =
                ProjectionIndexHOTStorage.rootReference(probe.getStorageEngineReader(), 0);
            try (HOTTrieReader r = new HOTTrieReader(probe.getStorageEngineReader())) {
              int leafCount = 0;
              int maxDepth = 0;
              final int[] fanOutHistogram = new int[33];
              // Traverse via leftmost-leaf + advanceToNextLeaf for depth,
              // plus a second pass via navigateToLeaf on each leaf key to
              // count fan-out of indirect nodes on the path.
              io.sirix.page.HOTLeafPage leaf = r.navigateToLeftmostLeaf(root);
              while (leaf != null) {
                leafCount++;
                final int pd = r.getPathDepth();
                if (pd > maxDepth) maxDepth = pd;
                for (int d = 0; d < pd; d++) {
                  final io.sirix.page.HOTIndirectPage hp = r.pathNodeAt(d);
                  fanOutHistogram[Math.min(32, hp.getNumChildren())]++;
                }
                leaf = r.advanceToNextLeaf();
              }
              final double log32 = Math.log(leafCount) / Math.log(32);
              final double log2 = Math.log(leafCount) / Math.log(2);
              final StringBuilder hist = new StringBuilder();
              for (int i = 2; i < 33; i++) {
                if (fanOutHistogram[i] > 0) {
                  if (hist.length() > 0) hist.append(',');
                  hist.append(i).append(":").append(fanOutHistogram[i]);
                }
              }
              System.out.println(String.format(
                  "[height] n=%5d leaves=%4d depth=%d (log32=%.1f, log2=%.1f)  fanout=[%s]",
                  n, leafCount, maxDepth, log32, log2, hist.toString()));
            }
          }
        }
      }
    }
  }

  /**
   * Latency micro-benchmark: measures per-put and per-get ns for
   * single-chunk ascending inserts at N=10,000. Validates the HFT-grade
   * claim by producing concrete numbers. Warms up first, then measures.
   */
  @Test
  void latencyBenchmark() throws IOException {
    final int n = 10_000;
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        // Warmup
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
          final byte[] payload = new byte[128];
          for (int i = 0; i < 1000; i++) storage.put(i, payload);
          wtx.commit();
        }
        JsonTestHelper.deleteEverything();
        Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
        try (Database<JsonResourceSession> db2 = Databases.openJsonDatabase(DATABASE_PATH)) {
          db2.createResource(
              ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
          try (JsonResourceSession s2 = db2.beginResourceSession(RESOURCE_NAME)) {
            // Measured write
            long writeNs;
            try (JsonNodeTrx wtx = s2.beginNodeTrx()) {
              final ProjectionIndexHOTStorage storage =
                  new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
              final byte[] payload = new byte[128];
              final long t0 = System.nanoTime();
              for (int i = 0; i < n; i++) storage.put(i, payload);
              writeNs = System.nanoTime() - t0;
              wtx.commit();
            }
            // Measured read
            long readNs;
            int found = 0;
            try (JsonNodeTrx probe = s2.beginNodeTrx()) {
              final ProjectionIndexHOTStorage storage =
                  new ProjectionIndexHOTStorage(probe.getStorageEngineWriter(), 0);
              final long t0 = System.nanoTime();
              for (int i = 0; i < n; i++) {
                if (storage.get(i) != null) found++;
              }
              readNs = System.nanoTime() - t0;
            }
            System.out.println(String.format(
                "[latency] N=%d  write=%.1f ns/op  read=%.1f ns/op  found=%d/%d",
                n, writeNs / (double) n, readNs / (double) n, found, n));
          }
        }
      }
    }
  }

  @Test
  void diagStoredVsReachable_n5000() throws IOException {
    final int n = 5000;
    final int payloadSize = 20 * 1024;
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
          final byte[] payload = new byte[payloadSize];
          for (int i = 0; i < n; i++) {
            payload[0] = (byte) (i & 0xFF);
            payload[1] = (byte) ((i >>> 8) & 0xFF);
            storage.put(i, payload);
          }
          wtx.commit();
        }
        try (JsonNodeTrx probe = session.beginNodeTrx()) {
          final PageReference root =
              ProjectionIndexHOTStorage.rootReference(probe.getStorageEngineReader(), 0);
          try (HOTTrieReader r = new HOTTrieReader(probe.getStorageEngineReader())) {
            final java.util.Set<Long> storedLeafIdx = new java.util.TreeSet<>();
            io.sirix.page.HOTLeafPage leaf = r.navigateToLeftmostLeaf(root);
            while (leaf != null) {
              for (int i = 0; i < leaf.getEntryCount(); i++) {
                final long[] dec = ProjectionIndexHOTStorage.decodeCompositeKey(leaf.getKey(i));
                storedLeafIdx.add(dec[0]);
              }
              leaf = r.advanceToNextLeaf();
            }
            int storedCount = storedLeafIdx.size();
            int reachable = 0;
            int storedUnreachable = 0;
            for (long li = 0; li < n; li++) {
              if (storedLeafIdx.contains(li)) {
                if (r.get(root, ProjectionIndexHOTStorage.encodeCompositeKey(li, 0)) != null) reachable++;
                else storedUnreachable++;
              }
            }
            System.out.println("[diag5k] stored=" + storedCount + " reachable=" + reachable
                + " stored-but-unreachable=" + storedUnreachable
                + " never-stored=" + (n - storedCount));
          }
        }
      }
    }
  }

  @Test
  void diagStoredVsReachable_n300() throws IOException {
    final int n = 300;
    final int payloadSize = 20 * 1024;
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
          final byte[] payload = new byte[payloadSize];
          for (int i = 0; i < n; i++) {
            payload[0] = (byte) (i & 0xFF);
            payload[1] = (byte) ((i >>> 8) & 0xFF);
            storage.put(i, payload);
          }
          wtx.commit();
        }
        try (JsonNodeTrx probe = session.beginNodeTrx()) {
          final PageReference root =
              ProjectionIndexHOTStorage.rootReference(probe.getStorageEngineReader(), 0);
          try (HOTTrieReader r = new HOTTrieReader(probe.getStorageEngineReader())) {
            int[] storedLeafCount = new int[1];
            int[] storedChunkCount = new int[1];
            final java.util.Set<Long> storedLeafIdx = new java.util.TreeSet<>();
            io.sirix.page.HOTLeafPage leaf = r.navigateToLeftmostLeaf(root);
            while (leaf != null) {
              final int ec = leaf.getEntryCount();
              storedLeafCount[0]++;
              for (int i = 0; i < ec; i++) {
                final byte[] k = leaf.getKey(i);
                final long[] dec = ProjectionIndexHOTStorage.decodeCompositeKey(k);
                storedLeafIdx.add(dec[0]);
                storedChunkCount[0]++;
              }
              leaf = r.advanceToNextLeaf();
            }
            System.out.println("[diag] stored-leafIndices=" + storedLeafIdx.size()
                + " stored-chunks=" + storedChunkCount[0] + " leaves=" + storedLeafCount[0]);

            int reachable = 0, missing = 0;
            final StringBuilder missingButStored = new StringBuilder();
            for (long li : storedLeafIdx) {
              final byte[] probeKey = ProjectionIndexHOTStorage.encodeCompositeKey(li, 0);
              if (r.get(root, probeKey) != null) reachable++;
              else {
                missing++;
                if (missingButStored.length() < 300)
                  missingButStored.append(li).append(',');
              }
            }
            System.out.println("[diag] reachable=" + reachable + " stored-but-unreachable="
                + missing + "  first-stored-unreachable=[" + missingButStored + "]");

            for (int i = 0; i < n; i++) {
              if (!storedLeafIdx.contains((long) i)) {
                System.out.println("[diag] leafIndex " + i + " NEVER stored");
                if (i > 10) break;
              }
            }

            // Navigate to leaf for k=2 via reader; then compare against
            // the leaf that actually STORES k=2 (iterated via leaf scan).
            final byte[] k2 = ProjectionIndexHOTStorage.encodeCompositeKey(2, 0);
            final io.sirix.page.HOTLeafPage routedLeaf = r.navigateToLeaf(root, k2);
            System.out.println("[diag] routed-to leaf for leafIndex=2: "
                + (routedLeaf == null ? "null"
                : "first=" + toHex(routedLeaf.getFirstKey())
                + " last=" + toHex(routedLeaf.getLastKey())
                + " entries=" + routedLeaf.getEntryCount()
                + " findEntry(k2)=" + routedLeaf.findEntry(k2)));

            // Dump path
            final int pd = r.getPathDepth();
            System.out.println("[diag] path depth = " + pd);
            for (int d = 0; d < pd; d++) {
              final io.sirix.page.HOTIndirectPage hp = r.pathNodeAt(d);
              final int ci = r.pathChildAt(d);
              System.out.println("[diag]   depth=" + d + " type=" + hp.getNodeType()
                  + " layout=" + hp.getLayoutType()
                  + " numChildren=" + hp.getNumChildren()
                  + " initialBytePos=" + hp.getInitialBytePos()
                  + " bitMask=0x" + Long.toHexString(hp.getBitMask())
                  + " chosenChild=" + ci);
              final StringBuilder pks = new StringBuilder();
              for (int i = 0; i < hp.getNumChildren(); i++) {
                if (i > 0) pks.append(',');
                pks.append("0x").append(Integer.toHexString(hp.getPartialKey(i)));
              }
              System.out.println("[diag]     partialKeys=[" + pks + "]");
            }

            // Dump children of each indirect on the routing path
            for (int d = 0; d < pd; d++) {
              final io.sirix.page.HOTIndirectPage hp = r.pathNodeAt(d);
              for (int ci = 0; ci < hp.getNumChildren(); ci++) {
                final io.sirix.page.PageReference cref = hp.getChildReference(ci);
                io.sirix.page.interfaces.Page p = cref.getPage();
                if (p == null) continue;
                final String kind;
                final byte[] firstK;
                if (p instanceof io.sirix.page.HOTLeafPage lp) { kind = "LEAF(n=" + lp.getEntryCount() + ")"; firstK = lp.getFirstKey(); }
                else if (p instanceof io.sirix.page.HOTIndirectPage hip) { kind = "INDIRECT(" + hip.getNodeType() + ",n=" + hip.getNumChildren() + ")"; firstK = null; }
                else { kind = String.valueOf(p); firstK = null; }
                System.out.println("[diag]   depth=" + d + " child[" + ci + "].partial=0x"
                    + Integer.toHexString(hp.getPartialKey(ci)) + " → " + kind
                    + (firstK != null ? " first=" + toHex(firstK) : ""));
              }
            }

            // Dump ALL paths to ALL leaves — find where the leaf that stores
            // leafIndex=2..3 lives in the tree.
            System.out.println("[diag] --- all leaf paths ---");
            dumpPaths(root, 0, "");

            // Find the leaf that actually stores leafIndex=2
            r.clearPathPublic();
            io.sirix.page.HOTLeafPage sc = r.navigateToLeftmostLeaf(root);
            while (sc != null) {
              for (int i = 0; i < sc.getEntryCount(); i++) {
                final byte[] kk = sc.getKey(i);
                final long[] d = ProjectionIndexHOTStorage.decodeCompositeKey(kk);
                if (d[0] == 2 && d[1] == 0) {
                  System.out.println("[diag] actual stored leaf: first=" + toHex(sc.getFirstKey())
                      + " last=" + toHex(sc.getLastKey())
                      + " entries=" + sc.getEntryCount());
                  break;
                }
              }
              sc = r.advanceToNextLeaf();
            }
          }
        }
      }
    }
  }

  private static void dumpPaths(io.sirix.page.PageReference ref, int depth, String path) {
    final io.sirix.page.interfaces.Page p = ref.getPage();
    if (p instanceof io.sirix.page.HOTLeafPage lp) {
      if (depth < 5 || lp.getFirstKey()[6] < 10) {
        System.out.println("[diag]     path=" + path + " LEAF first=" + toHex(lp.getFirstKey())
            + " last=" + toHex(lp.getLastKey()) + " n=" + lp.getEntryCount());
      }
      return;
    }
    if (!(p instanceof io.sirix.page.HOTIndirectPage hp)) return;
    if (depth > 12) { System.out.println("[diag]     path=" + path + " TOO DEEP"); return; }
    for (int i = 0; i < hp.getNumChildren(); i++) {
      final io.sirix.page.PageReference cref = hp.getChildReference(i);
      if (cref == null) continue;
      dumpPaths(cref, depth + 1, path + "[" + i + "/" + hp.getNumChildren() + "@iBP=" + hp.getInitialBytePos()
          + ",m=0x" + Long.toHexString(hp.getBitMask()) + ",pk=0x" + Integer.toHexString(hp.getPartialKey(i)) + "]");
    }
  }

  private static String toHex(byte[] bytes) {
    if (bytes == null) return "null";
    final StringBuilder sb = new StringBuilder();
    for (byte b : bytes) sb.append(String.format("%02X", b & 0xFF));
    return sb.toString();
  }

  /**
   * Direct HOT trie reproducer bypassing the projection storage —
   * establishes the defect is in the HOT layer itself, not in the
   * projection wrapper.
   */
  @Test
  void directHOT_denseKeys_200Entries_allRetrievable() throws IOException {
    final int n = 200;
    final PathKeySerializer keyser = PathKeySerializer.INSTANCE;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        final byte[] smallPayload = new byte[] { 0, 0, 1, 2, 3 };
        for (int i = 0; i < n; i++) {
          smallPayload[0] = (byte) (i & 0xFF);
          smallPayload[1] = (byte) ((i >>> 8) & 0xFF);
          storage.put(i, smallPayload);
        }
        wtx.commit();
      }
      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final PageReference root =
            ProjectionIndexHOTStorage.rootReference(probe.getStorageEngineReader(), 0);
        assertEquals(true, root != null);

        try (HOTTrieReader trieReader = new HOTTrieReader(probe.getStorageEngineReader())) {
          int found = 0, missing = 0;
          for (int i = 0; i < n; i++) {
            final byte[] key = ProjectionIndexHOTStorage.encodeCompositeKey(i, 0);
            final MemorySegment slice = trieReader.get(root, key);
            if (slice != null && slice.byteSize() > 0) found++;
            else missing++;
          }
          System.out.println("[directHOT] n=" + n + " found=" + found + " missing=" + missing);
          assertEquals(n, found, "all " + n + " keys must be retrievable via HOTTrieReader.get");
        }
      }
    }
  }
}
