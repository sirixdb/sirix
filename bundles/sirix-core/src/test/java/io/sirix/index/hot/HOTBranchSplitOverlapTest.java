/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replays the full-node branch split's captured sparse paths without posting deltas or fold knobs.
 * The retained leaf endpoints reproduce the page-1745 overlap: the new key sorts inside child 6's
 * range, but splitIndirectWithEntry places it before that child. The seed is validated before use;
 * only subsequent puts use the public writer. Mirroring the node's MSB exercises either split half.
 */
final class HOTBranchSplitOverlapTest {
  private static final String RESOURCE = "branch-split-overlap";
  private static final int INDEX_NUMBER = 0;
  private static final HexFormat HEX = HexFormat.of();
  private static final byte[] INSERTED_KEY = HEX.parseHex("0180000190000000008000019028a4b00100000014");
  private static final int[] DISC_BITS = {102, 103, 104, 105, 106, 107, 108, 163, 165};
  private static final int[] LOWER_PARTIALS =
      {0, 2, 4, 8, 9, 12, 16, 17, 24, 28, 32, 36, 40, 48, 52, 56, 64, 68, 72, 76, 80, 96, 100, 104, 112, 128};
  private static final int[] UPPER_PARTIALS = {256, 272, 288, 320, 384, 448};
  private static final String[][] LOWER_RANGES =
      {{"0180000190000000008000019004982c010000000f", "0180000190000000008000019004982c010000000f"},
          {"0180000190000000008000019004982c0100000010", "0180000190000000008000019004982c0100000014"},
          {"0180000190000000008000019009be880100000010", "018000019000000000800001900ee4e40100000014"},
          {"01800001900000000080000190140b400100000010", "01800001900000000080000190140b400100000013"},
          {"01800001900000000080000190140b400100000014", "01800001900000000080000190140b400100000014"},
          {"0180000190000000008000019019319c0100000010", "018000019000000000800001901e57f80100000014"},
          {"01800001900000000080000190237e540100000010", "018000019000000000800001902dcb0c0100000013"},
          {"018000019000000000800001902dcb0c0100000014", "018000019000000000800001902dcb0c0100000014"},
          {"0180000190000000008000019032f1680100000010", "0180000190000000008000019032f1680100000014"},
          {"018000019000000000800001903817c40100000010", "018000019000000000800001903d3e200100000014"},
          {"0180000190000000008000019042647c0100000011", "01800001900000000080000190478ad80100000014"},
          {"018000019000000000800001904cb1340100000011", "018000019000000000800001904cb1340100000014"},
          {"0180000190000000008000019051d7900100000010", "018000019000000000800001905c24480100000014"},
          {"01800001900000000080000190614aa40100000010", "018000019000000000800001906671000100000014"},
          {"018000019000000000800001906b975c0100000011", "018000019000000000800001906b975c0100000014"},
          {"0180000190000000008000019070bdb80100000011", "018000019000000000800001907b0a700100000014"},
          {"018000019000000000800001908030cc0100000011", "018000019000000000800001908557280100000013"},
          {"018000019000000000800001908a7d840100000012", "018000019000000000800001908fa3e00100000014"},
          {"0180000190000000008000019094ca3c0100000011", "0180000190000000008000019094ca3c0100000013"},
          {"0180000190000000008000019099f0980100000012", "018000019000000000800001909f16f40100000014"},
          {"01800001900000000080000190a43d500100000011", "01800001900000000080000190bdfd1c0100000013"},
          {"01800001900000000080000190c323780100000012", "01800001900000000080000190c323780100000014"},
          {"01800001900000000080000190c849d40100000012", "01800001900000000080000190cd70300100000014"},
          {"01800001900000000080000190d2968c0100000013", "01800001900000000080000190dce3440100000013"},
          {"01800001900000000080000190e209a00100000013", "01800001900000000080000190fbc96c0100000013"},
          {"018000019000000000800001910616240100000013", "018000019000000000800001911aaf940100000013"},};

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> versionsAndHalves() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(version -> Stream.of(Arguments.of(version, false), Arguments.of(version, true)));
  }

  @ParameterizedTest(name = "{0}, insert into upper half: {1}")
  @MethodSource("versionsAndHalves")
  void branchSplitKeepsEveryRevisionOrdered(final VersioningType version, final boolean upperHalf) {
    final Path databasePath = temporaryDirectory.resolve("db");
    final Map<String, byte[]> expected = new TreeMap<>();
    final List<Map<String, byte[]>> revisions = new ArrayList<>();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .versioningApproach(version)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final HOTIndexWriter<ValidTimeKey> writer = writer(wtx.getStorageEngineWriter());
          seedFullNode(writer, upperHalf, expected);
          assertIndex(wtx.getStorageEngineWriter(), expected);
          wtx.commit();
          revisions.add(new TreeMap<>(expected));
        }
        assertRevisions(session, revisions);
        final byte[] inserted = mirrored(INSERTED_KEY, upperHalf);
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final HOTIndexWriter<ValidTimeKey> writer = writer(wtx.getStorageEngineWriter());
          assertFullNodeBranchShape(writer, inserted);
          final long frontiersBefore = AbstractHOTIndexWriter.BRANCH_COMPLETE_FRONTIER.get();
          final long failuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
          put(writer, inserted, expected);
          assertIndex(wtx.getStorageEngineWriter(), expected);
          assertTrue(AbstractHOTIndexWriter.BRANCH_COMPLETE_FRONTIER.get() > frontiersBefore,
              "the malformed compressed half must be declined before publication");
          assertEquals(failuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
              "the rollback-only post-publication guard must never see the rejected half");
          wtx.commit();
          revisions.add(new TreeMap<>(expected));
        }
        assertRevisions(session, revisions);
        // Continue across both halves and across snapshot boundaries after the rejected candidate.
        for (int revision = 0; revision < 4; revision++) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final HOTIndexWriter<ValidTimeKey> writer = writer(wtx.getStorageEngineWriter());
            for (int offset = 1; offset <= 3; offset++) {
              final byte[] key = inserted.clone();
              key[15] += (byte) (revision * 3 + offset);
              if ((offset & 1) == 0) {
                key[12] ^= 2;
              }
              put(writer, key, expected);
              assertIndex(wtx.getStorageEngineWriter(), expected);
            }
            wtx.commit();
            revisions.add(new TreeMap<>(expected));
          }
          assertRevisions(session, revisions);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevisions(session, revisions);
    }
  }

  private static HOTIndexWriter<ValidTimeKey> writer(final StorageEngineWriter storage) {
    return HOTIndexWriter.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
  }

  private static void seedFullNode(final HOTIndexWriter<ValidTimeKey> writer, final boolean upperHalf,
      final Map<String, byte[]> expected) {
    final StorageEngineWriter storage = writer.storageEngineWriter;
    final ValidTimeIndexPage index = storage.prepareSecondaryIndexPage(IndexType.VALIDTIME);
    final PageReference[] children = new PageReference[HOTIndirectPage.MAX_NODE_ENTRIES];
    final int[] partials = new int[children.length];
    int slot = 0;
    if (upperHalf) {
      slot = seedUpperLeaves(storage, index, children, partials, slot, true, expected);
    }
    for (int i = 0; i < LOWER_RANGES.length; i++) {
      final byte[] first = mirrored(HEX.parseHex(LOWER_RANGES[i][0]), upperHalf);
      final byte[] last = mirrored(HEX.parseHex(LOWER_RANGES[i][1]), upperHalf);
      children[slot] = seedLeaf(storage, index, expected, first, last);
      partials[slot++] = LOWER_PARTIALS[i] | (upperHalf
          ? 256
          : 0);
    }
    if (!upperHalf) {
      seedUpperLeaves(storage, index, children, partials, slot, false, expected);
    }
    final HOTIndirectPage root = HOTIndirectPage.createMultiNodeMultiMask(
        index.incrementAndGetMaxHotPageKey(INDEX_NUMBER), storage.getRevisionNumber(), new byte[] {12, 13, 20},
        new long[] {0x03F8_1400_0000_0000L}, 3, partials, children, 1, (short) 102);
    storage.getLog().put(writer.rootReference, PageContainer.getInstance(root, root));
  }

  private static int seedUpperLeaves(final StorageEngineWriter storage, final ValidTimeIndexPage index,
      final PageReference[] children, final int[] partials, int slot, final boolean mirror,
      final Map<String, byte[]> expected) {
    for (final int partial : UPPER_PARTIALS) {
      final byte[] key = INSERTED_KEY.clone();
      for (int column = 0; column < DISC_BITS.length; column++) {
        final int bit = DISC_BITS[column];
        final int mask = 1 << (7 - bit % 8);
        key[bit / 8] &= (byte) ~mask;
        if ((partial & (1 << (DISC_BITS.length - 1 - column))) != 0) {
          key[bit / 8] |= (byte) mask;
        }
      }
      final byte[] mirroredKey = mirrored(key, mirror);
      children[slot] = seedLeaf(storage, index, expected, mirroredKey, mirroredKey);
      partials[slot++] = partial ^ (mirror
          ? 256
          : 0);
    }
    return slot;
  }

  private static PageReference seedLeaf(final StorageEngineWriter storage, final ValidTimeIndexPage index,
      final Map<String, byte[]> expected, final byte[] first, final byte[] last) {
    final HOTLeafPage leaf = new HOTLeafPage(index.incrementAndGetMaxHotPageKey(INDEX_NUMBER),
        storage.getRevisionNumber(), IndexType.VALIDTIME);
    final byte[] value = NodeReferencesSerializer.serialize(new NodeReferences().addNodeKey(7L));
    assertTrue(leaf.put(first, value));
    expected.put(HEX.formatHex(first), value);
    if (!Arrays.equals(first, last)) {
      assertTrue(leaf.put(last, value));
      expected.put(HEX.formatHex(last), value);
    }
    final PageReference reference = new PageReference();
    storage.getLog().put(reference, PageContainer.getInstance(leaf, leaf));
    return reference;
  }

  private static byte[] mirrored(final byte[] key, final boolean upperHalf) {
    final byte[] result = key.clone();
    if (upperHalf) {
      result[12] ^= 2; // Flip the full node's MSB, exchanging its two halves.
    }
    return result;
  }

  private static void assertFullNodeBranchShape(final HOTIndexWriter<ValidTimeKey> writer, final byte[] key) {
    final StorageEngineWriter storage = writer.storageEngineWriter;
    final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, storage.loadHOTPage(writer.rootReference));
    assertEquals(HOTIndirectPage.MAX_NODE_ENTRIES, root.getNumChildren());
    final int child = root.findChildIndex(key);
    final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, storage.loadHOTPage(root.getChildReference(child)));
    final HOTIncrementalInsert.DescentAnalysis analysis =
        HOTIncrementalInsert.analyzeDescent(new HOTIndirectPage[] {root}, new int[] {child}, 1, leaf, key);
    assertEquals(0, analysis.insertDepth());
    final HOTIncrementalInsert.InsertInfo info =
        HOTIncrementalInsert.getInsertInformation(root, analysis.affectedChildIndex(), analysis.mismatchBit());
    assertFalse(info.betaIsDiscBit());
    assertTrue(info.affectedCount() > 1 && info.affectedCount() < root.getNumChildren(),
        "the public put must reach branchSplitFullNode/splitIndirectWithEntry");
  }

  private static void put(final HOTIndexWriter<ValidTimeKey> writer, final byte[] key,
      final Map<String, byte[]> expected) {
    final ValidTimeKey logical = ValidTimeKeySerializer.INSTANCE.deserialize(key, 0, ValidTimeKeySerializer.KEY_BYTES);
    final long chunk = Integer.toUnsignedLong(ByteBuffer.wrap(key).getInt(ValidTimeKeySerializer.KEY_BYTES));
    writer.indexNodeKey(logical, (chunk << 16) | 7L);
    expected.put(HEX.formatHex(key), NodeReferencesSerializer.serialize(new NodeReferences().addNodeKey(7L)));
  }

  private static void assertRevisions(final JsonResourceSession session, final List<Map<String, byte[]>> revisions) {
    for (int revision = 1; revision <= revisions.size(); revision++) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        assertIndex(rtx.getStorageEngineReader(), revisions.get(revision - 1));
      }
    }
  }

  private static void assertIndex(final StorageEngineReader storage, final Map<String, byte[]> expected) {
    final PageReference root = HOTInvariantValidator.resolveRootRef(storage, IndexType.VALIDTIME, INDEX_NUMBER);
    assertNotNull(root);
    HOTInvariantValidator.validate(root, storage).assertOk(); // Includes I8 first-key order and I12 disjoint ranges.
    final List<String> actualKeys = new ArrayList<>(expected.size());
    collect(root, storage, expected, actualKeys);
    assertEquals(new ArrayList<>(expected.keySet()), actualKeys, "physical scan must be ordered and exact");
    final Map<ValidTimeKey, TreeSet<Long>> postings = new TreeMap<>();
    for (final String hex : expected.keySet()) {
      final byte[] key = HEX.parseHex(hex);
      final ValidTimeKey logical =
          ValidTimeKeySerializer.INSTANCE.deserialize(key, 0, ValidTimeKeySerializer.KEY_BYTES);
      final long chunk = Integer.toUnsignedLong(ByteBuffer.wrap(key).getInt(ValidTimeKeySerializer.KEY_BYTES));
      postings.computeIfAbsent(logical, ignored -> new TreeSet<>()).add((chunk << 16) | 7L);
    }
    final HOTIndexReader<ValidTimeKey> reader =
        HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
    for (final Map.Entry<ValidTimeKey, TreeSet<Long>> entry : postings.entrySet()) {
      final NodeReferences actual = reader.get(entry.getKey(), SearchMode.EQUAL);
      assertNotNull(actual);
      assertArrayEquals(entry.getValue().stream().mapToLong(Long::longValue).toArray(), actual.toSortedArray());
    }
  }

  private static void collect(final PageReference reference, final StorageEngineReader storage,
      final Map<String, byte[]> expected, final List<String> actualKeys) {
    final Page page = storage.loadHOTPage(reference);
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        final String key = HEX.formatHex(leaf.getKey(i));
        actualKeys.add(key);
        assertArrayEquals(expected.get(key), leaf.copyStoredValue(i), "posting at " + key);
      }
    } else {
      final HOTIndirectPage indirect = assertInstanceOf(HOTIndirectPage.class, page);
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collect(indirect.getChildReference(i), storage, expected, actualKeys);
      }
    }
  }
}
