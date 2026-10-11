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
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Disk-backed counterparts of the constructed ordering/owner regressions. Only the initial valid
 * topology is assembled: mutation, side-page ownership, commit and historical reads use production
 * storage and the public projection API, without mocked collaborators. These tests do not establish
 * ordinary-stream reachability of the direction-one topology.
 */
@Isolated
final class HOTConstructedShapeIntegrationTest {
  private static final String RESOURCE = "constructed-hot-shape";
  private static final byte[] EMPTY = new byte[0];

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void replayBulkPostingRemovalAndRevertPreservesEveryRevision() {
    final Path path = temporaryDirectory.resolve("replay");
    final String stream = """
        P 1 0 0 42
        M 257 0 0 0 3000
        C
        P 513 0 0 99
        X 257 0 0 0 500
        C
        V 1
        P 769 0 0 100
        C
        O
        """;
    HOTStructuralPropertyTest.replay(HOTStructuralPropertyTest.Kind.PATH, VersioningType.DIFFERENTIAL, 16, true, path,
        stream);
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("hot-structural-property")) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 3; revision++) {
        try (JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx(revision)) {
          final HOTLongIndexReader reader =
              HOTLongIndexReader.create(transaction.getStorageEngineReader(), IndexType.PATH, 0);
          final NodeReferences bulk = reader.get(257, SearchMode.EQUAL);
          assertNotNull(bulk);
          final int count = revision == 2
              ? 2500
              : 3000;
          assertEquals(count, bulk.getNodeKeys().getLongCardinality());
          for (int i = 0; i < 3000; i++) {
            assertEquals(revision != 2 || i >= 500, bulk.getNodeKeys().contains(i * 2L));
          }
          final NodeReferences original = reader.get(1, SearchMode.EQUAL);
          assertNotNull(original);
          assertTrue(original.getNodeKeys().contains(42));
          if (revision == 2) {
            final NodeReferences branch = reader.get(513, SearchMode.EQUAL);
            assertNotNull(branch);
            assertTrue(branch.getNodeKeys().contains(99));
          } else {
            assertNull(reader.get(513, SearchMode.EQUAL));
          }
          if (revision == 3) {
            final NodeReferences reverted = reader.get(769, SearchMode.EQUAL);
            assertNotNull(reverted);
            assertTrue(reverted.getNodeKeys().contains(100));
          } else {
            assertNull(reader.get(769, SearchMode.EQUAL));
          }
          System.out.println("[hot-live] replay cold revision " + revision + ": bulk posting count=" + count
              + "; every posting bit exact; original posting retained; reverted branch keys exact");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void mergeAcrossAnAncestorNeighbourCommitsInOrder(final VersioningType versioning) {
    try (final Fixture fixture = new Fixture(temporaryDirectory.resolve("merge"), versioning)) {
      final PageReference below =
          fixture.node(new int[] {60}, new int[] {0, 1}, fixture.leaf(0x37), fixture.leaf(0x3b));
      fixture.install(fixture.node(new int[] {58, 62}, new int[] {0, 2, 3}, fixture.leaf(0x10),
          fixture.leaf(0x25, 0x2d, 0x34), below));
      fixture.commitBaselineAndReopen();
      final long before = AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get();
      fixture.put(0x33, payload(3));
      assertEquals(before + 1, AbstractHOTIndexWriter.MERGE_SPINE_ORDER_DELEGATED.get());
      fixture.commitAndVerifyHistory("merge crossing ancestor neighbour");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void directionOneCountOverflowKeepsTheHalfTrieCondition(final VersioningType versioning) {
    try (final Fixture fixture = new Fixture(temporaryDirectory.resolve("direction-one-count"), versioning)) {
      final HOTLeafPage straddling = fixture.emptyLeaf();
      for (int i = 0; i < 256; i++) {
        fixture.add(straddling, 0x08000000L + i, EMPTY);
        fixture.add(straddling, 0x2a000000L + i, EMPTY);
      }
      fixture.installFullFirstByteRoot(fixture.register(straddling));
      fixture.commitBaselineAndReopen();
      final long before = AbstractHOTIndexWriter.DIRECTION_ONE_SPLIT_ABOVE_HALF.get();
      fixture.put(0x28000000L, payload(3));
      assertEquals(before + 1, AbstractHOTIndexWriter.DIRECTION_ONE_SPLIT_ABOVE_HALF.get());
      fixture.commitAndVerifyHistory("direction-one count overflow");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void directionOnePrefixGrowthOverflowKeepsTheHalfTrieCondition(final VersioningType versioning) {
    try (final Fixture fixture = new Fixture(temporaryDirectory.resolve("direction-one-bytes"), versioning)) {
      final HOTLeafPage straddling = fixture.emptyLeaf();
      for (int i = 0; i < 500; i++) {
        fixture.add(straddling, 0x2a000000L + i, payload(107));
      }
      assertTrue(straddling.getEntryCount() < HOTLeafPage.MAX_ENTRIES);
      fixture.installFullFirstByteRoot(fixture.register(straddling));
      fixture.commitBaselineAndReopen();
      final long before = AbstractHOTIndexWriter.DIRECTION_ONE_SPLIT_ABOVE_HALF.get();
      fixture.put(0x0a000000L, payload(15));
      assertEquals(before + 1, AbstractHOTIndexWriter.DIRECTION_ONE_SPLIT_ABOVE_HALF.get());
      fixture.commitAndVerifyHistory("direction-one prefix growth overflow");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replacingASpilledOwnerThroughTheFrontierPreservesHistory(final VersioningType versioning) {
    try (final Fixture fixture = new Fixture(temporaryDirectory.resolve("side-reference"), versioning)) {
      final long owner = slot(40, 15);
      final byte[] spilled = payload(9_000);
      final HOTLeafPage overflowing = fixture.emptyLeaf();
      fixture.add(overflowing, owner, spilled);
      for (int i = 1; i <= 500; i++) {
        fixture.add(overflowing, slot(40, 15 | (i << 4)), payload(108));
      }
      assertTrue(overflowing.getEntryCount() < HOTLeafPage.MAX_ENTRIES);
      assertTrue(overflowing.getRemainingSpace() < 512, "replacement must overflow by bytes");
      final PageReference overflowingRef = fixture.register(overflowing);
      final PageReference[] children = new PageReference[32];
      final int[] partials = new int[32];
      for (int i = 0; i < 32; i++) {
        partials[i] = i;
        children[i] = i == 31
            ? overflowingRef
            : fixture.leaf(slot(i < 16
                ? 32
                : 40, i & 15));
      }
      final PageReference parent = fixture.node(new int[] {44, 60, 61, 62, 63}, partials, children);
      final PageReference[] grandparents = new PageReference[32];
      final int[] grandPartials = new int[32];
      for (int i = 0; i < 30; i++) {
        grandPartials[i] = i;
        grandparents[i] = fixture.leaf(slot(i, 0));
      }
      grandparents[30] = parent;
      grandPartials[30] = 32;
      grandparents[31] = fixture.leaf(slot(44, 0));
      grandPartials[31] = 44;
      fixture.install(fixture.node(new int[] {42, 43, 44, 45, 46, 47}, grandPartials, grandparents));
      fixture.storage.putSegmentPage(owner, 0, spilled);
      fixture.commitBaselineAndReopen();
      final long routed = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get();
      final long carried = AbstractHOTIndexWriter.FRONTIER_SPLIT_CARRIED_OWNER_SIDE_REFERENCES.get();
      fixture.put(owner, payload(512));
      assertEquals(routed + 1, AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get());
      assertEquals(carried + 1, AbstractHOTIndexWriter.FRONTIER_SPLIT_CARRIED_OWNER_SIDE_REFERENCES.get());
      fixture.commitAndVerifyHistory("frontier replacing spilled owner: 9000 bytes -> 512 inline bytes");
    }
  }

  private static long slot(final int rowGroup, final int kind) {
    return ((long) rowGroup << 16) | kind;
  }

  private static byte[] payload(final int length) {
    final byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) 0x5a);
    return bytes;
  }

  private static byte[] key(final long slot) {
    final byte[] bytes = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(slot, bytes, 0);
    return bytes;
  }

  private static final class Fixture implements AutoCloseable {
    private final Path databasePath;
    private final VersioningType versioning;
    private final TreeMap<Long, byte[]> expected = new TreeMap<>();
    private final Map<Integer, byte[]> encodedValues = new TreeMap<>();
    private @Nullable Map<Long, byte[]> baseline;
    private @Nullable Database<JsonResourceSession> database;
    private @Nullable JsonResourceSession session;
    private @Nullable JsonNodeTrx transaction;
    private StorageEngineWriter engine;
    private ProjectionIndexHOTStorage storage;

    private Fixture(final Path databasePath, final VersioningType versioning) {
      this.databasePath = databasePath;
      this.versioning = versioning;
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
      database = Databases.openJsonDatabase(databasePath);
      assertTrue(
          database.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build()));
      openWriter();
    }

    private void openWriter() {
      final JsonResourceSession session = Objects.requireNonNull(database).beginResourceSession(RESOURCE);
      this.session = session;
      final JsonNodeTrx transaction = session.beginNodeTrx();
      this.transaction = transaction;
      engine = transaction.getStorageEngineWriter();
      storage = new ProjectionIndexHOTStorage(engine, 0);
    }

    /** Obtain real wire values from the production encoder in a separate fixture index. */
    private byte[] encoded(final byte[] value) {
      return encodedValues.computeIfAbsent(value.length, ignored -> {
        final ProjectionIndexHOTStorage encoder = new ProjectionIndexHOTStorage(engine, 1);
        encoder.putBlob(1L, value);
        final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, engine.loadHOTPage(encoder.getRootReference()));
        return leaf.copyStoredValue(leaf.findEntry(key(1L)));
      });
    }

    private long nextPageKey() {
      return engine.<ProjectionIndexPage>prepareSecondaryIndexPage(IndexType.PROJECTION)
                   .incrementAndGetMaxHotPageKey(0);
    }

    private HOTLeafPage emptyLeaf() {
      return new HOTLeafPage(nextPageKey(), engine.getRevisionNumber(), IndexType.PROJECTION);
    }

    private void add(final HOTLeafPage leaf, final long slot, final byte[] value) {
      assertTrue(leaf.put(key(slot), encoded(value)), "constructed leaf must fit before mutation");
      expected.put(slot, value);
    }

    private PageReference leaf(final long... slots) {
      final HOTLeafPage leaf = emptyLeaf();
      for (final long slot : slots) {
        add(leaf, slot, EMPTY);
      }
      return register(leaf);
    }

    private PageReference register(final Page page) {
      final PageReference reference = new PageReference();
      reference.setPage(page);
      engine.getLog().put(reference, PageContainer.getInstance(page, page));
      return reference;
    }

    private PageReference node(final int[] bits, final int[] partials, final PageReference... children) {
      int height = 1;
      for (final PageReference child : children) {
        if (engine.loadHOTPage(child) instanceof HOTIndirectPage indirect) {
          height = Math.max(height, indirect.getHeight() + 1);
        }
      }
      return register(HOTBulkBuilder.assembleIndirect(bits, partials, children, height, engine.getRevisionNumber(),
          IndexType.PROJECTION, this::nextPageKey));
    }

    private void install(final PageReference assembled) {
      final Page page = Objects.requireNonNull(engine.loadHOTPage(assembled));
      final PageReference root = storage.getRootReference();
      root.setPage(page);
      engine.getLog().put(root, PageContainer.getInstance(page, page));
    }

    private void installFullFirstByteRoot(final PageReference straddling) {
      final PageReference[] children = new PageReference[32];
      final int[] partials = new int[32];
      int index = 0;
      for (int partial = 0; partial <= 8; partial++) {
        partials[index] = partial;
        children[index++] = partial == 8
            ? straddling
            : leaf((long) partial << 24);
      }
      for (int partial = 0x30; partial <= 0x36; partial++) {
        partials[index] = partial;
        children[index++] = leaf((long) partial << 24);
      }
      for (int partial = 0x40; partial <= 0x4f; partial++) {
        partials[index] = partial;
        children[index++] = leaf((long) partial << 24);
      }
      assertEquals(32, index);
      install(node(new int[] {33, 34, 35, 36, 37, 38, 39}, partials, children));
    }

    private void verify(final StorageEngineReader reader, final Map<Long, byte[]> model) {
      final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
      final HOTInvariantValidator.Result result = root == null
          ? new HOTInvariantValidator.Result(List.of(), 0, 0)
          : HOTInvariantValidator.validate(root, reader);
      result.assertOk();
      assertEquals(model.size(), result.storedKeyCount());
      for (final Map.Entry<Long, byte[]> entry : model.entrySet()) {
        assertArrayEquals(entry.getValue(), ProjectionIndexHOTStorage.readBlob(reader, 0, entry.getKey()));
      }
    }

    private void put(final long slot, final byte[] value) {
      storage.putBlob(slot, value);
      expected.put(slot, value);
      verify(engine, expected);
    }

    private void commitBaselineAndReopen() {
      verify(engine, expected);
      baseline = new TreeMap<>(expected);
      Objects.requireNonNull(transaction).commit();
      close();
      Databases.clearGlobalCaches();
      database = Databases.openJsonDatabase(databasePath);
      openWriter();
      verify(engine, Objects.requireNonNull(baseline));
    }

    private void commitAndVerifyHistory(final String scenario) {
      final Map<Long, byte[]> baseline = Objects.requireNonNull(this.baseline);
      Objects.requireNonNull(transaction).commit();
      close();
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
          JsonResourceSession resource = reopened.beginResourceSession(RESOURCE)) {
        assertEquals(2, resource.getMostRecentRevisionNumber());
        try (JsonNodeReadOnlyTrx original = resource.beginNodeReadOnlyTrx(1);
            JsonNodeReadOnlyTrx changed = resource.beginNodeReadOnlyTrx(2)) {
          verify(original.getStorageEngineReader(), baseline);
          verify(changed.getStorageEngineReader(), expected);
        }
      }
      System.out.println("[hot-live] " + scenario + "; versioning=" + versioning + "; cold revision 1="
          + baseline.size() + " exact blobs; cold revision 2=" + expected.size()
          + " exact blobs; structural invariant valid in both revisions");
    }

    @Override
    public void close() {
      if (transaction != null) {
        transaction.close();
        transaction = null;
      }
      if (session != null) {
        session.close();
        session = null;
      }
      if (database != null) {
        database.close();
        database = null;
      }
    }
  }
}
