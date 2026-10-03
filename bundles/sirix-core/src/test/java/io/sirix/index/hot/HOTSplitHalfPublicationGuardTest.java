package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Guards the unpublished split halves independently of posting-payload-dependent leaf geometry. */
final class HOTSplitHalfPublicationGuardTest {

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void overlappingSplitHalfIsRejectedBeforePublication(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve("db");
    final List<HOTLeafPage> ownedLeaves = new ArrayList<>(64);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("guard").versioningApproach(versioningType).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("guard");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        final StorageEngineWriter storage = transaction.getStorageEngineWriter();
        storage.prepareSecondaryIndexPage(IndexType.PATH);
        assertRejectedHalf(storage, false, ownedLeaves);
        assertRejectedHalf(storage, true, ownedLeaves);
        storage.assertTransactionWritable();
      }
    } finally {
      // The fixture's leaves are deliberately unpublished. If the guard is mutated away, the
      // transaction closes any leaves integration registered before this final cleanup.
      for (final HOTLeafPage leaf : ownedLeaves) {
        if (!leaf.isClosed()) {
          leaf.close();
        }
      }
    }
  }

  private static void assertRejectedHalf(final StorageEngineWriter storage, final boolean upperHalf,
      final List<HOTLeafPage> ownedLeaves) {
    final int revision = storage.getRevisionNumber();
    final PageReference[] children = new PageReference[HOTIndirectPage.MAX_NODE_ENTRIES];
    final int[] partials = new int[children.length];
    final int halfOffset = upperHalf
        ? 0x80
        : 0;
    for (int slot = 0; slot < children.length; slot++) {
      final HOTLeafPage leaf = new HOTLeafPage(10_000 + ownedLeaves.size(), revision, IndexType.PATH);
      ownedLeaves.add(leaf);
      // The node discriminates bits 0, 2, 3, 4 and 5; bit 1 is deliberately absent.
      final int firstKey = ((slot & 16) << 3) | ((slot & 15) << 2);
      assertTrue(leaf.put(key(firstKey), key(1)));
      if (slot == (upperHalf
          ? 31
          : 15)) {
        assertTrue(leaf.put(key(halfOffset | 0x7f), key(2)));
      }
      children[slot] = reference(leaf);
      partials[slot] = slot;
    }
    final HOTIndirectPage original = HOTIndirectPage.createMultiNode(20_000 + halfOffset, revision, 0,
        0xBC00_0000_0000_0000L, partials, children, 1);
    final PageReference root = reference(original);
    HOTInvariantValidator.validate(root, storage).assertOk();

    final byte[] insertedKey = key(halfOffset | 0x50);
    final int descendedSlot = original.findChildIndex(insertedKey);
    final HOTIncrementalInsert.InsertInfo info = HOTIncrementalInsert.getInsertInformation(original, descendedSlot, 1);
    assertFalse(info.betaIsDiscBit());
    assertEquals(16, info.affectedCount());
    assertEquals(upperHalf
        ? 16
        : 0, info.firstAffected());

    // The source tree is sound, but the real split folds 0x50 after a retained leaf spanning
    // [0x3c, 0x7f] (or the corresponding upper-half keys). Their ranges overlap. This is the
    // sparse-partial failure formerly reached through the correction stream's inline payloads.
    try (HOTLeafPage inserted = new HOTLeafPage(30_000, revision, IndexType.PATH)) {
      assertTrue(inserted.put(insertedKey, key(3)));
      final HOTIncrementalInsert.BiNode split = HOTIncrementalInsert.splitIndirectWithEntry(original, info, 1, 1,
          reference(inserted), revision, new AtomicLong(30_001)::getAndIncrement);
      final PageReference badHalf = upperHalf
          ? split.right()
          : split.left();
      assertFalse(HOTInvariantValidator.validate(badHalf, storage).violations().isEmpty(),
          "the fixture must construct a malformed split half, not merely decline an earlier boundary check");
    }

    final TestWriter writer = new TestWriter(storage, root);
    final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(
        (HOTLeafPage) children[descendedSlot].getPage(), children[descendedSlot], new HOTIndirectPage[] {original},
        new PageReference[] {root}, new int[] {descendedSlot}, 1);
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveFrames = allocator.liveSlotCount(frameClass);
    final int loggedPages = storage.getLog().size();
    final long declinedBefore = AbstractHOTIndexWriter.BRANCH_COMPLETE_FRONTIER.get();

    assertFalse(writer.split(route, info, original, insertedKey), "the malformed half must be refused");
    assertEquals(declinedBefore + 1, AbstractHOTIndexWriter.BRANCH_COMPLETE_FRONTIER.get(),
        "the new-half guard must be reached");
    assertSame(original, root.getPage(), "rejection must precede root publication");
    assertEquals(loggedPages, storage.getLog().size(), "rejection must precede transaction-log registration");
    assertEquals(liveFrames, allocator.liveSlotCount(frameClass), "the speculative key leaf must be released");
    HOTInvariantValidator.validate(root, storage).assertOk();
  }

  private static PageReference reference(final HOTLeafPage leaf) {
    final PageReference reference = new PageReference();
    reference.setPage(leaf);
    return reference;
  }

  private static PageReference reference(final HOTIndirectPage node) {
    final PageReference reference = new PageReference();
    reference.setPage(node);
    return reference;
  }

  private static byte[] key(final int key) {
    return new byte[] {(byte) key};
  }

  private static final class TestWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestWriter(final StorageEngineWriter storage, final PageReference root) {
      super(storage, IndexType.PATH, 0);
      rootReference = root;
    }

    private boolean split(final LeafNavigationResult route, final HOTIncrementalInsert.InsertInfo info,
        final HOTIndirectPage original, final byte[] key) {
      try {
        final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("branchSplitFullNode",
            LeafNavigationResult.class, HOTIncrementalInsert.InsertInfo.class, HOTIndirectPage.class, int.class,
            int.class, int.class, byte[].class, byte[].class);
        method.setAccessible(true);
        return (boolean) method.invoke(this, route, info, original, 0, 1, 1, key, key(3));
      } catch (final InvocationTargetException failure) {
        if (failure.getCause() instanceof RuntimeException runtimeFailure) {
          throw runtimeFailure;
        }
        if (failure.getCause() instanceof Error error) {
          throw error;
        }
        throw new AssertionError(failure.getCause());
      } catch (final ReflectiveOperationException failure) {
        throw new AssertionError(failure);
      }
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] buffer) {
      keyBuffer = buffer;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
      System.arraycopy(key, 0, buffer, offset, key.length);
      return key.length;
    }

    @Override
    protected void prepareIndexPage() {
      // This fixture installs its materialized root without publishing it into the resource.
    }
  }
}
