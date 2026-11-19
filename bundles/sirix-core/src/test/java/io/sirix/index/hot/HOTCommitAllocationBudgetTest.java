package io.sirix.index.hot;

import com.sun.management.ThreadMXBean;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Runs in the clean allocation JVM: Mockito instrumentation itself allocates on storage calls. */
@Isolated
final class HOTCommitAllocationBudgetTest {
  private static final int ITERATIONS = 40_000;
  private static volatile Object sink;
  private static final MethodHandle REFRESH = refreshHandle();

  @TempDir
  Path directory;

  @Test
  void sessionWorkspaceSurvivesCommitRollbackAndWriterReplacementWithoutAllocation() {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("reuse"))));
    HOTCommitNormalizer.Scratch previous;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.resolve("reuse"))) {
      database.createResource(ResourceConfiguration.newBuilder("r").storeDiffs(false).build());
      final JsonResourceSession session = database.beginResourceSession("r");
      final InternalResourceSession<?, ?> internal = (InternalResourceSession<?, ?>) session;
      try (session) {
        previous = internal.getHOTCommitScratch();
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final StorageEngineWriter before = trx.getStorageEngineWriter();
          trx.commit();
          assertNotSame(before, trx.getStorageEngineWriter());
          assertSame(previous, internal.getHOTCommitScratch());
          trx.rollback();
          assertSame(previous, internal.getHOTCommitScratch());
        }
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          assertSame(previous, internal.getHOTCommitScratch());
          for (int i = 0; i < ITERATIONS; i++)
            sink = internal.getHOTCommitScratch();
          final long start = bean.getThreadAllocatedBytes(thread);
          for (int i = 0; i < ITERATIONS; i++)
            sink = internal.getHOTCommitScratch();
          assertEquals(0, bean.getThreadAllocatedBytes(thread) - start,
              "repeated session workspace acquisition must allocate zero bytes");
          for (final HOTIndirectPage ancestor : previous.ancestors)
            assertNull(ancestor);
          for (final PageReference child : previous.children)
            assertNull(child);
          trx.rollback();
        }
      }
      assertThrows(IllegalStateException.class, internal::getHOTCommitScratch);
      try (JsonResourceSession reopened = database.beginResourceSession("r")) {
        assertNotSame(previous, ((InternalResourceSession<?, ?>) reopened).getHOTCommitScratch());
      }
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  @Test
  void publicationAllocatesAtMostItsPinnedContainerAndCacheRetirementBudget() {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("publication"))));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.resolve("publication"))) {
      database.createResource(ResourceConfiguration.newBuilder("r").storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("r");
          JsonNodeTrx trx = session.beginNodeTrx();
          HOTLeafPage a = new HOTLeafPage(1, 1, IndexType.CAS);
          HOTLeafPage b = new HOTLeafPage(2, 1, IndexType.CAS)) {
        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        final HOTIndirectPage page = HOTIndirectPage.createBiNode(3, 1, 0, ref(a), ref(b), 1);
        final PageReference reference = ref(page);
        for (int i = 0; i < ITERATIONS; i++) {
          writer.getLog().put(reference, PageContainer.getInstance(page, page));
        }
        final long containerStart = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < ITERATIONS; i++)
          sink = PageContainer.getInstance(page, page);
        assertEquals(32L * ITERATIONS, bean.getThreadAllocatedBytes(thread) - containerStart);
        final long start = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < ITERATIONS; i++) {
          writer.getLog().put(reference, PageContainer.getInstance(page, page));
        }
        final long bytes = bean.getThreadAllocatedBytes(thread) - start;
        assertTrue(bytes <= (32L + 152L) * ITERATIONS,
            "publication bound is exactly 32 B container + 152 B existing cache retirement, actual=" + bytes);
        assertSame(page, writer.getLog().get(reference).getModified());
        trx.rollback();
      }
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void normalizationPinsFoldAndPublicationCountsWithZeroWorkspace(final int folds) {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("folds"))));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.resolve("folds"))) {
      database.createResource(ResourceConfiguration.newBuilder("r").storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("r");
          JsonNodeTrx trx = session.beginNodeTrx();
          HOTLeafPage a = new HOTLeafPage(1, 1, IndexType.CAS);
          HOTLeafPage b = new HOTLeafPage(2, 1, IndexType.CAS)) {
        final InternalResourceSession<?, ?> internal = (InternalResourceSession<?, ?>) session;
        final HOTCommitNormalizer.Scratch scratch = internal.getHOTCommitScratch();
        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        final PageReference left = ref(a);
        final PageReference right = ref(b);
        writer.getLog().put(left, PageContainer.getInstance(a, a));
        writer.getLog().put(right, PageContainer.getInstance(b, b));
        final PageReference[] chain = new PageReference[folds + 1];
        for (int i = 0; i < chain.length; i++)
          chain[i] = new PageReference();
        long measured = 0;
        // Match the other allocation tests' compilation warmup; reuse TIL identities so the
        // measured operation cannot coincide with fixture growth or first-use linkage.
        for (int iteration = 0; iteration < ITERATIONS + 1_000; iteration++) {
          HOTIndirectPage root = HOTIndirectPage.createBiNode(3, 1, folds, left, right, 1);
          PageReference reference = chain[0];
          writer.getLog().put(reference, PageContainer.getInstance(root, root));
          for (int depth = folds - 1; depth >= 0; depth--) {
            root = HOTIndirectPage.createBiNode(4 + depth, 1, depth, reference, right, folds - depth + 1);
            reference = chain[folds - depth];
            writer.getLog().put(reference, PageContainer.getInstance(root, root));
          }
          final long start = bean.getThreadAllocatedBytes(thread);
          final HOTIndirectPage normalized =
              HOTCommitNormalizer.normalize(reference, root, writer, internal.getHOTCommitScratch());
          final long bytes = bean.getThreadAllocatedBytes(thread) - start;
          if (iteration >= ITERATIONS)
            measured += bytes;
          assertEquals(1, normalized.getHeight());
          assertEquals(folds + 2, normalized.getNumChildren());
          assertEquals(0, scratch.unionBitCount);
          for (final HOTIndirectPage ancestor : scratch.ancestors)
            assertNull(ancestor);
          for (final PageReference child : scratch.children)
            assertNull(child);
        }
        // Three- and four-child SingleMask output graphs each occupy exactly 240 bytes.
        // One publication per folded ancestor; cache escape elimination may reduce its cost.
        assertTrue(measured <= 1_000L * folds * (240L + 32L + 152L),
            "only the counted fold graphs and publications are allowed, actual=" + measured);
        trx.rollback();
      }
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  @Test
  void cleanAndResidentDirtyNoFoldPathsAllocateNothing() throws Throwable {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("db"))));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.resolve("db"))) {
      database.createResource(ResourceConfiguration.newBuilder("r").storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("r"); JsonNodeTrx trx = session.beginNodeTrx()) {
        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        final HOTLeafPage a = new HOTLeafPage(100, 1, IndexType.CAS);
        final HOTLeafPage b = new HOTLeafPage(101, 1, IndexType.CAS);
        final PageReference ar = ref(a);
        final PageReference br = ref(b);
        writer.getLog().put(ar, PageContainer.getInstance(a, a));
        writer.getLog().put(br, PageContainer.getInstance(b, b));
        final HOTIndirectPage root = HOTIndirectPage.createBiNode(102, 1, 0, ar, br, 1);
        final PageReference clean = ref(root);
        final PageReference dirty = ref(root);
        writer.getLog().put(dirty, PageContainer.getInstance(root, root));
        for (int round = 0; round < 4; round++) {
          normalizeBatch(clean, root, writer, scratch);
          normalizeBatch(dirty, root, writer, scratch);
        }
        final long cleanStart = bean.getThreadAllocatedBytes(thread);
        normalizeBatch(clean, root, writer, scratch);
        final long cleanBytes = bean.getThreadAllocatedBytes(thread) - cleanStart;
        final long dirtyStart = bean.getThreadAllocatedBytes(thread);
        normalizeBatch(dirty, root, writer, scratch);
        final long dirtyBytes = bean.getThreadAllocatedBytes(thread) - dirtyStart;
        final long controlStart = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < ITERATIONS; i++)
          sink = new byte[32];
        final long controlBytes = bean.getThreadAllocatedBytes(thread) - controlStart;
        assertEquals(0, cleanBytes, "a clean-root no-op must not allocate even traversal scratch");
        assertEquals(0, dirtyBytes, "visiting a resident dirty page without folding must allocate zero bytes");
        assertTrue(controlBytes >= ITERATIONS * 32L, "positive control must escape and allocate");
        assertSame(root, writer.loadHOTPage(dirty));
        assertEquals(1, root.getHeight());
        final AbstractHOTIndexWriter<byte[]> indexWriter = new AllocationWriter(writer);
        final HOTIndirectPage parent = HOTIndirectPage.createBiNode(103, 1, 0, dirty, ar, 2);
        final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(a, ar,
            new HOTIndirectPage[] {parent}, new PageReference[] {ref(parent)}, new int[] {0}, 1);
        for (int i = 0; i < ITERATIONS; i++) {
          parent.setHeightForWrite(1);
          REFRESH.invokeExact(indexWriter, route, 1, dirty, 0);
          parent.setHeightForWrite(3);
          REFRESH.invokeExact(indexWriter, route, 1, dirty, 2);
        }
        final long metadataStart = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < ITERATIONS; i++) {
          parent.setHeightForWrite(1);
          REFRESH.invokeExact(indexWriter, route, 1, dirty, 0);
          parent.setHeightForWrite(3);
          REFRESH.invokeExact(indexWriter, route, 1, dirty, 2);
        }
        assertEquals(0, bean.getThreadAllocatedBytes(thread) - metadataStart,
            "per-key growth and shrink propagation on a resident owned spine must add no allocation");
        assertEquals(2, parent.getHeight());
        trx.rollback();
      }
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  @Test
  void refusedFoldDoesNotAllocateOrPopulateTheLazyMaskCache() {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = new HOTLeafPage(1, 1, IndexType.CAS);
        HOTLeafPage b = new HOTLeafPage(2, 1, IndexType.CAS);
        HOTLeafPage c = new HOTLeafPage(3, 1, IndexType.CAS)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, 0, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, 0, ref(child), ref(c), 2);
      for (int i = 0; i < ITERATIONS; i++)
        sink = HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, scratch);
      final long start = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++)
        sink = HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, scratch);
      assertEquals(0, bean.getThreadAllocatedBytes(thread) - start,
          "a refused fold must allocate no scratch or output");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 72})
  void successfulFoldAllocatesOnlyItsRequiredPageOutput(final int childBit) {
    final ThreadMXBean bean = allocationBean();
    final long thread = Thread.currentThread().threadId();
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = new HOTLeafPage(1, 1, IndexType.CAS);
        HOTLeafPage b = new HOTLeafPage(2, 1, IndexType.CAS);
        HOTLeafPage c = new HOTLeafPage(3, 1, IndexType.CAS)) {
      final PageReference[] children = {ref(a), ref(b), ref(c)};
      final byte[] positions = {0, 9};
      final long[] masks = {0x8080000000000000L};
      final int[] partials = {0, 1, 2};
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, childBit, children[0], children[1], 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, 0, ref(child), children[2], 2);
      for (int i = 0; i < ITERATIONS; i++) {
        sink = HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, scratch);
        sink = ownedOutput(childBit, positions, masks, partials, children);
      }
      final long outputStart = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++)
        sink = ownedOutput(childBit, positions, masks, partials, children);
      final long outputBytes = bean.getThreadAllocatedBytes(thread) - outputStart;
      final long foldStart = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++)
        sink = HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, scratch);
      final long foldBytes = bean.getThreadAllocatedBytes(thread) - foldStart;
      assertTrue(outputBytes >= ITERATIONS * 32L, "required output must escape and be counted");
      // Established factories are an independent allocating control, but may also allocate a
      // temporary shuffle array depending on JVM escape analysis. The fold's exact graph budget
      // excludes that temporary workspace on every JVM.
      final long pinnedGraphBytes = childBit == 72
          ? 352L
          : 240L;
      assertTrue(outputBytes >= pinnedGraphBytes * ITERATIONS,
          "independent factory must allocate at least the required output graph");
      assertEquals(pinnedGraphBytes * ITERATIONS, foldBytes,
          "each accepted fold allocates exactly one owned graph and zero workspace");
      assertTrue(foldBytes <= outputBytes,
          "a fold may allocate at most its required page output: fold=" + foldBytes + " output=" + outputBytes);
      assertEquals(3, ((HOTIndirectPage) sink).getNumChildren());
    }
  }

  private static void normalizeBatch(final PageReference reference, final HOTIndirectPage root,
      final StorageEngineWriter writer, final HOTCommitNormalizer.Scratch scratch) {
    for (int i = 0; i < ITERATIONS; i++) {
      sink = HOTCommitNormalizer.normalize(reference, root, writer, scratch);
    }
  }

  private static HOTIndirectPage ownedOutput(final int childBit, final byte[] positions, final long[] masks,
      final int[] partials, final PageReference[] children) {
    return childBit == 72
        ? HOTIndirectPage.createSpanNodeMultiMask(10, 1, positions, masks, 2, partials, children, 1, (short) 0)
        : HOTIndirectPage.createSpanNode(10, 1, 0, 0xC000000000000000L, partials, children, 1);
  }

  private static MethodHandle refreshHandle() {
    try {
      return MethodHandles.privateLookupIn(AbstractHOTIndexWriter.class, MethodHandles.lookup())
                          .findVirtual(AbstractHOTIndexWriter.class, "refreshOuterHeight",
                              MethodType.methodType(void.class, AbstractHOTIndexWriter.LeafNavigationResult.class,
                                  int.class, PageReference.class, int.class));
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static final class AllocationWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private AllocationWriter(final StorageEngineWriter writer) {
      super(writer, IndexType.CAS, 0);
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
    protected void prepareIndexPage() {}
  }

  private static ThreadMXBean allocationBean() {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    return bean;
  }

  private static PageReference ref(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }
}
