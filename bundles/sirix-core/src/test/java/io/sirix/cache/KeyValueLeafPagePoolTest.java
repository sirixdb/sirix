package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class KeyValueLeafPagePoolTest {

  private KeyValueLeafPagePool pagePool;
  private final ResourceConfiguration resourceConfig = new ResourceConfiguration.Builder("test").build();

  @BeforeEach
  public void setUp() {
    pagePool = KeyValueLeafPagePool.getInstance();
    // Ensure the pool is initialized before running tests
    pagePool.init(1 << 30); // Set max buffer size to 1GB
  }

  @AfterEach
  public void tearDown() {
    // Clean up after each test to prevent interference
    pagePool.free();
  }

  @Test
  public void testBorrowAndReturnPage() {
    KeyValueLeafPage page = pagePool.borrowPage(4096, 1, IndexType.DOCUMENT, resourceConfig, 1);
    assertNotNull(page, "Borrowed page should not be null");
    assertEquals(4096, page.getSlotMemoryByteSize(), "Page size should be 4096 bytes");

    pagePool.returnPage(page);

    // Borrow the page again and check if it's reused
    KeyValueLeafPage reusedPage = pagePool.borrowPage(4096, 1, IndexType.PATH, resourceConfig, 2);
    assertNotNull(reusedPage, "Reused page should not be null");
    assertEquals(4096, reusedPage.getSlotMemoryByteSize(), "Reused page size should be 4096 bytes");
    assertEquals(page.getPageKey(), reusedPage.getPageKey(), "Reused page should have the same key as the original");
  }

  @Test
  public void testBorrowSize8AndReturnPage() {
    KeyValueLeafPage page = pagePool.borrowPage(8, 1, IndexType.DOCUMENT, resourceConfig, 1);
    assertNotNull(page, "Borrowed page should not be null");
    assertEquals(4096, page.getSlotMemoryByteSize(), "Page size should be 4096 bytes");

    pagePool.returnPage(page);

    // Borrow the page again and check if it's reused
    KeyValueLeafPage reusedPage = pagePool.borrowPage(8, 2, IndexType.PATH, resourceConfig, 2);
    assertNotNull(reusedPage, "Reused page should not be null");
    assertEquals(4096, reusedPage.getSlotMemoryByteSize(), "Reused page size should be 4096 bytes");
    assertEquals(2, reusedPage.getPageKey(), "Reused page should have the correct page key");
    assertEquals(IndexType.PATH, reusedPage.getIndexType(), "Reused page should have the correct index type");
  }

  @Test
  public void testBorrowPageWithUnsupportedSize() {
    // Trying to borrow a page with an unsupported size should throw an exception
    assertThrows(IllegalArgumentException.class,
                 () -> pagePool.borrowPage(550000, 1, IndexType.DOCUMENT, resourceConfig, 1),
                 "Should throw IllegalArgumentException for unsupported size");
  }

  @Test
  public void testBorrowPageWithZeroSize() {
    // Trying to borrow a page of zero size should throw an exception
    assertThrows(IllegalArgumentException.class,
                 () -> pagePool.borrowPage(0, 1, IndexType.DOCUMENT, resourceConfig, 1),
                 "Should throw IllegalArgumentException for zero size");
  }

  @Test
  public void testReturnPageWithUnsupportedSize() {
    KeyValueLeafPage page = pagePool.borrowPage(8192, 1, IndexType.DOCUMENT, resourceConfig, 1);

    // Allocate a page that doesn't fit into any existing pool size
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment segment = arena.allocate(500000);
      @SuppressWarnings("resource")
      KeyValueLeafPage unsupportedPage = new KeyValueLeafPage(1, IndexType.PATH, resourceConfig, 1, segment, null);

      // Return should not throw exception but log warning instead
      assertDoesNotThrow(() -> pagePool.returnPage(unsupportedPage),
                         "Return should handle unsupported page gracefully");

      // Clean up
      pagePool.returnPage(page);
    }
  }

  @Test
  public void testMultipleConcurrentBorrowAndReturn() throws InterruptedException {
    int numThreads = 10;
    int numIterations = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    Runnable task = () -> {
      for (int i = 0; i < numIterations; i++) {
        KeyValueLeafPage page = pagePool.borrowPage(16384, i, IndexType.DOCUMENT, resourceConfig, i);
        assertNotNull(page, "Borrowed page should not be null");
        pagePool.returnPage(page);
      }
      latch.countDown();
    };

    for (int i = 0; i < numThreads; i++) {
      executor.submit(task);
    }

    latch.await();
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate in time");
  }

  @Test
  public void testStressTestPool() {
    int allocations = 1000;
    for (int i = 0; i < allocations; i++) {
      KeyValueLeafPage page = pagePool.borrowPage(32768, i, IndexType.NAME, resourceConfig, i);
      assertNotNull(page, "Borrowed page should not be null");
      pagePool.returnPage(page);
    }
    // Verify that all allocations and returns were successful
    System.out.println("Stress test completed successfully.");
  }

  @Test
  public void testClearPageBeforeReturning() {
    KeyValueLeafPage page = pagePool.borrowPage(4096, 1, IndexType.DOCUMENT, resourceConfig, 1);
    assertNotNull(page, "Borrowed page should not be null");

    MemorySegment segment = page.slots();
    segment.set(java.lang.foreign.ValueLayout.JAVA_BYTE, 0, (byte) 42);

    pagePool.returnPage(page);

    KeyValueLeafPage reusedPage = pagePool.borrowPage(4096, 2, IndexType.PATH, resourceConfig, 2);
    MemorySegment reusedSegment = reusedPage.slots();

    byte value = reusedSegment.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    assertEquals(0, value, "Page content should be cleared before reuse");
  }

  @Test
  public void testPoolInitialization() {
    assertTrue(pagePool.isInitialized(), "Pool should be initialized after setUp");
    assertFalse(pagePool.isShutdown(), "Pool should not be shutdown after setUp");
  }

  @Test
  public void testPoolStatistics() {
    var stats = pagePool.getDetailedStatistics();
    assertNotNull(stats, "Statistics should not be null");
    assertTrue(stats.containsKey("totalPages"), "Statistics should contain totalPages");
    assertTrue(stats.containsKey("isInitialized"), "Statistics should contain isInitialized");
  }
}