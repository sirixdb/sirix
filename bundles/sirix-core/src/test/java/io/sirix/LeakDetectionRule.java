package io.sirix;

import io.sirix.access.Databases;
import io.sirix.page.KeyValueLeafPage;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.HashSet;
import java.util.Set;

/**
 * JUnit Rule that detects memory leaks (unclosed pages) after each test.
 * <p>
 * Usage:
 * <pre>{@code
 * @Rule
 * public LeakDetectionRule leakDetection = new LeakDetectionRule();
 * }</pre>
 * <p>
 * This rule:
 * 1. Clears caches after each test to ensure pages are properly released
 * 2. Checks for leaked pages (pages created but not closed)
 * 3. Fails the test if leaks are detected (when -Dsirix.debug.memory.leaks=true)
 *
 * @author Johannes Lichtenberger
 */
public class LeakDetectionRule extends TestWatcher {

  private long pagesCreatedBefore;
  private long pagesClosedBefore;
  private Set<KeyValueLeafPage> livePagesBefore;

  @Override
  protected void starting(Description description) {
    // Capture baseline counters before test
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      pagesCreatedBefore = KeyValueLeafPage.PAGES_CREATED.get();
      pagesClosedBefore = KeyValueLeafPage.PAGES_CLOSED.get();
      // Snapshot live pages before test to identify new leaks
      livePagesBefore = new HashSet<>(KeyValueLeafPage.ALL_LIVE_PAGES);
    }
  }

  @Override
  protected void finished(Description description) {
    // Free caches to release any remaining pages
    Databases.freeAllocatedMemory();

    if (!KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      return; // Leak tracking not enabled
    }

    // Force GC to trigger any finalizers for leaked pages
    System.gc();
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    System.gc();

    // Check for leaks - only count NEW pages that are still live (not pages from previous tests)
    long pagesCreated = KeyValueLeafPage.PAGES_CREATED.get() - pagesCreatedBefore;
    long pagesClosed = KeyValueLeafPage.PAGES_CLOSED.get() - pagesClosedBefore;
    
    // Find pages that are NEW in this test (weren't live before test started)
    Set<KeyValueLeafPage> newLeakedPages = new HashSet<>();
    for (KeyValueLeafPage page : KeyValueLeafPage.ALL_LIVE_PAGES) {
      if (!livePagesBefore.contains(page)) {
        newLeakedPages.add(page);
      }
    }
    
    int newLeaksCount = newLeakedPages.size();
    long finalized = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();

    if (newLeaksCount > 0 || finalized > 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("\n\n=== PAGE LEAK DETECTED in ").append(description.getDisplayName()).append(" ===\n");
      sb.append("Pages created in test: ").append(pagesCreated).append("\n");
      sb.append("Pages closed in test: ").append(pagesClosed).append("\n");
      sb.append("NEW leaked pages in this test: ").append(newLeaksCount).append("\n");
      sb.append("Pages finalized without close: ").append(finalized).append("\n");

      if (newLeaksCount > 0) {
        sb.append("\nLEAKED PAGES (first 10):\n");
        int count = 0;
        for (KeyValueLeafPage page : newLeakedPages) {
          if (count++ >= 10) {
            sb.append("  ... and ").append(newLeaksCount - 10).append(" more\n");
            break;
          }
          sb.append("  - pageKey=").append(page.getPageKey())
            .append(", type=").append(page.getIndexType())
            .append(", rev=").append(page.getRevision())
            .append(", guardCount=").append(page.getGuardCount())
            .append("\n");

          if (page.getCreationStackTrace() != null) {
            sb.append("    Created at:\n");
            StackTraceElement[] stack = page.getCreationStackTrace();
            for (int i = 2; i < Math.min(stack.length, 6); i++) {
              sb.append("      at ").append(stack[i]).append("\n");
            }
          }
        }
      }

      // Force-close leaked pages to allow subsequent tests to run
      for (KeyValueLeafPage page : newLeakedPages) {
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        if (!page.isClosed()) {
          page.close();
        }
      }

      throw new AssertionError(sb.toString());
    }
  }
}

