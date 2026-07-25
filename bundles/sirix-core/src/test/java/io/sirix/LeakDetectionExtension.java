package io.sirix;

import io.sirix.access.Databases;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Set;

/**
 * JUnit 5 Extension that detects memory leaks (unclosed pages) after each test.
 * <p>
 * Unlike the JUnit 4 Rule, this extension's {@code afterEach} runs AFTER {@code @AfterEach}, so it
 * properly detects leaks after the test's cleanup (e.g., database close) has completed.
 * <p>
 * Usage:
 * 
 * <pre>
 * {
 *   &#64;code
 *   &#64;ExtendWith(LeakDetectionExtension.class)
 *   class MyTest {
 *     @AfterEach
 *     void tearDown() {
 *       holder.close(); // Runs BEFORE leak detection
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * This extension: 1. Takes a snapshot of live pages before each test 2. Runs AFTER @AfterEach
 * (after database cleanup) 3. Clears caches and checks for leaked pages 4. Fails the test if real
 * leaks are detected (when -Dsirix.debug.memory.leaks=true)
 *
 * @author Johannes Lichtenberger
 */
public class LeakDetectionExtension implements BeforeEachCallback, AfterEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(LeakDetectionExtension.class);

  /** An identity-keyed snapshot: page equality merges distinct instances, which fakes leaks. */
  private static Set<KeyValueLeafPage> identitySetOf(final Set<KeyValueLeafPage> source) {
    final Set<KeyValueLeafPage> identitySet =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    identitySet.addAll(source);
    return identitySet;
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    if (!KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      return;
    }

    // Store baseline in the extension context (thread-safe per test)
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    store.put("pagesCreatedBefore", KeyValueLeafPage.PAGES_CREATED.get());
    store.put("pagesClosedBefore", KeyValueLeafPage.PAGES_CLOSED.get());
    // Per-test baseline for the collected-without-close counter too. It is a CUMULATIVE process
    // counter: comparing it against zero made every test after the first genuine leak fail, blaming
    // whichever test happened to run next.
    store.put("pagesFinalizedBefore", KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get());
    // Identity, not equals: KeyValueLeafPage overrides equals/hashCode, so a HashSet baseline merges
    // distinct instances that share a page key and revision — and every instance it merged away then
    // looks "new" after the test, i.e. a fabricated leak.
    store.put("livePagesBefore", identitySetOf(KeyValueLeafPage.ALL_LIVE_PAGES));
  }

  @Override
  public void afterEach(ExtensionContext context) {
    // This runs AFTER @AfterEach, so database should already be closed

    // Free any remaining cached pages — close no longer clears caches (they stay warm for
    // production reopens), so leak detection must drop them explicitly or legitimately
    // cached pages would be reported as leaks.
    Databases.freeAllocatedMemory();
    Databases.clearGlobalCaches();

    if (!KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      return;
    }

    // Force GC to trigger any finalizers for leaked pages
    System.gc();
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    System.gc();

    // Retrieve baseline from context
    ExtensionContext.Store store = context.getStore(NAMESPACE);
    long pagesCreatedBefore = store.get("pagesCreatedBefore", Long.class);
    long pagesClosedBefore = store.get("pagesClosedBefore", Long.class);
    @SuppressWarnings("unchecked")
    Set<KeyValueLeafPage> livePagesBefore = store.get("livePagesBefore", Set.class);

    // Calculate stats
    long pagesFinalizedBefore = store.get("pagesFinalizedBefore", Long.class);

    long pagesCreated = KeyValueLeafPage.PAGES_CREATED.get() - pagesCreatedBefore;
    long pagesClosed = KeyValueLeafPage.PAGES_CLOSED.get() - pagesClosedBefore;

    // Find pages that are NEW in this test (weren't live before test started)
    Set<KeyValueLeafPage> newLeakedPages = identitySetOf(java.util.Collections.emptySet());
    for (KeyValueLeafPage page : KeyValueLeafPage.ALL_LIVE_PAGES) {
      if (!livePagesBefore.contains(page)) {
        newLeakedPages.add(page);
      }
    }

    int newLeaksCount = newLeakedPages.size();
    long finalized = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get() - pagesFinalizedBefore;

    if (newLeaksCount > 0 || finalized > 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("\n\n=== PAGE LEAK DETECTED in ").append(context.getDisplayName()).append(" ===\n");
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
          sb.append("  - pageKey=")
            .append(page.getPageKey())
            .append(", type=")
            .append(page.getIndexType())
            .append(", rev=")
            .append(page.getRevision())
            .append(", guardCount=")
            .append(page.getGuardCount())
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

      // Retire the leaked pages so the next test starts clean. retire(), not a guard drain: draining
      // forges a release for whoever actually holds the page, and this runs while other tests' work
      // may still be in flight. The orphan bit frees an unguarded page now and a guarded one at its
      // holder's last release.
      for (KeyValueLeafPage page : newLeakedPages) {
        if (!page.isClosed()) {
          page.retire();
        }
      }

      throw new AssertionError(sb.toString());
    }
  }
}


