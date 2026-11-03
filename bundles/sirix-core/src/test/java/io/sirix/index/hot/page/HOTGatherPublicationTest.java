package io.sirix.index.hot.page;

import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class HOTGatherPublicationTest {
  @Test
  void concurrentFirstReadersSeeCompleteGatherState() throws Exception {
    final int readers = 4;
    final long constructionsBefore = HOTIndirectPage.gatherConstructionCount();
    final HOTIndirectPage[] pages = new HOTIndirectPage[512];
    for (int round = 0; round < pages.length; round++) {
      final PageReference[] children = new PageReference[4];
      for (int child = 0; child < children.length; child++) {
        children[child] = new PageReference();
        children[child].setKey(child + 1);
      }
      pages[round] = HOTIndirectPage.createSpanNodeMultiMask(round + 1, 1, new byte[] {0, 10},
          new long[] {(0x10L << 56) | (0x04L << 48)}, 2, new int[] {0, 1, 2, 3}, children, 1, (short) 3);
    }

    // Eager construction: every multi-mask page built its shuffle before it existed anywhere else,
    // copies of an unchanged layout share it, and no lookup builds anything.
    final long built = HOTIndirectPage.gatherConstructionCount() - constructionsBefore;
    assertEquals(pages.length, built, "one shuffle per created multi-mask page");
    final HOTIndirectPage[] copies = new HOTIndirectPage[pages.length];
    for (int round = 0; round < pages.length; round++) {
      copies[round] = round % 2 == 0
          ? new HOTIndirectPage(pages[round])
          : pages[round].copyWithNewPageKey(100_000L + round, 2);
    }
    assertEquals(built, HOTIndirectPage.gatherConstructionCount() - constructionsBefore,
        "copy-on-write copies share the immutable gather state");
    final Phaser firstRead = new Phaser(readers);
    try (var executor = Executors.newFixedThreadPool(readers)) {
      final Future<?>[] results = new Future<?>[readers];
      for (int reader = 0; reader < readers; reader++) {
        final int expected = reader;
        results[reader] = executor.submit(() -> {
          final byte[] key = new byte[11];
          key[0] = (byte) ((expected & 2) == 0
              ? 0
              : 0x10);
          key[10] = (byte) ((expected & 1) == 0
              ? 0
              : 0x04);
          try {
            for (int round = 0; round < pages.length; round++) {
              // Every round races the first read of a fresh, shared immutable page and its copy.
              if (firstRead.arriveAndAwaitAdvance() < 0) {
                return;
              }
              assertEquals(expected, pages[round].computeDensePartialKey(key));
              assertEquals(expected, copies[round].computeDensePartialKey(key));
            }
          } catch (final RuntimeException | Error failure) {
            firstRead.forceTermination();
            throw failure;
          }
        });
      }
      for (final Future<?> result : results) {
        result.get();
      }
    }
    assertEquals(built, HOTIndirectPage.gatherConstructionCount() - constructionsBefore,
        "lookups never construct gather state");
  }
}
