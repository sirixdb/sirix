package io.sirix.cache;

import io.sirix.index.projection.GroupWorkspace;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated
final class GroupWorkspaceCleanupTest {
  @Test
  void failedRegionReturnStillReturnsEveryOtherRegionAndTheGrant() {
    final String previous = System.setProperty("sirix.allocator.scratchIdleBuffers", "1");
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    final GroupWorkspace workspace = GroupWorkspace.forScan(allocator);
    final AtomicInteger publications = new AtomicInteger();
    final OutOfMemoryError failure = new OutOfMemoryError("injected spare publication failure");
    try {
      for (int i = 0; i < 3; i++) {
        workspace.newBlock(262144);
      }
      allocator.scratch().setSparePublicationHookForTesting(() -> {
        if (publications.incrementAndGet() == 1) {
          throw failure;
        }
      });
      assertSame(failure, assertThrows(OutOfMemoryError.class, workspace::close));
      assertEquals(2, publications.get(), "cleanup continued through the last region");
      assertEquals(0, allocator.scratchStats().lentBytes());
      assertEquals(0, allocator.aggregationBudget().grantedBytes());
      allocator.scratch().setSparePublicationHookForTesting(null);
      try (GroupWorkspace following = GroupWorkspace.forScan(allocator)) {
        following.newBlock(262144);
      }
      assertEquals(0, allocator.scratchStats().lentBytes());
      assertEquals(0, allocator.aggregationBudget().grantedBytes());
    } finally {
      allocator.scratch().setSparePublicationHookForTesting(null);
      workspace.close();
      allocator.shutdown();
      if (previous == null) {
        System.clearProperty("sirix.allocator.scratchIdleBuffers");
      } else {
        System.setProperty("sirix.allocator.scratchIdleBuffers", previous);
      }
    }
  }
}
