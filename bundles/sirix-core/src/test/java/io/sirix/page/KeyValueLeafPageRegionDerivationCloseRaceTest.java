/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Closed-page ownership regressions for the two-phase slotted-page region builders. */
final class KeyValueLeafPageRegionDerivationCloseRaceTest {

  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  @ParameterizedTest(name = "{0} builder cannot install after its last guard closes an orphan")
  @EnumSource(Derivation.class)
  void orphanedDuringWalkCannotInstallARegionTable(final Derivation derivation) {
    final FrameSlotAllocator regionAllocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final long regionBytesBefore = regionAllocator.getActiveMemoryBytes();

    try (TrackingAllocator pageAllocator = new TrackingAllocator()) {
      final ResourceConfiguration config = new ResourceConfiguration.Builder("region-close-race").build();
      final KeyValueLeafPage page =
          spy(new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false, pageAllocator, null));
      writeAllDerivedKinds(page);
      final AtomicInteger interceptedReleases = new AtomicInteger();
      doAnswer(invocation -> {
        interceptedReleases.incrementAndGet();
        assertEquals(1, page.getGuardCount(), "the hook must run at the builder's final-guard release boundary");
        assertFalse(page.isClosed());
        page.markOrphaned();
        return invocation.callRealMethod();
      }).when(page).releaseGuard();

      Object result = null;
      RegionTable tableAfterDerivation = null;
      long regionBytesAfter = Long.MIN_VALUE;
      try {
        result = derivation.derive(page);
        tableAfterDerivation = page.getRegionTable();
        regionBytesAfter = regionAllocator.getActiveMemoryBytes();

        final Object derivedResult = result;
        final RegionTable installedTable = tableAfterDerivation;
        final long retainedRegionBytes = regionBytesAfter;
        assertAll(
            () -> assertNull(derivedResult, "a builder whose guard closed the page must return no payload/header"),
            () -> assertTrue(page.isClosed(), "the orphan's final guard must close the page"),
            () -> assertEquals(0, page.getGuardCount()),
            () -> assertNull(installedTable, "a closed page must never regain RegionTable ownership"),
            () -> assertEquals(1, interceptedReleases.get(), "each builder owns exactly one derivation guard"),
            () -> assertEquals(pageAllocator.allocationCount(), pageAllocator.releaseCount(),
                "closing the orphan must return every page-frame allocation"),
            () -> assertEquals(0, pageAllocator.liveAllocationCount()), () -> assertEquals(regionBytesBefore,
                retainedRegionBytes, "a rejected install must not retain a pooled RegionTable payload allocation"));
      } finally {
        // A failing pre-fix run may have minted a table after close(). Release that orphaned
        // ownership so one parameter cannot contaminate the allocator accounting of the next.
        if (tableAfterDerivation != null) {
          tableAfterDerivation.close();
        }
        if (!page.isClosed()) {
          page.close();
        }
      }
    }
  }

  private static void writeAllDerivedKinds(final KeyValueLeafPage page) {
    final ObjectNamedNumberNode number = new ObjectNamedNumberNode(0L, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 11, -1L, 0, 0, 0L, 42L,
        HASH_FUNCTION, (byte[]) null);
    number.setWriteSingleton(true);
    page.serializeNewRecord(number, 0L, slot(0L));

    final ObjectNamedStringNode string = new ObjectNamedStringNode(1L, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 12, -1L, 0, 0, 0L,
        "value".getBytes(StandardCharsets.UTF_8), HASH_FUNCTION, (byte[]) null, false, null);
    string.setWriteSingleton(true);
    page.serializeNewRecord(string, 1L, slot(1L));

    final ObjectNamedBooleanNode bool = new ObjectNamedBooleanNode(2L, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 13, -1L, 0, 0, 0L, true,
        HASH_FUNCTION, (byte[]) null);
    bool.setWriteSingleton(true);
    page.serializeNewRecord(bool, 2L, slot(2L));
  }

  private static int slot(final long nodeKey) {
    return (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
  }

  private enum Derivation {
    NUMBER {
      @Override
      Object derive(final KeyValueLeafPage page) {
        return page.getNumberRegionHeader();
      }
    },
    STRING {
      @Override
      Object derive(final KeyValueLeafPage page) {
        return page.getStringRegionHeader();
      }
    },
    NAMES {
      @Override
      Object derive(final KeyValueLeafPage page) {
        page.ensureRegionsFor(RegionTable.maskOf(RegionTable.KIND_OBJECT_KEY_NAMEKEY));
        return page.regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
      }
    },
    BOOLEAN {
      @Override
      Object derive(final KeyValueLeafPage page) {
        return page.getBooleanRegionPayload();
      }
    };

    abstract Object derive(KeyValueLeafPage page);
  }

  /** Confined test allocator with exact ownership accounting; no process allocator reservation. */
  private static final class TrackingAllocator implements MemorySegmentAllocator, AutoCloseable {
    private final Arena arena = Arena.ofConfined();
    private final Set<Long> liveAddresses = new HashSet<>();
    private int allocationCount;
    private int releaseCount;

    @Override
    public void init(final long maxBufferSize) {
      // Already initialized by construction.
    }

    @Override
    public boolean isInitialized() {
      return true;
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("the test owns this allocator through close()");
    }

    @Override
    public MemorySegment allocate(final long size) {
      final MemorySegment segment = arena.allocate(size, Long.BYTES);
      if (!liveAddresses.add(segment.address())) {
        throw new IllegalStateException("test allocator returned an already-live address");
      }
      allocationCount++;
      return segment;
    }

    @Override
    public void release(final MemorySegment segment) {
      if (!liveAddresses.remove(segment.address())) {
        throw new IllegalStateException("test allocator released an unknown address");
      }
      releaseCount++;
    }

    @Override
    public long getMaxBufferSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public long getPhysicalMemoryBytes() {
      return liveAddresses.size() * (long) PageLayout.INITIAL_PAGE_SIZE;
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      segment.fill((byte) 0);
    }

    int allocationCount() {
      return allocationCount;
    }

    int releaseCount() {
      return releaseCount;
    }

    int liveAllocationCount() {
      return liveAddresses.size();
    }

    @Override
    public void close() {
      arena.close();
    }
  }
}
