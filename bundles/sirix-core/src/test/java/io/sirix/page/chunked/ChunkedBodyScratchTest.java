/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import io.sirix.exception.SirixIOException;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.chunked.ChunkedPageGenerator.Body;
import io.sirix.page.chunked.ChunkedPageGenerator.Hash;
import io.sirix.page.chunked.ChunkedPageGenerator.Names;
import io.sirix.page.chunked.ChunkedPageGenerator.ParentKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.PathKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.Recipe;
import io.sirix.page.chunked.ChunkedPageGenerator.Shape;
import io.sirix.page.chunked.ChunkedPageGenerator.Sizes;
import io.sirix.page.chunked.ChunkedPageGenerator.Values;
import io.sirix.page.chunked.ChunkedPageHarness.ChunkedLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The record-page codec keeps no native memory per thread: the staging a body is assembled in or
 * decompressed into, and the scratch a lazily read chunk is expanded through, are borrowed from the
 * allocator for one page or one chunk and returned with it, also when the page is refused.
 *
 * <p>
 * These used to be thread-local segments of an automatic arena: 128 KiB twice for the body codec
 * and 64 KiB for chunk expansion, per thread, for as long as the thread lived and outside every
 * budget. The assertions are deltas of the process allocator's counters for
 * {@link ScratchUse#CODEC} and of its lent-buffer gauge, so the class runs isolated; nothing
 * measures time.
 */
@Isolated
@DisplayName("Chunked body scratch")
final class ChunkedBodyScratchTest {

  private boolean previouslyEnabled;
  private int previousTarget;

  @BeforeEach
  void setUp() {
    Allocators.getInstance().init(2L * 1024 * 1024 * 1024);
    previouslyEnabled = ChunkedBodyConfig.setEnabledForTesting(false);
    previousTarget = ChunkedBodyConfig.targetChunkBytes();
    // A small target over 200 records: several chunks, so "one borrow per chunk" is not "one borrow".
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(previousTarget);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.CODEC);
  }

  private static Recipe recipe() {
    return new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.SEQUENTIAL, PathKeys.FEW, Values.MIXED, Names.WIDE,
        Shape.ALTERNATING_HOLES, Sizes.MIXED, 200, false);
  }

  @Test
  @DisplayName("encoding, decoding and expanding a page borrow their staging and return it with the page")
  void aPagesStagingIsBorrowedAndReturned() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    // Warm: the first encode, decode and expansion on this thread make the allocator back the buffers.
    final MemorySegment warmWire = ChunkedPageHarness.serializeChunked(config, recipe());
    ChunkedPageHarness.deserialize(config, warmWire).close();
    final KeyValueLeafPage warmLazy = ChunkedPageHarness.deserializeLazily(config, warmWire);
    try {
      warmLazy.ensureAllChunks();
    } finally {
      warmLazy.close();
    }

    final ScratchStats start = stats();
    final long lent = start.totalLentBuffers();

    final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe());
    final ScratchStats encoded = stats();
    assertTrue(borrows(encoded) > borrows(start), "the encoder stages the body in borrowed scratch");
    assertEquals(lent, encoded.totalLentBuffers(), "the encode returned its staging");

    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    assertTrue(layout.chunkCount >= 3, "the recipe framed only " + layout.chunkCount + " chunks");

    final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
    final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
    try {
      final ScratchStats decoded = stats();
      assertTrue(borrows(decoded) >= borrows(encoded) + 2, "each decode stages the body in borrowed scratch");
      assertEquals(lent, decoded.totalLentBuffers(),
          "both pages are open and hold no scratch: a decode copies what it keeps into the page's frame");

      lazy.ensureAllChunks();
      final ScratchStats expanded = stats();
      assertEquals(layout.chunkCount, borrows(expanded) - borrows(decoded),
          "one borrow per chunk expanded, returned before the next");
      assertEquals(lent, expanded.totalLentBuffers(), "the expansions returned their scratch");
      ChunkedPageHarness.assertSameSlottedPage(eager, lazy, "expanded through borrowed scratch");

      assertEquals(0, expanded.totalCreated() - start.totalCreated(),
          "a warm thread encodes, decodes and expands through the buffers the allocator kept resident");
      assertEquals(0, expanded.staleReleases() - start.staleReleases());
      assertEquals(0, expanded.refusals() - start.refusals());
    } finally {
      eager.close();
      lazy.close();
    }
  }

  @Test
  @DisplayName("a page refused while decoding or expanding returns its staging")
  void aRefusedPageReturnsItsStaging() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe());
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    assertTrue(layout.chunkCount >= 3, "the recipe framed only " + layout.chunkCount + " chunks");
    // One flipped bit in the last chunk's payload: the META section still decodes, the chunk does not.
    final MemorySegment damaged = Arena.ofAuto().allocate(wire.byteSize());
    MemorySegment.copy(wire, 0, damaged, 0, wire.byteSize());
    final long victim = layout.chunkPayloadOffset[layout.chunkCount - 1];
    damaged.set(ValueLayout.JAVA_BYTE, victim, (byte) (damaged.get(ValueLayout.JAVA_BYTE, victim) ^ 0x01));

    final ScratchStats start = stats();
    final long lent = start.totalLentBuffers();

    assertThrows(SirixIOException.class, () -> ChunkedPageHarness.deserialize(config, damaged));
    final ScratchStats refused = stats();
    assertTrue(borrows(refused) > borrows(start), "the refused decode had borrowed its staging");
    assertEquals(lent, refused.totalLentBuffers(), "and returned it on the way out");

    final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, damaged);
    try {
      assertThrows(SirixIOException.class, lazy::ensureAllChunks);
      final ScratchStats expanded = stats();
      assertTrue(borrows(expanded) > borrows(refused), "the refused expansion had borrowed its scratch");
      assertEquals(lent, expanded.totalLentBuffers(), "and returned it on the way out");
      assertEquals(0, expanded.staleReleases() - start.staleReleases());
    } finally {
      lazy.close();
    }
  }
}
