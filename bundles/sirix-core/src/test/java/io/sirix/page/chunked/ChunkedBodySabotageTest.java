/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
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

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Damage every part of a chunked body and require the reader to say so.
 *
 * <p>
 * The failure mode this guards against is not a crash but a quiet one: a chunk table that disagrees
 * with the page it describes hands a record's bytes to some other record's slot, and every read
 * after that is confidently wrong. So each sabotage below asserts two things — that the read fails,
 * and that the message names the frame or the field that failed, because an unattributable error on
 * a 26 KB page is only marginally better than no error.
 *
 * <p>
 * The parent reference's whole-payload hash already covers all of this on the real read path. These
 * checks are what stands behind it once a reader fetches parts of a page rather than the whole of
 * it, and what turns "the page did not parse" into "chunk 3 failed its checksum".
 */
@DisplayName("Chunked body sabotage")
final class ChunkedBodySabotageTest {

  private boolean previouslyEnabled;
  private int previousTarget;
  private ResourceConfiguration config;

  @BeforeEach
  void setUp() {
    Allocators.getInstance().init(256L * 1024 * 1024);
    previouslyEnabled = ChunkedBodyConfig.setEnabledForTesting(false);
    previousTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(512);
    config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(previousTarget);
  }

  @Test
  @DisplayName("a flipped bit in any chunk payload names that chunk")
  void everyChunkPayloadIsChecksummed() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    assertTrue(layout.chunkCount >= 3, "this suite wants a page of several chunks, got " + layout.chunkCount);
    for (int chunk = 0; chunk < layout.chunkCount; chunk++) {
      final int victim = (int) layout.chunkPayloadOffset[chunk];
      final int index = chunk;
      assertRefused(wire, "chunk " + chunk + " payload", bytes -> bytes[victim] ^= 0x01, "chunk " + index, "checksum");
    }
  }

  @Test
  @DisplayName("a flipped bit in the META payload names the META frame")
  void metaPayloadIsChecksummed() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int victim = (int) layout.metaPayloadOffset;
    assertRefused(wire, "META payload", bytes -> bytes[victim] ^= 0x01, "META frame", "checksum");
  }

  /**
   * The checksum itself is as corruptible as the bytes it covers, and a reader that trusted a damaged
   * table would decode a chunk it had no reason to believe in.
   */
  @Test
  @DisplayName("a flipped bit in a chunk's stored checksum is caught too")
  void chunkChecksumFieldIsItselfChecked() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int victim = (int) ChunkedPageHarness.chunkHashOffset(layout, 1);
    assertRefused(wire, "chunk 1 checksum field", bytes -> bytes[victim] ^= 0x40, "chunk 1", "checksum");
  }

  @Test
  @DisplayName("a chunk table that covers the wrong number of entries is refused")
  void entryCountSumMustMatchThePage() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int entryCountAt = (int) layout.chunkRowOffset[layout.chunkCount - 1] + 2;
    final int declared = layout.chunkEntryCount[layout.chunkCount - 1];
    assertRefused(wire, "last chunk covering one entry too few", bytes -> writeShort(bytes, entryCountAt, declared - 1),
        "chunk table covers", "entries");
  }

  @Test
  @DisplayName("chunk entry ranges that overlap are refused")
  void entryRangesMustBeContiguous() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int firstEntryAt = (int) layout.chunkRowOffset[1];
    final int declared = layout.chunkFirstEntry[1];
    assertRefused(wire, "chunk 1 starting one entry early", bytes -> writeShort(bytes, firstEntryAt, declared - 1),
        "chunk 1", "expected");
  }

  @Test
  @DisplayName("a chunk table that covers the wrong number of heap bytes is refused")
  void rawLengthSumMustMatchTheHeader() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int rawLenAt = (int) layout.chunkRowOffset[0] + 2 + 2;
    final int declared = layout.chunkRawLen[0];
    assertRefused(wire, "chunk 0 claiming one heap byte too many", bytes -> writeInt(bytes, rawLenAt, declared + 1),
        "heap bytes", "the header says");
  }

  @Test
  @DisplayName("a codec id no writer emits is refused rather than guessed at")
  void unknownCodecIsRefused() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int codecAt = (int) layout.chunkRowOffset[0] + 2 + 2 + 4 + 4;
    assertRefused(wire, "chunk 0 with codec 7", bytes -> bytes[codecAt] = 7, "unsupported codec 7", "chunk 0");
  }

  @Test
  @DisplayName("a body length that disagrees with the frames is refused")
  void bodyLengthMustMatchTheFrames() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    // bodyTotalLen sits immediately ahead of the META frame header.
    final int bodyLenAt = (int) (layout.chunkTableOffset - ChunkedBodyConfig.META_FRAME_HEADER_BYTES) - 4;
    assertRefused(wire, "a body one byte shorter than its frames",
        bytes -> writeInt(bytes, bodyLenAt, layout.bodyTotalLen - 1), "frames occupy", "declares");
  }

  @Test
  @DisplayName("a populated page claiming no chunks is refused")
  void chunkCountMustCoverThePage() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final int chunkCountAt = (int) layout.chunkTableOffset;
    assertRefused(wire, "a populated page with a zero chunk count", bytes -> bytes[chunkCountAt] = 0,
        "covers 0 entries", "");
  }

  /**
   * A frame cut short cannot be checksummed, so this one is allowed to surface as whatever the read
   * of the missing bytes throws. What it may never do is return a page.
   */
  @Test
  @DisplayName("a body truncated mid-chunk never yields a page")
  void truncationNeverYieldsAPage() {
    final MemorySegment wire = victim();
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    final byte[] full = ChunkedPageHarness.toArray(wire);
    final int cutAt = (int) layout.chunkPayloadOffset[1] + 3;
    final byte[] truncated = new byte[cutAt];
    System.arraycopy(full, 0, truncated, 0, cutAt);

    KeyValueLeafPage decoded = null;
    try {
      decoded = ChunkedPageHarness.deserialize(config, MemorySegment.ofArray(truncated));
    } catch (final RuntimeException expected) {
      return;
    } finally {
      if (decoded != null) {
        decoded.close();
      }
    }
    fail("a truncated body decoded into a page");
  }

  /** Apply a mutation to a copy of the wire bytes and require an attributable refusal. */
  private void assertRefused(final MemorySegment wire, final String what, final Consumer<byte[]> sabotage,
      final String mustMention, final String mustAlsoMention) {
    final byte[] bytes = ChunkedPageHarness.toArray(wire);
    sabotage.accept(bytes);
    final SirixIOException thrown = assertThrows(SirixIOException.class,
        () -> ChunkedPageHarness.deserialize(config, MemorySegment.ofArray(bytes)).close(),
        what + ": the reader accepted a damaged body");
    final String message = thrown.getMessage();
    assertTrue(message.contains(mustMention),
        what + ": the failure must mention '" + mustMention + "', said: " + message);
    assertTrue(mustAlsoMention.isEmpty() || message.contains(mustAlsoMention),
        what + ": the failure must mention '" + mustAlsoMention + "', said: " + message);
  }

  /**
   * A page of several chunks whose records exercise the columns and both elision sections, so the
   * damage lands on a body with something in every section.
   */
  private MemorySegment victim() {
    final Recipe recipe = new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW,
        Values.MIXED, Names.WIDE, Shape.DENSE, Sizes.MIXED, 96, false);
    final KeyValueLeafPage page = ChunkedPageGenerator.build(recipe, config);
    try {
      return ChunkedPageHarness.serialize(config, page, true);
    } finally {
      page.close();
    }
  }

  private static void writeShort(final byte[] bytes, final int at, final int value) {
    bytes[at] = (byte) value;
    bytes[at + 1] = (byte) (value >>> 8);
  }

  private static void writeInt(final byte[] bytes, final int at, final int value) {
    bytes[at] = (byte) value;
    bytes[at + 1] = (byte) (value >>> 8);
    bytes[at + 2] = (byte) (value >>> 16);
    bytes[at + 3] = (byte) (value >>> 24);
  }
}
