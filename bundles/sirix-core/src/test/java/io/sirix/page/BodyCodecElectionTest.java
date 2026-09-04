/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The body codec election between probes.
 *
 * <p>
 * A record page serialized on a thread whose last probe elected zero-run must still be written with
 * LZ77 when LZ77 is smaller — the election only decides whether byte-run is encoded as well. The
 * kill switch {@code sirix.codecBakeoff.stickyOnly} is the mutation: under it the stale election is
 * written verbatim, which is what cost 5.1 % of leaf bytes on a 1M-row load.
 */
@DisplayName("Body codec election between probes")
final class BodyCodecElectionTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  private static final int CODEC_ZERO_RUN = 0;
  private static final int CODEC_LZ77 = 3;

  /** Fused records per kind on the fixture page: enough repeated record headers for LZ77 to win. */
  private static final int RECORDS_PER_KIND = 16;

  private Arena arena;
  private boolean stickyOnlyBefore;

  @BeforeAll
  static void requireTheInstrumentIsOn() {
    assertTrue(PageKind.sectionDiagEnabled(),
        "the section diagnostic is off: run with -Dsirix.pageSectionDiag=true. The codec counters this "
            + "suite asserts on read zero when the gate is off, so a passing run would prove nothing.");
  }

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    stickyOnlyBefore = PageKind.CODEC_BAKEOFF_STICKY_ONLY;
  }

  @AfterEach
  void tearDown() {
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = stickyOnlyBefore;
    PageKind.resetStickyCodecElectionForCurrentThread();
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("a record page after a stale zero-run election is written with LZ77, and is smaller for it")
  void recordPageIgnoresAStaleZeroRunElection() {
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = false;
    final long lz77Before = PageSectionDiag.codecPages(CODEC_LZ77);
    final long zeroRunBefore = PageSectionDiag.codecPages(CODEC_ZERO_RUN);
    final long comparedBytes = serializeRecordPageAfterElecting(CODEC_ZERO_RUN);
    assertEquals(1, PageSectionDiag.codecPages(CODEC_LZ77) - lz77Before,
        "between probes the page must be written with the codec that actually wins, not the stale election");
    assertEquals(0, PageSectionDiag.codecPages(CODEC_ZERO_RUN) - zeroRunBefore,
        "the stale zero-run election must not be written");

    // The same page under the old rule, so the witness is a byte count and not only a codec id.
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = true;
    final long electedOnlyBytes = serializeRecordPageAfterElecting(CODEC_ZERO_RUN);
    assertTrue(comparedBytes < electedOnlyBytes, "comparing must emit fewer bytes than the stale election: compared="
        + comparedBytes + " B, elected-only=" + electedOnlyBytes + " B");
  }

  @Test
  @DisplayName("the kill switch writes the elected codec alone (the mutation)")
  void killSwitchWritesTheStaleElection() {
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = true;
    final long lz77Before = PageSectionDiag.codecPages(CODEC_LZ77);
    final long zeroRunBefore = PageSectionDiag.codecPages(CODEC_ZERO_RUN);
    serializeRecordPageAfterElecting(CODEC_ZERO_RUN);
    assertEquals(1, PageSectionDiag.codecPages(CODEC_ZERO_RUN) - zeroRunBefore,
        "under the old rule the stale zero-run election is written verbatim");
    assertEquals(0, PageSectionDiag.codecPages(CODEC_LZ77) - lz77Before);
  }

  @Test
  @DisplayName("a probe page re-elects from evidence: a record page after a reset is LZ77 under both rules")
  void probePageElectsFromEvidence() {
    for (final boolean stickyOnly : new boolean[] {false, true}) {
      PageKind.CODEC_BAKEOFF_STICKY_ONLY = stickyOnly;
      PageKind.resetStickyCodecElectionForCurrentThread();
      final long lz77Before = PageSectionDiag.codecPages(CODEC_LZ77);
      serializeRecordPage();
      assertEquals(1, PageSectionDiag.codecPages(CODEC_LZ77) - lz77Before,
          "a warm-up page probes every codec and LZ77 wins on a record page (stickyOnly=" + stickyOnly + ")");
    }
  }

  /**
   * Serialize a fresh record page with {@code codec} elected on this thread; returns the wire bytes.
   */
  private long serializeRecordPageAfterElecting(final int codec) {
    PageKind.electBodyCodecForTesting(codec);
    return serializeRecordPage();
  }

  private long serializeRecordPage() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("bodyCodecElection").build();
    final KeyValueLeafPage page =
        new KeyValueLeafPage(0, IndexType.DOCUMENT, config, 1, arena.allocate(SIXTYFOUR_KB), null);
    try {
      long nodeKey = 0;
      for (int i = 0; i < RECORDS_PER_KIND; i++) {
        writeNumber(page, nodeKey++, 100 + i, 4_000_000_000_000_000_000L + i);
        writeString(page, nodeKey++, 200 + i, "value-" + i + "-" + "p".repeat(40));
      }
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      return sink.writePosition();
    } finally {
      page.close();
    }
  }

  private static void writeNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slotOf(nodeKey));
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slotOf(nodeKey));
  }

  private static int slotOf(final long nodeKey) {
    return (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
  }
}
