package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Independent wire vectors from the 1.0 envelope contract, not encoder-generated expectations. */
public final class PageEnvelopeContractTest {
  private static final ResourceConfiguration CONFIG = ResourceConfiguration.newBuilder("envelopes").build();

  @Test
  public void optionalEntriesLeaveBodyAndKindFlagsIntact() {
    final BytesIn<?> source = hex("0083070102aabb4001cc55");
    assertEquals(3, PageKind.readVersionAndFlagsAllowing(source, (byte) 3));
    assertEquals(0x55, source.readByte());
    assertEquals(0, source.remaining());
    final BytesIn<?> empty = hex("00800055");
    PageKind.readVersionAndFlags(empty);
    assertEquals(0x55, empty.readByte());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0080", "008080", "0080050100", "00800101", "0080020180", "0080030102ff", "00800401000100",
      "00800402000100", "0080ffffffffffffffffff01", "00808080808080808080808000"})
  public void malformedBlocksRefuseWithoutCrossingEntryBoundary(final String wire) {
    final BytesIn<?> source = hex(wire);
    final IllegalStateException error =
        assertThrows(wire, IllegalStateException.class, () -> PageKind.readVersionAndFlags(source));
    assertTrue(error.getMessage(), error.getMessage().contains("extension"));
    assertTrue(source.position() <= wire.length() / 2);
  }

  @Test
  public void malformedEntryLeavesBodyBoundaryIntact() {
    final BytesIn<?> boundary = hex("008002018055");
    assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlags(boundary));
    assertEquals(5, boundary.position());
    assertEquals(0x55, boundary.readByte());
  }

  @Test
  public void unknownRequiredTagsAndFlagsNameTheProblem() {
    final BytesIn<?> requiredSource = hex("0080028000");
    final IllegalStateException required =
        assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlags(requiredSource));
    assertTrue(required.getMessage(), required.getMessage().contains("required page extension 128"));
    final BytesIn<?> mixed = hex("0084");
    final IllegalStateException unknownFlags =
        assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlagsAllowing(mixed, (byte) 3));
    assertTrue(unknownFlags.getMessage(), unknownFlags.getMessage().contains("flags 0x84 (allowed mask 0x3)"));
    assertEquals(2, mixed.position());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6})
  public void unknownKindFlagsNameTheProblem(final int bit) {
    final byte flag = (byte) (1 << bit);
    final BytesIn<?> source = Bytes.wrapForRead(new byte[] {0, flag});
    assertTrue(assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlags(source)).getMessage()
                                                                                                    .contains("flags"));
  }

  @Test
  public void randomOptionalEntriesHaveExactIndependentBoundaries() {
    final Random random = new Random(0x10E17);
    for (int trial = 0; trial < 256; trial++) {
      final byte[] payload = new byte[random.nextInt(600)];
      random.nextBytes(payload);
      final BytesOut<?> entries = Bytes.elasticHeapByteBuffer();
      entries.writeByte((byte) random.nextInt(128));
      entries.writeStopBit(payload.length);
      entries.write(payload);
      final BytesOut<?> wire = Bytes.elasticHeapByteBuffer();
      wire.writeByte((byte) 0).writeByte((byte) 0x80).writeStopBit(entries.toByteArray().length);
      wire.write(entries.toByteArray());
      wire.writeByte((byte) 0x55);
      final byte[] bytes = wire.toByteArray();
      final BytesIn<?> source = Bytes.wrapForRead(bytes);
      PageKind.readVersionAndFlags(source);
      assertEquals(bytes.length - 1, source.position());
      assertEquals(0x55, source.readByte());
      for (int cut = 2; cut < bytes.length - 1; cut++) {
        final byte[] truncated = Arrays.copyOf(bytes, cut);
        final BytesIn<?> truncatedSource = Bytes.wrapForRead(truncated);
        assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlags(truncatedSource));
      }
    }
  }

  private static Page[] metadataPages() {
    return new Page[] {new NamePage(), new UberPage(), new IndirectPage(), new RevisionRootPage(),
        new PathSummaryPage(), new CASPage(), new PathPage(), new DeweyIDPage(), new VectorPage(),
        new ProjectionIndexPage(), new ValidTimeIndexPage(), new TemporalAggregateIndexPage(), new ChangeLogIndexPage(),
        new OverflowPage(new byte[] {1, 2, 3}), new HOTLeafPage(42, 1, IndexType.CAS), HOTIndirectPage.createSpanNode(
            42, 1, 0, 1L, new int[] {0, 1}, new PageReference[] {new PageReference(), new PageReference()}, 1)};
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("metadataPages")
  public void metadataKindsSkipOptionalEntriesRejectTailsAndDropUnknownOnRewrite(final Page page) throws IOException {
    final PagePersister persister = new PagePersister();
    final BytesOut<?> original = Bytes.elasticHeapByteBuffer();
    persister.serializePage(CONFIG, original, page, SerializationType.DATA);
    final byte[] oldBytes = original.toByteArray();
    final byte[] extended = new byte[oldBytes.length + 4];
    System.arraycopy(oldBytes, 0, extended, 0, 3);
    extended[2] |= (byte) 0x80;
    extended[3] = 3;
    extended[4] = 1;
    extended[5] = 1;
    extended[6] = 42;
    System.arraycopy(oldBytes, 3, extended, 7, oldBytes.length - 3);
    final byte[] required = extended.clone();
    required[4] = (byte) 0x81;
    final BytesIn<?> requiredSource = Bytes.wrapForRead(required);
    final Class<? extends RuntimeException> unsupportedType = page instanceof UberPage
        ? UnsupportedStoreFeatureException.class
        : IllegalStateException.class;
    final String requiredMessage = page instanceof UberPage
        ? "required uber extension 0x81"
        : "required page extension 129";
    assertTrue(assertThrows(unsupportedType,
        () -> persister.deserializePage(CONFIG, requiredSource, SerializationType.DATA)).getMessage()
                                                                                        .contains(requiredMessage));
    final byte[] unknownFlag = oldBytes.clone();
    unknownFlag[2] |= 0x40;
    final BytesIn<?> unknownFlagSource = Bytes.wrapForRead(unknownFlag);
    assertTrue(assertThrows(unsupportedType,
        () -> persister.deserializePage(CONFIG, unknownFlagSource, SerializationType.DATA)).getMessage()
                                                                                           .contains("flags"));
    final byte[] unknownVersion = oldBytes.clone();
    unknownVersion[1] = 1;
    final BytesIn<?> unknownVersionSource = Bytes.wrapForRead(unknownVersion);
    assertThrows(unsupportedType,
        () -> persister.deserializePage(CONFIG, unknownVersionSource, SerializationType.DATA));
    // A deserialized revision root keeps the epoch-microsecond commit timestamp it was committed
    // with, so a rewrite is byte-identical without pinning the timestamp here.
    final Page parsed = persister.deserializePage(CONFIG, Bytes.wrapForRead(extended), SerializationType.DATA);
    final BytesOut<?> rewritten = Bytes.elasticHeapByteBuffer();
    persister.serializePage(CONFIG, rewritten, parsed, SerializationType.DATA);
    assertArrayEquals(page.getClass().getSimpleName(), oldBytes, rewritten.toByteArray());
    if (page instanceof OverflowPage || page instanceof HOTLeafPage) {
      parsed.close();
      page.close();
      return;
    }
    final byte[] trailing = Arrays.copyOf(oldBytes, oldBytes.length + 1);
    final BytesIn<?> trailingSource = Bytes.wrapForRead(trailing);
    final IllegalStateException error = assertThrows(page.getClass().getSimpleName(), IllegalStateException.class,
        () -> persister.deserializePage(CONFIG, trailingSource, SerializationType.DATA));
    assertTrue(error.getMessage(), error.getMessage().contains("Trailing"));
    parsed.close();
    page.close();
  }

  @Test
  public void recordPageRawSlotsPreserveFullByteKindsAndAllReadPathsSeeExtensions() throws IOException {
    final PagePersister persister = new PagePersister();
    try (final KeyValueLeafPage page = new KeyValueLeafPage(41, IndexType.DOCUMENT, CONFIG, 1, null, null, false)) {
      final byte[] raw = {(byte) 200, 12, 34};
      page.setSlot(raw, 7);
      final BytesOut<?> sink = Bytes.elasticHeapByteBuffer();
      persister.serializePage(CONFIG, sink, page, SerializationType.DATA);
      final byte[] original = sink.toByteArray();
      final byte[] extended = new byte[original.length + 3];
      System.arraycopy(original, 0, extended, 0, 3);
      extended[2] |= (byte) 0x80;
      extended[3] = 2;
      extended[4] = 1;
      extended[5] = 0;
      System.arraycopy(original, 3, extended, 6, original.length - 3);
      try (final KeyValueLeafPage decoded =
          (KeyValueLeafPage) persister.deserializePage(CONFIG, Bytes.wrapForRead(extended), SerializationType.DATA)) {
        assertArrayEquals(raw, decoded.getSlotAsByteArray(7));
        assertEquals(0, PageLayout.getDirNodeKindId(decoded.getSlottedPage(), 7));
      }
      try (final KeyValueLeafPage decoded = (KeyValueLeafPage) persister.deserializePageLazily(CONFIG,
          Bytes.wrapForRead(extended), SerializationType.DATA, null)) {
        assertArrayEquals(raw, decoded.getSlotAsByteArray(7));
      }
      final long originalOffset = persister.probeRegionTableOffset(Bytes.wrapForRead(original), new long[5], null);
      final long extendedOffset = persister.probeRegionTableOffset(Bytes.wrapForRead(extended), new long[5], null);
      assertEquals(originalOffset + 3, extendedOffset);
      try (final RegionsOnlyPage regions =
          persister.deserializeRegionsOnlyPage(CONFIG, Bytes.wrapForRead(extended), 0, 0)) {
        assertEquals(41, regions.getPageKey());
      }
      extended[4] = (byte) 0x81;
      final BytesIn<?> requiredSource = Bytes.wrapForRead(extended);
      final long[] scratch = new long[5];
      assertThrows(IllegalStateException.class, () -> persister.probeRegionTableOffset(requiredSource, scratch, null));
    }
  }

  @Test
  public void unknownPageAndCompactRecordKindsRefuseWithTheirIds() {
    final PagePersister persister = new PagePersister();
    final BytesIn<?> unknownPage = hex("7f0000");
    assertTrue(assertThrows(IllegalStateException.class,
        () -> persister.deserializePage(CONFIG, unknownPage, SerializationType.DATA)).getMessage()
                                                                                     .contains("PageKind id: 127"));
    final BytesIn<?> unknownRecord = new ByteArrayBytesIn(new byte[] {(byte) 200});
    assertTrue(assertThrows(IllegalStateException.class,
        () -> CONFIG.recordPersister.deserialize(unknownRecord, 7, null, CONFIG)).getMessage()
                                                                                 .contains("NodeKind id: 200"));
    // One-byte raw payload length, unknown compact kind 63: 0x007f.
    final BytesIn<?> unknownCompactRecord = hex("007f");
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> PageLayout.readCompactDirEntry(unknownCompactRecord)).getMessage().contains("nodeKindId: 63"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"008002018055", "00808080808080808080808000"})
  public void byteArraySourceAlsoBoundsOverflowingAndTruncatedLengths(final String wire) {
    final BytesIn<?> source = new ByteArrayBytesIn(HexFormat.of().parseHex(wire));
    assertThrows(IllegalStateException.class, () -> PageKind.readVersionAndFlags(source));
    assertTrue(source.position() <= wire.length() / 2);
  }

  private static BytesIn<?> hex(final String value) {
    return Bytes.wrapForRead(HexFormat.of().parseHex(value));
  }
}
