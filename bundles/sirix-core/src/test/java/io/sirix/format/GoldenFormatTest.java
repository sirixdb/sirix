package io.sirix.format;

import io.sirix.JsonTestHelper;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.io.Superblock;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.io.bytepipe.JavaLz4BlockDecoder;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.page.PageKind;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Golden-file tests: pin the EXACT bytes of representative serialized structures so that any
 * accidental change to the binary format fails CI instead of silently breaking every existing
 * resource. A legitimate format change must update these constants consciously — together with a
 * {@code BinaryEncodingVersion}/superblock-version bump and a migration note in
 * {@code docs/DISK_FORMAT.md}.
 */
public final class GoldenFormatTest {

  private StorageEngineWriter storageEngineWriter;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    storageEngineWriter = database.beginResourceSession(JsonTestHelper.RESOURCE).createStorageEngineWriter();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ==================== golden constants ====================

  private static final String GOLDEN_SUPERBLOCK_DATA =
      "53495249584442210000000000000000040302010010000000100000000000000030000000000000"
          + "000000000000000000000000000000008b366a74bdb5c9f2";
  private static final String GOLDEN_SUPERBLOCK_REVISIONS =
      "53495249584442210000000001000000040302012000000000000000000000000010000000000000"
          + "000000000000000000000000000000005cf2431e4f70b533";
  private static final String GOLDEN_UBER_PAGE = "03000001000000";
  private static final String GOLDEN_REVISION_REFERENCES_NODE = "230003000000030000000700000004010000";
  private static final String GOLDEN_VARINTS = "0001fe01d804808080808040ffffffffffffffff7f0300959aef3a";
  private static final String GOLDEN_ROARING64 =
      "01020000000000000000020002000000000100040000000000000000000000000000000000040000"
          + "00000001000000010000000000000001000000fe02000000010202000000010064000102010000000"
          + "00002000000000000000000000001000000";

  /** Stable id registries — changing any value is an on-disk format break. */
  private static final String GOLDEN_PAGE_KIND_IDS =
      "KEYVALUELEAFPAGE=1,NAMEPAGE=2,UBERPAGE=3,INDIRECTPAGE=4,REVISIONROOTPAGE=5,PATHSUMMARYPAGE=6,"
          + "CASPAGE=8,OVERFLOWPAGE=9,PATHPAGE=10,DEWEYIDPAGE=11,HOT_LEAF_PAGE=12,HOT_INDIRECT_PAGE=13,"
          + "BITMAP_CHUNK_PAGE=14,VECTORPAGE=15,PROJECTIONPAGE=16,VALIDTIMEPAGE=17,"
          + "PROJECTION_SEGMENT_PAGE=18";

  private static final String GOLDEN_INDEX_TYPE_IDS =
      "REVISIONS=0,DOCUMENT=1,CHANGED_NODES=2,RECORD_TO_REVISIONS=3,PATH_SUMMARY=4,PATH=5,CAS=6,"
          + "NAME=7,DEWEYID_TO_RECORDID=8,VECTOR=9,PROJECTION=10,VALIDTIME=11";

  // ==================== cases ====================

  @Test
  public void superblockBytesArePinned() {
    final String dataHex = hex(Superblock.build(Superblock.ROLE_DATA));
    final String revisionsHex = hex(Superblock.build(Superblock.ROLE_REVISIONS));
    assertGolden("superblock-data", GOLDEN_SUPERBLOCK_DATA, dataHex);
    assertGolden("superblock-revisions", GOLDEN_SUPERBLOCK_REVISIONS, revisionsHex);
  }

  @Test
  public void uberPageBytesArePinned() throws IOException {
    final var config = storageEngineWriter.getResourceSession().getResourceConfig();
    final UberPage uberPage = new UberPage();
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    new PagePersister().serializePage(config, sink, uberPage, SerializationType.DATA);
    assertGolden("uber-page", GOLDEN_UBER_PAGE, hex(sink));
  }

  @Test
  public void revisionReferencesNodeBytesArePinned() {
    final var config = storageEngineWriter.getResourceSession().getResourceConfig();
    final var node = new RevisionReferencesNode(42, new int[] {3, 7, 260});
    final NodeKind kind = (NodeKind) node.getKind();
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    sink.writeByte(kind.getId());
    kind.serialize(sink, node, config);
    assertGolden("revision-references-node", GOLDEN_REVISION_REFERENCES_NODE, hex(sink));
  }

  @Test
  public void deltaVarIntEncodingsArePinned() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    DeltaVarIntCodec.encodeSigned(sink, 0);
    DeltaVarIntCodec.encodeSigned(sink, -1);
    DeltaVarIntCodec.encodeSigned(sink, 127);
    DeltaVarIntCodec.encodeSigned(sink, 300);
    DeltaVarIntCodec.encodeSignedLong(sink, 1L << 40);
    DeltaVarIntCodec.encodeSignedLong(sink, Long.MIN_VALUE / 2);
    DeltaVarIntCodec.encodeDelta(sink, 1001, 1000);
    DeltaVarIntCodec.encodeDelta(sink, io.sirix.settings.Fixed.NULL_NODE_KEY.getStandardProperty(), 1000);
    DeltaVarIntCodec.encodeAbsolute(sink, 123456789L);
    assertGolden("varints", GOLDEN_VARINTS, hex(sink));
  }

  @Test
  public void roaring64SerializationIsPinned() throws IOException {
    // Third-party format coupling: NamePage and node-reference sets embed Roaring64Bitmap's
    // portable serialization. If the library ever changes its wire format, this fails.
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addLong(1L);
    bitmap.addLong(100L);
    bitmap.addLong(1L << 40);
    bitmap.runOptimize();
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bitmap.serialize(new DataOutputStream(bos));
    assertGolden("roaring64", GOLDEN_ROARING64, hex(bos.toByteArray()));
  }

  @Test
  public void lz4BlockDecoderDecodesHandcraftedBlock() {
    // [token lit=4|match=4] "abcd" [dist=4 LE] → "abcd" + 8-byte overlapping match; final token 0x00.
    final byte[] block = {0x44, 'a', 'b', 'c', 'd', 0x04, 0x00, 0x00};
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment src = arena.allocate(block.length);
      MemorySegment.copy(block, 0, src, ValueLayout.JAVA_BYTE, 0, block.length);
      final MemorySegment dst = arena.allocate(64);
      final int n = JavaLz4BlockDecoder.decompressSafe(src, 0, block.length, dst, 0, 64);
      final byte[] out = new byte[n];
      MemorySegment.copy(dst, ValueLayout.JAVA_BYTE, 0, out, 0, n);
      assertEquals("abcdabcdabcd", new String(out, StandardCharsets.US_ASCII));
    }
  }

  @Test
  public void lz4BlockDecoderMatchesNativeCompressor() {
    // When liblz4 is present, prove the pure-Java decoder consumes real native-compressed
    // blocks — the exact bytes an LZ4-capable host persists and an LZ4-less host must read.
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final byte[] input = new byte[8192];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) ((i * 31 + (i >>> 5)) & 0x3F); // repetitive → compressible
    }
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment src = arena.allocate(input.length);
      MemorySegment.copy(input, 0, src, ValueLayout.JAVA_BYTE, 0, input.length);
      final MemorySegment compressed = arena.allocate(lz4.compressBound(input.length));
      final int compressedLen;
      try {
        compressedLen = lz4.compressSegment(src, compressed);
      } catch (UnsupportedOperationException nativeUnavailable) {
        return; // no liblz4 on this host — the handcrafted-block test still covers the decoder
      }
      final MemorySegment out = arena.allocate(input.length);
      final int n = JavaLz4BlockDecoder.decompressSafe(compressed, 0, compressedLen, out, 0, input.length);
      assertEquals(input.length, n);
      final byte[] roundTripped = new byte[n];
      MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0, roundTripped, 0, n);
      assertArrayEquals(input, roundTripped);
    }
  }

  @Test
  public void pageKindIdsArePinned() {
    final StringJoiner joiner = new StringJoiner(",");
    for (final PageKind kind : PageKind.values()) {
      joiner.add(kind.name() + "=" + (kind.getID() & 0xFF));
    }
    assertEquals(GOLDEN_PAGE_KIND_IDS, joiner.toString());
  }

  @Test
  public void nodeKindIdsArePinned() {
    final StringJoiner joiner = new StringJoiner(",");
    for (final NodeKind kind : NodeKind.values()) {
      joiner.add(kind.name() + "=" + kind.getId());
    }
    assertGolden("node-kind-ids", GOLDEN_NODE_KIND_IDS, joiner.toString());
  }

  private static final String GOLDEN_NODE_KIND_IDS =
      "ELEMENT=1,ATTRIBUTE=2,NAMESPACE=13,TEXT=3,PROCESSING_INSTRUCTION=7,COMMENT=8,XML_DOCUMENT=9,"
          + "WHITESPACE=4,DELETE=5,NULL=6,DUMB=20,ATOMIC=15,PATH=16,CASRB=17,PATHRB=18,NAMERB=19,"
          + "RB_NODE_VALUE=55,DEWEYIDMAPPING=23,OBJECT=24,ARRAY=25,OBJECT_NAMED_BOOLEAN=48,"
          + "OBJECT_NAMED_NUMBER=49,OBJECT_NAMED_STRING=50,OBJECT_NAMED_NULL=51,OBJECT_NAMED_OBJECT=52,"
          + "OBJECT_NAMED_ARRAY=53,STRING_VALUE=30,BOOLEAN_VALUE=27,NUMBER_VALUE=28,NULL_VALUE=29,"
          + "JSON_DOCUMENT=31,HASH_ENTRY=32,HASH_NAME_COUNT_TO_NAME_ENTRY=33,DEWEY_ID_NODE=34,"
          + "REVISION_REFERENCES_NODE=35,PROJECTION_INDEX_LEAF=44,VECTOR_NODE=56,VECTOR_INDEX_METADATA=58,"
          + "UNKNOWN=22";

  @Test
  public void indexTypeIdsArePinned() {
    final StringJoiner joiner = new StringJoiner(",");
    for (final IndexType type : IndexType.values()) {
      joiner.add(type.name() + "=" + type.getID());
    }
    assertEquals(GOLDEN_INDEX_TYPE_IDS, joiner.toString());
  }

  // ==================== helpers ====================

  private static void assertGolden(final String what, final String expected, final String actual) {
    if ("TBD".equals(expected)) {
      System.out.println("GOLDEN[" + what + "]=" + actual);
      throw new AssertionError("Golden constant for " + what + " not yet recorded; actual printed above");
    }
    assertEquals(what, expected, actual);
  }

  private static String hex(final BytesOut<?> sink) {
    return hex(sink.toByteArray());
  }

  private static String hex(final ByteBuffer buf) {
    final byte[] bytes = new byte[buf.remaining()];
    buf.duplicate().get(bytes);
    return hex(bytes);
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
