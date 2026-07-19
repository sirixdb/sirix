package io.sirix.format;

import io.brackit.query.atomic.QNm;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.xml.ElementNode;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.Assert.assertEquals;

/**
 * Golden byte-pins for COMPOSITE pages — a populated {@link KeyValueLeafPage} (record heap,
 * compact directory, structural encoders, PAX assembly) and a populated {@link HOTLeafPage}.
 * Together with {@link GoldenFormatTest} (headers, envelopes, single records, codecs, id
 * registries) this pins whole-page composition: any byte-level change to the serialization
 * pipeline fails here and must be shipped as a conscious format bump.
 */
public final class GoldenCompositePageTest {

  private Holder holder;
  private StorageEngineReader storageEngineReader;
  private Arena arena;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateDeweyIDResourceSession();
    storageEngineReader = holder.getResourceSession().createStorageEngineReader();
    arena = Arena.ofConfined();
  }

  @After
  public void tearDown() throws SirixException {
    storageEngineReader.close();
    holder.close();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    arena.close();
  }

  private static final String GOLDEN_KVLP =
      "01000000010000000100000000000000000100000002005600000056000000010000000000000000"
          + "00030000000000000000000000000000000000000000000000000000000000000000000000000000"
          + "00000000000000000000000000000000000000000000000000000000000000000000000000000000"
          + "00000000000000000000000000000000000000000000000000000000000000000000000000000000"
          + "000000000000000000020000003a0000000100110000005700000003fd554000001d010400f12901"
          + "0f000102030405060708090a0b131415000001000309071919030c0e0a01000f1d506d3cfcc7dd02"
          + "0002b10102c70101000105071717011d00f001c0f638d2d64b9c6b020002af0102c5010000000000"
          + "000000000000000000";

  private static final String GOLDEN_HOT_LEAF =
      "0c00000101000000050000030000002f00000000000000100000001f0000000500616c7068610700"
          + "76616c75652d31040062657461070076616c75652d32050067616d6d61070076616c75652d33";

  @Test
  public void keyValueLeafPageBytesArePinned() throws IOException {
    final var config = storageEngineReader.getResourceSession().getResourceConfig();
    final KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config,
        storageEngineReader.getRevisionNumber(), arena.allocate(SIXTYFOUR_KB), null);
    try {
      page.setRecord(elementNode(config, 0L, 3L, 4L));
      page.setRecord(elementNode(config, 1L, 4L, 3L));

      final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
      new PagePersister().serializePage(config, data, page, SerializationType.DATA);
      assertGolden("kvlp", GOLDEN_KVLP, hex(data.toByteArray()));
    } finally {
      page.close();
    }
  }

  @Test
  public void hotLeafPageBytesArePinned() throws IOException {
    final var config = storageEngineReader.getResourceSession().getResourceConfig();
    final MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH, slotMemory, null,
        new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    try {
      page.put("alpha".getBytes(StandardCharsets.UTF_8), "value-1".getBytes(StandardCharsets.UTF_8));
      page.put("beta".getBytes(StandardCharsets.UTF_8), "value-2".getBytes(StandardCharsets.UTF_8));
      page.put("gamma".getBytes(StandardCharsets.UTF_8), "value-3".getBytes(StandardCharsets.UTF_8));

      final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
      new PagePersister().serializePage(config, data, page, SerializationType.DATA);
      assertGolden("hot-leaf", GOLDEN_HOT_LEAF, hex(data.toByteArray()));
    } finally {
      page.close();
    }
  }

  private static ElementNode elementNode(final io.sirix.access.ResourceConfiguration config, final long nodeKey,
      final long leftSibling, final long rightSibling) {
    final LongArrayList attributeKeys = new LongArrayList();
    attributeKeys.add(88L);
    final LongArrayList namespaceKeys = new LongArrayList();
    namespaceKeys.add(99L);
    final ElementNode node = new ElementNode(nodeKey, 1L, Constants.NULL_REVISION_NUMBER, 0, rightSibling,
        leftSibling, 12L, 12L, config.storeChildCount() ? 1L : 0L, 0L, 0L, 1L, 6, 7, 5, config.nodeHashFunction,
        SirixDeweyID.newRootID(), attributeKeys, namespaceKeys, new QNm("a", "b", "c"));
    node.setHash(node.computeHash(Bytes.elasticOffHeapByteBuffer()));
    return node;
  }

  private static void assertGolden(final String what, final String expected, final String actual) {
    if ("TBD".equals(expected)) {
      System.out.println("GOLDEN[" + what + "]=" + actual);
      throw new AssertionError("Golden constant for " + what + " not yet recorded; actual printed above");
    }
    assertEquals(what, expected, actual);
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
