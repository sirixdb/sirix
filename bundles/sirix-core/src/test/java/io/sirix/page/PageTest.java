package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.HashCountEntryNode;
import io.sirix.node.BytesOut;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.HashEntryNode;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.foreign.Arena;
import java.nio.ByteBuffer;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertArrayEquals;

/**
 * Test class for all classes implementing the {@link Page} interface.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public class PageTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader pageReadTrx;

  @BeforeClass
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateDeweyIDResourceSession();
    pageReadTrx = holder.getResourceSession().beginStorageEngineReader();
  }

  @AfterClass
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    holder.close();
    arena.close();
  }

  @Test(dataProvider = "instantiatePages")
  public void testByteRepresentation(final Page[] handlers) {
    for (final Page handler : handlers) {
      final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
      PageKind.getKind(handler.getClass())
              .serializePage(pageReadTrx.getResourceSession().getResourceConfig(), data, handler,
                  SerializationType.DATA);
      // handler.serialize(pageReadTrx, data, SerializationType.DATA);
      final var pageBytes = data.toByteArray();
      final Page serializedPage = PageKind.getKind(handler.getClass())
                                          .deserializePage(pageReadTrx.getResourceSession().getResourceConfig(),
                                              Bytes.wrapForRead(data.toByteArray()), SerializationType.DATA);
      // serializedPage.serialize(pageReadTrx, data, SerializationType.DATA);
      final var serializedPageBytes = data.toByteArray();
      assertArrayEquals("Check for " + handler.getClass() + " failed.", pageBytes, serializedPageBytes);
    }
  }

  private Arena arena = Arena.ofConfined();

  /**
   * Providing different implementations of the {@link Page} as Dataprovider to the test class.
   *
   * @return different classes of the {@link ByteHandler}
   * @throws SirixIOException if an I/O error occurs
   */
  @DataProvider(name = "instantiatePages")
  public Object[][] instantiatePages() throws SirixIOException {
    // IndirectPage setup.
    final IndirectPage indirectPage = new IndirectPage();
    // RevisionRootPage setup.
    // final RevisionRootPage revRootPage = new RevisionRootPage();

    // NodePage setup.
    final KeyValueLeafPage nodePage = new KeyValueLeafPage(XmlTestHelper.random.nextInt(Integer.MAX_VALUE),
        IndexType.DOCUMENT, pageReadTrx.getResourceSession().getResourceConfig(), pageReadTrx.getRevisionNumber(),
        arena.allocate(SIXTYFOUR_KB), null);
    for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
      final DataRecord record = XmlTestHelper.generateOne();
      nodePage.setRecord(record);
    }
    // NamePage setup.
    final NamePage namePage = new NamePage();
    final String name = new String(XmlTestHelper.generateRandomBytes(256));
    namePage.setName(name, NodeKind.ELEMENT, createPageTrxMock());

    // PathPage setup.
    final PathPage valuePage = new PathPage();

    // PathSummaryPage setup.
    final PathSummaryPage pathSummaryPage = new PathSummaryPage();

    return new Object[][] {{Page.class, new Page[] {indirectPage, namePage, valuePage, pathSummaryPage}}};
  }

  private StorageEngineWriter createPageTrxMock() {
    final var hashEntryNode = new HashEntryNode(2, 12, "name");
    final var hashCountEntryNode = new HashCountEntryNode(3, 1);

    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.createRecord(any(HashEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(hashEntryNode);
    when(storageEngineWriter.createRecord(any(HashCountEntryNode.class), eq(IndexType.NAME), eq(0))).thenReturn(hashCountEntryNode);
    when(storageEngineWriter.prepareRecordForModification(2L, IndexType.NAME, 0)).thenReturn(hashCountEntryNode);
    return storageEngineWriter;
  }
}
