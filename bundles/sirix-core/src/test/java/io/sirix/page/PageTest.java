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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.stream.Stream;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Test class for all classes implementing the {@link Page} interface.
 *
 * <p>Converted from TestNG to JUnit 5 (2026-06-11): TestNG classes never ran under the gradle
 * {@code useJUnitPlatform()} config. The old TestNG data provider also supplied
 * {@code (Class, Page[])} to a single-parameter test method, so the test failed at parameter
 * injection even when run directly; the provider now supplies only the {@code Page[]}. The
 * assertion previously compared the same buffer to itself — it now re-serializes the deserialized
 * page into a fresh buffer, restoring the intended round-trip check.</p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PageTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader storageEngineReader;

  @BeforeAll
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateDeweyIDResourceSession();
    storageEngineReader = holder.getResourceSession().createStorageEngineReader();
  }

  @AfterAll
  public void tearDown() throws SirixException {
    storageEngineReader.close();
    holder.close();
    arena.close();
  }

  @ParameterizedTest
  @MethodSource("instantiatePages")
  public void testByteRepresentation(final Page[] handlers) throws IOException {
    // PagePersister is the production (de)serialization entry point: PageKind#serializePage writes
    // the page-kind id byte itself, while PageKind#deserializePage expects the dispatcher to have
    // consumed it already — so the round-trip must go through PagePersister.
    final var pagePersister = new PagePersister();
    final var resourceConfig = storageEngineReader.getResourceSession().getResourceConfig();
    for (final Page handler : handlers) {
      final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
      pagePersister.serializePage(resourceConfig, data, handler, SerializationType.DATA);
      final var pageBytes = data.toByteArray();
      final Page deserializedPage =
          pagePersister.deserializePage(resourceConfig, Bytes.wrapForRead(data.toByteArray()), SerializationType.DATA);
      final BytesOut<?> reserializedData = Bytes.elasticOffHeapByteBuffer();
      pagePersister.serializePage(resourceConfig, reserializedData, deserializedPage, SerializationType.DATA);
      final var deserializedPageBytes = reserializedData.toByteArray();
      assertArrayEquals(pageBytes, deserializedPageBytes, "Check for " + handler.getClass() + " failed.");
    }
  }

  private Arena arena = Arena.ofConfined();

  /**
   * Providing different implementations of the {@link Page} as method source to the test class.
   *
   * @return different classes of the {@link ByteHandler}
   * @throws SirixIOException if an I/O error occurs
   */
  public Stream<Arguments> instantiatePages() throws SirixIOException {
    // IndirectPage setup.
    final IndirectPage indirectPage = new IndirectPage();
    // RevisionRootPage setup.
    // final RevisionRootPage revRootPage = new RevisionRootPage();

    // NodePage setup.
    final KeyValueLeafPage nodePage = new KeyValueLeafPage(XmlTestHelper.random.nextInt(Integer.MAX_VALUE),
        IndexType.DOCUMENT, storageEngineReader.getResourceSession().getResourceConfig(), storageEngineReader.getRevisionNumber(),
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

    return Stream.of(Arguments.of((Object) new Page[] {indirectPage, namePage, valuePage, pathSummaryPage}));
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
