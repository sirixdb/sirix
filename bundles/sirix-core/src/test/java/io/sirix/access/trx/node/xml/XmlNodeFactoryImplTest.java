package io.sirix.access.trx.node.xml;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.settings.Fixed;
import io.sirix.page.RevisionRootPage;
import net.openhft.hashing.LongHashFunction;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class XmlNodeFactoryImplTest {

  private StorageEngineWriter pageTrx;
  private XmlNodeFactoryImpl factory;
  private long nodeCounter;

  @Before
  public void setUp() {
    pageTrx = mock(StorageEngineWriter.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    when(pageTrx.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(revisionRootPage.getMaxNodeKeyInDocumentIndex()).thenAnswer(invocation -> nodeCounter++);
    when(pageTrx.getRevisionNumber()).thenReturn(1);
    when(pageTrx.createRecord(any(DataRecord.class), any(IndexType.class), anyInt())).thenAnswer(
        invocation -> invocation.getArgument(0));
    when(pageTrx.createNameKey(anyString(), any(NodeKind.class))).thenAnswer(
        invocation -> Math.abs(invocation.getArgument(0, String.class).hashCode()));
    factory = new XmlNodeFactoryImpl(LongHashFunction.xx3(), pageTrx);
  }

  @Test
  public void testFactoryReusesAttributeNodeProxy() {
    final AttributeNode first = factory.createAttributeNode(5L,
                                                            new QNm("urn:first", "p1", "att1"),
                                                            "v1".getBytes(StandardCharsets.UTF_8),
                                                            11L,
                                                            null);
    final long firstNodeKey = first.getNodeKey();

    final QNm secondName = new QNm("urn:second", "p2", "att2");
    final byte[] secondValue = "v2".getBytes(StandardCharsets.UTF_8);
    final AttributeNode second = factory.createAttributeNode(6L, secondName, secondValue, 22L, null);

    assertSame("Factory should reuse transaction-local attribute proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 6L, second.getParentKey());
    assertEquals("Path node key should be rebound", 22L, second.getPathNodeKey());
    assertArrayEquals("Attribute value should be rebound", secondValue, second.getRawValue());
    assertEquals("Attribute name should be rebound", secondName, second.getName());
  }

  @Test
  public void testFactoryReusesNamespaceNodeProxy() {
    final NamespaceNode first = factory.createNamespaceNode(9L, new QNm("urn:first", "p1", "xmlns"), 12L, null);
    final long firstNodeKey = first.getNodeKey();

    final QNm secondName = new QNm("urn:second", "p2", "xmlns");
    final NamespaceNode second = factory.createNamespaceNode(10L, secondName, 23L, null);

    assertSame("Factory should reuse transaction-local namespace proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 10L, second.getParentKey());
    assertEquals("Path node key should be rebound", 23L, second.getPathNodeKey());
    assertEquals("Namespace name should be rebound", secondName, second.getName());
  }

  @Test
  public void testFactoryReusesPINodeProxy() {
    final byte[] firstContent = "01234567890-pi".getBytes(StandardCharsets.UTF_8);
    final PINode first = factory.createPINode(1L,
                                              10L,
                                              20L,
                                              new QNm("urn:first", "p1", "target1"),
                                              firstContent,
                                              true,
                                              111L,
                                              null);
    final long firstNodeKey = first.getNodeKey();
    final boolean firstWasCompressed = first.isCompressed();

    final QNm secondTarget = new QNm("urn:second", "p2", "target2");
    final byte[] secondContent = "short".getBytes(StandardCharsets.UTF_8);
    final PINode second = factory.createPINode(2L, 30L, 40L, secondTarget, secondContent, true, 222L, null);

    assertSame("Factory should reuse transaction-local PI proxy", first, second);
    assertTrue("Compression flag should be set for long PI content", firstWasCompressed);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 2L, second.getParentKey());
    assertEquals("Left sibling key should be rebound", 30L, second.getLeftSiblingKey());
    assertEquals("Right sibling key should be rebound", 40L, second.getRightSiblingKey());
    assertEquals("Path node key should be rebound", 222L, second.getPathNodeKey());
    assertArrayEquals("PI content should be rebound", secondContent, second.getRawValue());
    assertFalse("Compression flag should be recomputed for short PI content", second.isCompressed());
    assertEquals("PI target should be rebound", secondTarget, second.getName());
  }

  @Test
  public void testFactoryReusesElementNodeProxyAndClearsLists() {
    final ElementNode first = factory.createElementNode(1L, 10L, 20L, new QNm("urn:first", "p1", "e1"), 111L, null);
    first.insertAttribute(900L);
    first.insertNamespace(901L);
    final long firstNodeKey = first.getNodeKey();

    final QNm secondName = new QNm("urn:second", "p2", "e2");
    final ElementNode second = factory.createElementNode(2L, 30L, 40L, secondName, 222L, null);

    assertSame("Factory should reuse transaction-local element proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 2L, second.getParentKey());
    assertEquals("Left sibling key should be rebound", 30L, second.getLeftSiblingKey());
    assertEquals("Right sibling key should be rebound", 40L, second.getRightSiblingKey());
    assertEquals("Path node key should be rebound", 222L, second.getPathNodeKey());
    assertEquals("Element name should be rebound", secondName, second.getName());
    assertEquals("First child key should be reset", Fixed.NULL_NODE_KEY.getStandardProperty(), second.getFirstChildKey());
    assertEquals("Last child key should be reset", Fixed.NULL_NODE_KEY.getStandardProperty(), second.getLastChildKey());
    assertEquals("Attribute list must be cleared on rebind", 0, second.getAttributeCount());
    assertEquals("Namespace list must be cleared on rebind", 0, second.getNamespaceCount());
  }

  @Test
  public void testFactoryReusesTextNodeProxy() {
    final TextNode first = factory.createTextNode(4L,
                                                  1L,
                                                  2L,
                                                  "01234567890".getBytes(StandardCharsets.UTF_8),
                                                  true,
                                                  null);
    final long firstNodeKey = first.getNodeKey();

    final byte[] secondValue = "short".getBytes(StandardCharsets.UTF_8);
    final TextNode second = factory.createTextNode(5L, 3L, 4L, secondValue, true, null);

    assertSame("Factory should reuse transaction-local text proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 5L, second.getParentKey());
    assertEquals("Left sibling key should be rebound", 3L, second.getLeftSiblingKey());
    assertEquals("Right sibling key should be rebound", 4L, second.getRightSiblingKey());
    assertArrayEquals("Text value should be rebound", secondValue, second.getRawValue());
    assertFalse("Compression flag should be recomputed for short values", second.isCompressed());
  }

  @Test
  public void testFactoryReusesCommentNodeProxy() {
    final CommentNode first = factory.createCommentNode(4L,
                                                        1L,
                                                        2L,
                                                        "comment-one".getBytes(StandardCharsets.UTF_8),
                                                        true,
                                                        null);
    final long firstNodeKey = first.getNodeKey();

    final byte[] secondValue = "ok".getBytes(StandardCharsets.UTF_8);
    final CommentNode second = factory.createCommentNode(8L, 6L, 7L, secondValue, true, null);

    assertSame("Factory should reuse transaction-local comment proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 8L, second.getParentKey());
    assertEquals("Left sibling key should be rebound", 6L, second.getLeftSiblingKey());
    assertEquals("Right sibling key should be rebound", 7L, second.getRightSiblingKey());
    assertArrayEquals("Comment value should be rebound", secondValue, second.getRawValue());
    assertFalse("Compression flag should be recomputed for short values", second.isCompressed());
  }
}
