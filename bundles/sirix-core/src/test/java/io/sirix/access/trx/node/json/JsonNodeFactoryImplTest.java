/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for JsonNodeFactoryImpl to ensure correct node creation, especially that Object* value
 * nodes don't have structural fields, and that regular value nodes are created with correct sibling
 * values.
 *
 * @author Johannes Lichtenberger
 */
public class JsonNodeFactoryImplTest {

  private StorageEngineWriter storageEngineWriter;
  private JsonResourceSession resourceSession;
  private JsonNodeFactoryImpl factory;
  private ResourceConfiguration resourceConfig;
  private long nodeCounter = 1;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
    resourceConfig = resourceSession.getResourceConfig();

    // Create a mock StorageEngineWriter that captures created nodes
    storageEngineWriter = mock(StorageEngineWriter.class);

    // Mock the RevisionRootPage to provide node keys
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(revisionRootPage.getMaxNodeKeyInDocumentIndex()).thenAnswer(inv -> nodeCounter++);

    // Mock the ResourceSession
    doReturn(resourceSession).when(storageEngineWriter).getResourceSession();
    when(storageEngineWriter.getRevisionNumber()).thenReturn(0);

    // Mock createRecord to return the node passed to it (used by PathNode, DeweyIDNode)
    when(storageEngineWriter.createRecord(any(DataRecord.class), any(IndexType.class), anyInt())).thenAnswer(
        invocation -> invocation.getArgument(0));

    // Mock createNameKey for ObjectKeyNode
    when(storageEngineWriter.createNameKey(anyString(), any(NodeKind.class))).thenReturn(5); // Return a dummy name key

    // Mock allocation methods for direct-to-heap creation
    final KeyValueLeafPage testKvl = new KeyValueLeafPage(0, IndexType.DOCUMENT, resourceConfig, 0, null, null);
    final long[] allocatedKey = {0};
    doAnswer(inv -> {
      allocatedKey[0] = nodeCounter++;
      return null;
    }).when(storageEngineWriter).allocateForDocumentCreation();
    when(storageEngineWriter.getAllocKvl()).thenReturn(testKvl);
    when(storageEngineWriter.getAllocNodeKey()).thenAnswer(inv -> allocatedKey[0]);
    when(storageEngineWriter.getAllocSlotOffset()).thenAnswer(
        inv -> (int) (allocatedKey[0] % Constants.NDP_NODE_COUNT));

    factory = new JsonNodeFactoryImpl(LongHashFunction.xx3(), storageEngineWriter);
  }

  @After
  public void tearDown() {
    if (resourceSession != null) {
      resourceSession.close();
    }
    JsonTestHelper.deleteEverything();
  }

  // ============================================================================
  // Regular Value Nodes - Direct Factory Tests
  // ============================================================================

  @Test
  public void testFactoryCreateStringNode_SiblingsCorrect() {
    final byte[] value = "test string".getBytes(StandardCharsets.UTF_8);
    final long parentKey = 1L;
    final long leftSibKey = 100L;
    final long rightSibKey = 200L;

    // Create StringNode via factory (array element - has siblings)
    final StringNode node = factory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, value, false, // doCompress
        null);

    // CRITICAL: Factory should create node with correct sibling values
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be STRING_VALUE", NodeKind.STRING_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertArrayEquals("Value should match", value, node.getRawValue());

    // Test the actual issue: Do the siblings match what was passed to the factory?
    assertEquals("Left sibling key should match factory parameter", leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateNumberNode_SiblingsCorrect() {
    final double value = 42.5;
    final long parentKey = 2L;
    final long leftSibKey = 300L;
    final long rightSibKey = 400L;

    // Create NumberNode via factory
    final NumberNode node = factory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, null);

    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be NUMBER_VALUE", NodeKind.NUMBER_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertEquals("Value should match", value, node.getValue().doubleValue(), 0.0001);

    assertEquals("Left sibling key should match factory parameter", leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateBooleanNode_SiblingsCorrect() {
    final boolean value = true;
    final long parentKey = 3L;
    final long leftSibKey = 500L;
    final long rightSibKey = 600L;

    // Create BooleanNode via factory
    final BooleanNode node = factory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, null);

    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be BOOLEAN_VALUE", NodeKind.BOOLEAN_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertEquals("Value should match", value, node.getValue());

    assertEquals("Left sibling key should match factory parameter", leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateNullNode_SiblingsCorrect() {
    final long parentKey = 4L;
    final long leftSibKey = 700L;
    final long rightSibKey = 800L;

    // Create NullNode via factory
    final NullNode node = factory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, null);

    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be NULL_VALUE", NodeKind.NULL_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());

    assertEquals("Left sibling key should match factory parameter", leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateStringNode_NullSiblings() {
    final byte[] value = "single element".getBytes(StandardCharsets.UTF_8);
    final long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();

    // Create node with NULL siblings (single element in array)
    final StringNode node = factory.createJsonStringNode(1L, // parentKey
        nullKey, // leftSibKey = NULL
        nullKey, // rightSibKey = NULL
        value, false, null);

    // Verify NULL siblings are preserved
    assertEquals("Left sibling should be NULL", nullKey, node.getLeftSiblingKey());
    assertEquals("Right sibling should be NULL", nullKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateNumberNode_LargeKeys() {
    // Test with large sibling keys to ensure no overflow
    final long leftSibKey = Long.MAX_VALUE - 1000;
    final long rightSibKey = Long.MAX_VALUE - 500;
    final int value = 12345;

    final NumberNode node = factory.createJsonNumberNode(1L, leftSibKey, rightSibKey, value, null);

    // Verify large keys are preserved
    assertEquals("Left sibling with large key should be correct", leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling with large key should be correct", rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateMultipleNodes_ReboundStateIsCorrect() {
    final StringNode firstStringNode =
        factory.createJsonStringNode(1L, 10L, 20L, "node1".getBytes(StandardCharsets.UTF_8), false, null);
    final long firstStringNodeKey = firstStringNode.getNodeKey();

    final StringNode secondStringNode =
        factory.createJsonStringNode(1L, 30L, 40L, "node2".getBytes(StandardCharsets.UTF_8), false, null);
    final NumberNode numberNode = factory.createJsonNumberNode(1L, 50L, 60L, 100.0, null);

    assertSame("String node proxy should be reused", firstStringNode, secondStringNode);
    assertNotEquals("Rebound string node should expose new node key", firstStringNodeKey,
        secondStringNode.getNodeKey());
    assertEquals("Second string node left sibling", 30L, secondStringNode.getLeftSiblingKey());
    assertEquals("Second string node right sibling", 40L, secondStringNode.getRightSiblingKey());

    assertEquals("Number node left sibling should match create call", 50L, numberNode.getLeftSiblingKey());
    assertEquals("Number node right sibling should match create call", 60L, numberNode.getRightSiblingKey());
  }

  @Test
  public void testFactoryReusesPayloadFreeObjectNodeProxy() {
    final ObjectNode first = factory.createJsonObjectNode(1L, 10L, 20L, null);
    final long firstNodeKey = first.getNodeKey();

    final ObjectNode second = factory.createJsonObjectNode(2L, 30L, 40L, null);

    assertSame("Factory should reuse transaction-local object proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 2L, second.getParentKey());
    assertEquals("Left sibling should be rebound", 30L, second.getLeftSiblingKey());
    assertEquals("Right sibling should be rebound", 40L, second.getRightSiblingKey());
  }

  @Test
  public void testFactoryReusesNumberNodeProxy() {
    final NumberNode first = factory.createJsonNumberNode(1L, 10L, 20L, 41, null);
    final long firstNodeKey = first.getNodeKey();

    final NumberNode second = factory.createJsonNumberNode(2L, 30L, 40L, 42, null);

    assertSame("Factory should reuse transaction-local number proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 2L, second.getParentKey());
    assertEquals("Left sibling should be rebound", 30L, second.getLeftSiblingKey());
    assertEquals("Right sibling should be rebound", 40L, second.getRightSiblingKey());
    assertEquals("Value should be rebound", 42L, second.getValue().longValue());
  }

  @Test
  public void testFactoryReusesStringNodeProxy() {
    final StringNode first =
        factory.createJsonStringNode(1L, 10L, 20L, "first".getBytes(StandardCharsets.UTF_8), false, null);
    final long firstNodeKey = first.getNodeKey();

    final byte[] secondValue = "second".getBytes(StandardCharsets.UTF_8);
    final StringNode second = factory.createJsonStringNode(2L, 30L, 40L, secondValue, false, null);

    assertSame("Factory should reuse transaction-local string proxy", first, second);
    assertNotEquals("Rebound proxy must expose updated node key", firstNodeKey, second.getNodeKey());
    assertEquals("Parent key should be rebound", 2L, second.getParentKey());
    assertEquals("Left sibling should be rebound", 30L, second.getLeftSiblingKey());
    assertEquals("Right sibling should be rebound", 40L, second.getRightSiblingKey());
    assertArrayEquals("Value should be rebound", secondValue, second.getRawValueWithoutDecompression());
  }

}
