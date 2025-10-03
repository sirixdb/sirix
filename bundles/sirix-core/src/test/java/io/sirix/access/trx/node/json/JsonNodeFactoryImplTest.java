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
import io.sirix.access.trx.node.HashType;
import io.sirix.api.PageTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.*;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for JsonNodeFactoryImpl to ensure correct node creation,
 * especially that Object* value nodes don't have structural fields,
 * and that regular value nodes are created with correct sibling values.
 *
 * @author Johannes Lichtenberger
 */
public class JsonNodeFactoryImplTest {

  private PageTrx pageTrx;
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
    
    // Create a mock PageTrx that captures created nodes
    pageTrx = mock(PageTrx.class);
    
    // Mock the RevisionRootPage to provide node keys
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    when(pageTrx.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(revisionRootPage.getMaxNodeKeyInDocumentIndex()).thenAnswer(inv -> nodeCounter++);
    
    // Mock the ResourceSession
    doReturn(resourceSession).when(pageTrx).getResourceSession();
    when(pageTrx.getRevisionNumber()).thenReturn(0);
    
    // Mock createRecord to return the node passed to it
    when(pageTrx.createRecord(any(DataRecord.class), any(IndexType.class), anyInt()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    
    // Mock createNameKey for ObjectKeyNode
    when(pageTrx.createNameKey(anyString(), any(NodeKind.class)))
        .thenReturn(5); // Return a dummy name key
    
    factory = new JsonNodeFactoryImpl(
        LongHashFunction.xx3(), 
        pageTrx
    );
  }

  @After
  public void tearDown() {
    if (resourceSession != null) {
      resourceSession.close();
    }
    JsonTestHelper.closeEverything();
  }

  // ============================================================================
  // Object* Value Nodes (Object Properties) - NO siblings, NO children
  // ============================================================================

  @Test
  public void testCreateJsonObjectNullNode_NoStructuralFields() {
    // Create ObjectNullNode (object property - leaf node)
    final ObjectNullNode node = factory.createJsonObjectNullNode(1L, null);
    
    assertNotNull(node);
    assertEquals(NodeKind.OBJECT_NULL_VALUE, node.getKind());
    assertEquals(1L, node.getParentKey());
    
    // CRITICAL: Object* value nodes should have NO siblings
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    
    // CRITICAL: Object* value nodes should have NO children
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    
    // Verify pageTrx.createRecord was called
    verify(pageTrx).createRecord(eq(node), eq(IndexType.DOCUMENT), eq(-1));
  }

  @Test
  public void testCreateJsonObjectStringNode_NoStructuralFields() {
    final byte[] value = "test object string".getBytes(StandardCharsets.UTF_8);
    
    // Create ObjectStringNode (object property - leaf node)
    final ObjectStringNode node = factory.createJsonObjectStringNode(
        1L,    // parentKey
        value,
        false, // doCompress
        null
    );
    
    assertNotNull(node);
    assertEquals(NodeKind.OBJECT_STRING_VALUE, node.getKind());
    assertEquals(1L, node.getParentKey());
    assertArrayEquals(value, node.getRawValue());
    
    // CRITICAL: Object* value nodes should have NO siblings
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    
    // CRITICAL: Object* value nodes should have NO children
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
  }

  @Test
  public void testCreateJsonObjectBooleanNode_NoStructuralFields() {
    // Create ObjectBooleanNode (object property - leaf node)
    final ObjectBooleanNode node = factory.createJsonObjectBooleanNode(
        1L,    // parentKey
        false, // value
        null
    );
    
    assertNotNull(node);
    assertEquals(NodeKind.OBJECT_BOOLEAN_VALUE, node.getKind());
    assertEquals(1L, node.getParentKey());
    assertFalse(node.getValue());
    
    // CRITICAL: Object* value nodes should have NO siblings
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    
    // CRITICAL: Object* value nodes should have NO children
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
  }

  @Test
  public void testCreateJsonObjectNumberNode_NoStructuralFields() {
    final double value = 123.456;
    
    // Create ObjectNumberNode (object property - leaf node)
    final ObjectNumberNode node = factory.createJsonObjectNumberNode(
        1L,    // parentKey
        value,
        null
    );
    
    assertNotNull(node);
    assertEquals(NodeKind.OBJECT_NUMBER_VALUE, node.getKind());
    assertEquals(1L, node.getParentKey());
    assertEquals(value, node.getValue().doubleValue(), 0.0001);
    
    // CRITICAL: Object* value nodes should have NO siblings
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    
    // CRITICAL: Object* value nodes should have NO children
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
  }

  // ============================================================================
  // Round-trip Serialization Tests - Object* Value Nodes
  // ============================================================================

  @Test
  public void testObjectNullNode_SerializeDeserialize() {
    // Create node
    final ObjectNullNode node1 = factory.createJsonObjectNullNode(100L, null);
    final long nodeKey = node1.getNodeKey();
    
    // Serialize
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    node1.getKind().serialize(data, node1, resourceConfig);
    
    // Deserialize
    final ObjectNullNode node2 = (ObjectNullNode) NodeKind.OBJECT_NULL_VALUE.deserialize(
        data.asBytesIn(), 
        nodeKey, 
        null, 
        resourceConfig
    );
    
    // Verify
    assertEquals(node1.getNodeKey(), node2.getNodeKey());
    assertEquals(node1.getParentKey(), node2.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getRightSiblingKey());
  }

  @Test
  public void testObjectStringNode_SerializeDeserialize() {
    final byte[] value = "serialization test".getBytes(StandardCharsets.UTF_8);
    
    // Create node
    final ObjectStringNode node1 = factory.createJsonObjectStringNode(200L, value, false, null);
    final long nodeKey = node1.getNodeKey();
    
    // Serialize
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    node1.getKind().serialize(data, node1, resourceConfig);
    
    // Deserialize
    final ObjectStringNode node2 = (ObjectStringNode) NodeKind.OBJECT_STRING_VALUE.deserialize(
        data.asBytesIn(), 
        nodeKey, 
        null, 
        resourceConfig
    );
    
    // Verify
    assertEquals(node1.getNodeKey(), node2.getNodeKey());
    assertEquals(node1.getParentKey(), node2.getParentKey());
    assertArrayEquals(value, node2.getRawValue());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getRightSiblingKey());
  }

  @Test
  public void testObjectBooleanNode_SerializeDeserialize() {
    // Create node
    final ObjectBooleanNode node1 = factory.createJsonObjectBooleanNode(300L, true, null);
    final long nodeKey = node1.getNodeKey();
    
    // Serialize
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    node1.getKind().serialize(data, node1, resourceConfig);
    
    // Deserialize
    final ObjectBooleanNode node2 = (ObjectBooleanNode) NodeKind.OBJECT_BOOLEAN_VALUE.deserialize(
        data.asBytesIn(), 
        nodeKey, 
        null, 
        resourceConfig
    );
    
    // Verify
    assertEquals(node1.getNodeKey(), node2.getNodeKey());
    assertEquals(node1.getParentKey(), node2.getParentKey());
    assertEquals(node1.getValue(), node2.getValue());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getRightSiblingKey());
  }

  @Test
  public void testObjectNumberNode_SerializeDeserialize() {
    final double value = 999.999;
    
    // Create node
    final ObjectNumberNode node1 = factory.createJsonObjectNumberNode(400L, value, null);
    final long nodeKey = node1.getNodeKey();
    
    // Serialize
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    node1.getKind().serialize(data, node1, resourceConfig);
    
    // Deserialize
    final ObjectNumberNode node2 = (ObjectNumberNode) NodeKind.OBJECT_NUMBER_VALUE.deserialize(
        data.asBytesIn(), 
        nodeKey, 
        null, 
        resourceConfig
    );
    
    // Verify
    assertEquals(node1.getNodeKey(), node2.getNodeKey());
    assertEquals(node1.getParentKey(), node2.getParentKey());
    assertEquals(value, node2.getValue().doubleValue(), 0.0001);
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node2.getRightSiblingKey());
  }

  @Test
  public void testWithDeweyID() {
    final SirixDeweyID deweyID = new SirixDeweyID("1.3.5");
    
    // Create ObjectNullNode with DeweyID
    final ObjectNullNode node = factory.createJsonObjectNullNode(1L, deweyID);
    
    assertNotNull(node);
    assertNotNull(node.getDeweyID());
    assertEquals(deweyID, node.getDeweyID());
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
    final StringNode node = factory.createJsonStringNode(
        parentKey,
        leftSibKey,
        rightSibKey,
        value,
        false, // doCompress
        null
    );
    
    // CRITICAL: Factory should create node with correct sibling values
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be STRING_VALUE", NodeKind.STRING_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertArrayEquals("Value should match", value, node.getRawValue());
    
    // Test the actual issue: Do the siblings match what was passed to the factory?
    assertEquals("Left sibling key should match factory parameter", 
        leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", 
        rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateNumberNode_SiblingsCorrect() {
    final double value = 42.5;
    final long parentKey = 2L;
    final long leftSibKey = 300L;
    final long rightSibKey = 400L;
    
    // Create NumberNode via factory
    final NumberNode node = factory.createJsonNumberNode(
        parentKey,
        leftSibKey,
        rightSibKey,
        value,
        null
    );
    
    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be NUMBER_VALUE", NodeKind.NUMBER_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertEquals("Value should match", value, node.getValue().doubleValue(), 0.0001);
    
    assertEquals("Left sibling key should match factory parameter", 
        leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", 
        rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateBooleanNode_SiblingsCorrect() {
    final boolean value = true;
    final long parentKey = 3L;
    final long leftSibKey = 500L;
    final long rightSibKey = 600L;
    
    // Create BooleanNode via factory
    final BooleanNode node = factory.createJsonBooleanNode(
        parentKey,
        leftSibKey,
        rightSibKey,
        value,
        null
    );
    
    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be BOOLEAN_VALUE", NodeKind.BOOLEAN_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    assertEquals("Value should match", value, node.getValue());
    
    assertEquals("Left sibling key should match factory parameter", 
        leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", 
        rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateNullNode_SiblingsCorrect() {
    final long parentKey = 4L;
    final long leftSibKey = 700L;
    final long rightSibKey = 800L;
    
    // Create NullNode via factory
    final NullNode node = factory.createJsonNullNode(
        parentKey,
        leftSibKey,
        rightSibKey,
        null
    );
    
    // Verify siblings
    assertNotNull("Node should not be null", node);
    assertEquals("Node kind should be NULL_VALUE", NodeKind.NULL_VALUE, node.getKind());
    assertEquals("Parent key should match", parentKey, node.getParentKey());
    
    assertEquals("Left sibling key should match factory parameter", 
        leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling key should match factory parameter", 
        rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateStringNode_NullSiblings() {
    final byte[] value = "single element".getBytes(StandardCharsets.UTF_8);
    final long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    
    // Create node with NULL siblings (single element in array)
    final StringNode node = factory.createJsonStringNode(
        1L,      // parentKey
        nullKey, // leftSibKey = NULL
        nullKey, // rightSibKey = NULL
        value,
        false,
        null
    );
    
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
    
    final NumberNode node = factory.createJsonNumberNode(
        1L,
        leftSibKey,
        rightSibKey,
        value,
        null
    );
    
    // Verify large keys are preserved
    assertEquals("Left sibling with large key should be correct", 
        leftSibKey, node.getLeftSiblingKey());
    assertEquals("Right sibling with large key should be correct", 
        rightSibKey, node.getRightSiblingKey());
  }

  @Test
  public void testFactoryCreateMultipleNodes_SiblingsIndependent() {
    // Create multiple nodes to ensure sibling values don't interfere with each other
    final StringNode node1 = factory.createJsonStringNode(
        1L, 10L, 20L, "node1".getBytes(StandardCharsets.UTF_8), false, null
    );
    final StringNode node2 = factory.createJsonStringNode(
        1L, 30L, 40L, "node2".getBytes(StandardCharsets.UTF_8), false, null
    );
    final NumberNode node3 = factory.createJsonNumberNode(
        1L, 50L, 60L, 100.0, null
    );
    
    // Each node should have its own independent sibling values
    assertEquals("Node1 left sibling", 10L, node1.getLeftSiblingKey());
    assertEquals("Node1 right sibling", 20L, node1.getRightSiblingKey());
    
    assertEquals("Node2 left sibling", 30L, node2.getLeftSiblingKey());
    assertEquals("Node2 right sibling", 40L, node2.getRightSiblingKey());
    
    assertEquals("Node3 left sibling", 50L, node3.getLeftSiblingKey());
    assertEquals("Node3 right sibling", 60L, node3.getRightSiblingKey());
  }

  @Test
  public void testFactoryComparison_ObjectVsRegularNodes() {
    final byte[] stringValue = "test".getBytes(StandardCharsets.UTF_8);
    final double numberValue = 123.0;
    
    // Create regular value nodes (array elements) with siblings via factory
    final StringNode regularString = factory.createJsonStringNode(
        1L, 100L, 200L, stringValue, false, null
    );
    final NumberNode regularNumber = factory.createJsonNumberNode(
        1L, 300L, 400L, numberValue, null
    );
    
    // Create Object* value nodes (object properties) via factory
    final ObjectStringNode objectString = factory.createJsonObjectStringNode(
        1L, stringValue, false, null
    );
    final ObjectNumberNode objectNumber = factory.createJsonObjectNumberNode(
        1L, numberValue, null
    );
    
    // CRITICAL DIFFERENCE: Regular value nodes HAVE siblings (from factory parameters)
    assertEquals("Regular StringNode should have left sibling from factory", 
        100L, regularString.getLeftSiblingKey());
    assertEquals("Regular StringNode should have right sibling from factory", 
        200L, regularString.getRightSiblingKey());
    assertEquals("Regular NumberNode should have left sibling from factory", 
        300L, regularNumber.getLeftSiblingKey());
    assertEquals("Regular NumberNode should have right sibling from factory", 
        400L, regularNumber.getRightSiblingKey());
    
    // CRITICAL DIFFERENCE: Object* value nodes have NO siblings (inherent to their design)
    assertEquals("ObjectStringNode should have NO left sibling", 
        Fixed.NULL_NODE_KEY.getStandardProperty(), objectString.getLeftSiblingKey());
    assertEquals("ObjectStringNode should have NO right sibling", 
        Fixed.NULL_NODE_KEY.getStandardProperty(), objectString.getRightSiblingKey());
    assertEquals("ObjectNumberNode should have NO left sibling", 
        Fixed.NULL_NODE_KEY.getStandardProperty(), objectNumber.getLeftSiblingKey());
    assertEquals("ObjectNumberNode should have NO right sibling", 
        Fixed.NULL_NODE_KEY.getStandardProperty(), objectNumber.getRightSiblingKey());
  }
}
