/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 */

package io.sirix.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Array node test.
 */
public class ArrayNodeTest {

  private StorageEngineWriter pageTrx;

  private Database<JsonResourceSession> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNode() throws IOException {
    final var hashFunction = LongHashFunction.xx3();
    final ArrayNode node = new ArrayNode(
        13L, // nodeKey
        14L, // parentKey
        1L,  // pathNodeKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // firstChildKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // lastChildKey
        0, // childCount
        0, // descendantCount
        0, // hash
        hashFunction,
        (byte[]) null // deweyID
    );
    check(node);

    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .storeChildCount(false)
        .build();

    // Serialize and deserialize node.
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.ARRAY.serialize(data, node, config);
    
    var bytesIn = data.asBytesIn();
    final ArrayNode node2 = (ArrayNode) NodeKind.ARRAY.deserialize(
        bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final ArrayNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(1L, node.getPathNodeKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(NodeKind.ARRAY, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
  }
}
