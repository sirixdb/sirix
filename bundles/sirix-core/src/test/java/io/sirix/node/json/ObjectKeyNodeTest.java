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

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * ObjectKey node test.
 */
public class ObjectKeyNodeTest {

  private StorageEngineWriter pageTrx;
  private Database<JsonResourceSession> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNode() throws IOException {
    final var hashFunction = LongHashFunction.xx3();
    final ObjectKeyNode node = new ObjectKeyNode(
        13L, // nodeKey
        14L, // parentKey
        1L,  // pathNodeKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        17L, // firstChildKey (the value node)
        42,  // nameKey
        0, // descendantCount
        0, // hash
        hashFunction,
        (byte[]) null // deweyID
    );
    check(node);

    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .build();

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT_KEY.serialize(data, node, config);
    
    var bytesIn = data.asBytesIn();
    final ObjectKeyNode node2 = (ObjectKeyNode) NodeKind.OBJECT_KEY.deserialize(
        bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final ObjectKeyNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(1L, node.getPathNodeKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals(17L, node.getFirstChildKey());
    assertEquals(42, node.getNameKey());
    assertEquals(NodeKind.OBJECT_KEY, node.getKind());
    assertTrue(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
    assertTrue(node.hasLeftSibling());
  }
}
