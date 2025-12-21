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
 * Null node test.
 */
public class NullNodeTest {

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
  public void test() throws IOException {
    final var hashFunction = LongHashFunction.xx3();
    final NullNode node = new NullNode(
        13L, // nodeKey
        14L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        0, // hash
        hashFunction,
        (byte[]) null // deweyID
    );
    check(node);

    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .build();

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.NULL_VALUE.serialize(data, node, config);
    
    var bytesIn = data.asBytesIn();
    final NullNode node2 = (NullNode) NodeKind.NULL_VALUE.deserialize(
        bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final NullNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals(NodeKind.NULL_VALUE, node.getKind());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
    assertTrue(node.hasLeftSibling());
  }
}
