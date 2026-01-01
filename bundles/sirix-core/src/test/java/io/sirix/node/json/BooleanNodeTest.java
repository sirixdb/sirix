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
 * Boolean node test.
 */
public class BooleanNodeTest {

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
    final BooleanNode node = new BooleanNode(
        13L, // nodeKey
        14L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        0, // hash
        true, // value
        hashFunction,
        (byte[]) null // deweyID
    );
    check(node);

    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .build();

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.BOOLEAN_VALUE.serialize(data, node, config);
    
    var bytesIn = data.asBytesIn();
    final BooleanNode node2 = (BooleanNode) NodeKind.BOOLEAN_VALUE.deserialize(
        bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final BooleanNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertTrue(node.getValue());
    assertEquals(NodeKind.BOOLEAN_VALUE, node.getKind());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
    assertTrue(node.hasLeftSibling());
  }
}
