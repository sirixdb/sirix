package io.sirix.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * String node test.
 */
public class StringNodeTest {

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
    final StringNode node = new StringNode(13L, // nodeKey
        14L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        0, // hash
        "hello world".getBytes(StandardCharsets.UTF_8), // value
        hashFunction, (byte[]) null // deweyID
    );
    check(node);

    final var config = ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.STRING_VALUE.serialize(data, node, config);

    var bytesIn = data.asBytesIn();
    final StringNode node2 = (StringNode) NodeKind.STRING_VALUE.deserialize(bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final StringNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals("hello world", node.getValue());
    assertEquals(NodeKind.STRING_VALUE, node.getKind());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
    assertTrue(node.hasLeftSibling());
  }
}
