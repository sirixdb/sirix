package io.sirix.node;

import io.brackit.query.atomic.QNm;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.node.delegates.NodeDelegate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Round-trips a NAMERB (name red-black index) node with non-ASCII names to pin the on-disk
 * string encoding to UTF-8 — the serializer must never fall back to the platform-default
 * charset, or names corrupt on any JVM whose default is not UTF-8.
 */
public final class NameRBNodeEncodingTest {

  private StorageEngineWriter storageEngineWriter;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    storageEngineWriter = database.beginResourceSession(JsonTestHelper.RESOURCE).createStorageEngineWriter();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void nonAsciiNameRoundTripsAsUtf8() {
    final ResourceConfiguration config = storageEngineWriter.getResourceSession().getResourceConfig();
    final QNm name = new QNm("https://example.org/ünïcode", "präfix", "地方élément");
    final NodeDelegate nodeDelegate = new NodeDelegate(7, 3, config.nodeHashFunction, 0, 0, (byte[]) null);
    final RBNodeKey<QNm> node = new RBNodeKey<>(name, 42, nodeDelegate);
    node.setLeftChildKey(11);
    node.setRightChildKey(13);
    node.setChanged(true);

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.NAMERB.serialize(data, node, config);

    @SuppressWarnings("unchecked")
    final RBNodeKey<QNm> deserialized =
        (RBNodeKey<QNm>) NodeKind.NAMERB.deserialize(data.asBytesIn(), node.getNodeKey(), null, config);

    assertEquals(name, deserialized.getKey());
    assertEquals(42, deserialized.getValueNodeKey());
    assertEquals(11, deserialized.getLeftChildKey());
    assertEquals(13, deserialized.getRightChildKey());
    assertTrue(deserialized.isChanged());
  }
}
