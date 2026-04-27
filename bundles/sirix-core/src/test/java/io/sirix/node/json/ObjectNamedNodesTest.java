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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Round-trip tests for the fused OBJECT_NAMED_* node kinds introduced in iter#28
 * (task #62). Each new kind carries an OBJECT_KEY's name + structural fields AND
 * the primitive payload in a single slotted-page record.
 *
 * <p>Exercises both the {@link NodeKind#serialize(io.sirix.node.BytesOut, io.sirix.node.interfaces.DataRecord, io.sirix.access.ResourceConfiguration)}
 * / {@link NodeKind#deserialize} legacy-bytes round-trip and the constructor / getter
 * contract. The FlyweightNode page-memory binding is exercised indirectly through
 * the broader fuzz/integration suites once emission is wired up.
 */
public class ObjectNamedNodesTest {

  private StorageEngineWriter storageEngineWriter;
  private Database<JsonResourceSession> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    storageEngineWriter = database.beginResourceSession(JsonTestHelper.RESOURCE).createStorageEngineWriter();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  // ==================== OBJECT_NAMED_NULL ====================

  @Test
  public void testNamedNullRoundTrip() throws IOException {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final ObjectNamedNullNode node = new ObjectNamedNullNode(
        13L,                // nodeKey
        14L,                // parentKey
        15L,                // rightSibling
        16L,                // leftSibling
        42,                 // nameKey
        101L,               // pathNodeKey
        Constants.NULL_REVISION_NUMBER,
        0,                  // lastModRev
        0L,                 // hash
        hashFunction,
        (byte[]) null);
    checkNamedNull(node);

    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();

    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT_NAMED_NULL.serialize(data, node, config);

    var bytesIn = data.asBytesIn();
    final ObjectNamedNullNode node2 =
        (ObjectNamedNullNode) NodeKind.OBJECT_NAMED_NULL.deserialize(bytesIn, node.getNodeKey(), null, config);
    checkNamedNull(node2);
  }

  private void checkNamedNull(final ObjectNamedNullNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(15L, node.getRightSiblingKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(42, node.getNameKey());
    assertEquals(101L, node.getPathNodeKey());
    assertEquals(NodeKind.OBJECT_NAMED_NULL, node.getKind());
    assertTrue(node.hasParent());
    assertFalse(node.hasFirstChild());
    assertEquals(0L, node.getChildCount());
    assertEquals(0L, node.getDescendantCount());
  }

  // ==================== OBJECT_NAMED_BOOLEAN ====================

  @Test
  public void testNamedBooleanRoundTripTrue() throws IOException {
    testNamedBooleanRoundTrip(true);
  }

  @Test
  public void testNamedBooleanRoundTripFalse() throws IOException {
    testNamedBooleanRoundTrip(false);
  }

  private void testNamedBooleanRoundTrip(final boolean value) throws IOException {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final ObjectNamedBooleanNode node = new ObjectNamedBooleanNode(
        13L, 14L, 15L, 16L, 42, 101L, Constants.NULL_REVISION_NUMBER, 0, 0L, value,
        hashFunction, (byte[]) null);
    checkNamedBoolean(node, value);

    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT_NAMED_BOOLEAN.serialize(data, node, config);

    var bytesIn = data.asBytesIn();
    final ObjectNamedBooleanNode node2 =
        (ObjectNamedBooleanNode) NodeKind.OBJECT_NAMED_BOOLEAN.deserialize(bytesIn, node.getNodeKey(), null, config);
    checkNamedBoolean(node2, value);
  }

  private void checkNamedBoolean(final ObjectNamedBooleanNode node, final boolean value) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(15L, node.getRightSiblingKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(42, node.getNameKey());
    assertEquals(101L, node.getPathNodeKey());
    assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, node.getKind());
    assertEquals(value, node.getValue());
    assertTrue(node.hasParent());
    assertFalse(node.hasFirstChild());
  }

  // ==================== OBJECT_NAMED_NUMBER ====================

  @Test
  public void testNamedNumberRoundTripInt() throws IOException {
    testNamedNumberRoundTrip(42);
  }

  @Test
  public void testNamedNumberRoundTripLong() throws IOException {
    testNamedNumberRoundTrip(1234567890123L);
  }

  @Test
  public void testNamedNumberRoundTripDouble() throws IOException {
    testNamedNumberRoundTrip(3.1415);
  }

  @Test
  public void testNamedNumberRoundTripFloat() throws IOException {
    testNamedNumberRoundTrip(2.718f);
  }

  private void testNamedNumberRoundTrip(final Number value) throws IOException {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(
        13L, 14L, 15L, 16L, 42, 101L, Constants.NULL_REVISION_NUMBER, 0, 0L, value,
        hashFunction, (byte[]) null);
    checkNamedNumber(node, value);

    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT_NAMED_NUMBER.serialize(data, node, config);

    var bytesIn = data.asBytesIn();
    final ObjectNamedNumberNode node2 =
        (ObjectNamedNumberNode) NodeKind.OBJECT_NAMED_NUMBER.deserialize(bytesIn, node.getNodeKey(), null, config);
    checkNamedNumber(node2, value);
  }

  private void checkNamedNumber(final ObjectNamedNumberNode node, final Number value) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(15L, node.getRightSiblingKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(42, node.getNameKey());
    assertEquals(101L, node.getPathNodeKey());
    assertEquals(NodeKind.OBJECT_NAMED_NUMBER, node.getKind());
    assertEquals(value, node.getValue());
    assertTrue(node.hasParent());
  }

  // ==================== OBJECT_NAMED_STRING ====================

  @Test
  public void testNamedStringRoundTrip() throws IOException {
    testNamedStringRoundTripBytes("hello world".getBytes(Constants.DEFAULT_ENCODING));
  }

  @Test
  public void testNamedStringRoundTripEmpty() throws IOException {
    testNamedStringRoundTripBytes(new byte[0]);
  }

  @Test
  public void testNamedStringRoundTripLarge() throws IOException {
    final byte[] payload = new byte[1024];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i & 0xFF);
    }
    testNamedStringRoundTripBytes(payload);
  }

  private void testNamedStringRoundTripBytes(final byte[] value) throws IOException {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final ObjectNamedStringNode node = new ObjectNamedStringNode(
        13L, 14L, 15L, 16L, 42, 101L, Constants.NULL_REVISION_NUMBER, 0, 0L, value,
        hashFunction, (byte[]) null);
    checkNamedString(node, value);

    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT_NAMED_STRING.serialize(data, node, config);

    var bytesIn = data.asBytesIn();
    final ObjectNamedStringNode node2 =
        (ObjectNamedStringNode) NodeKind.OBJECT_NAMED_STRING.deserialize(bytesIn, node.getNodeKey(), null, config);
    checkNamedString(node2, value);
  }

  private void checkNamedString(final ObjectNamedStringNode node, final byte[] value) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(15L, node.getRightSiblingKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(42, node.getNameKey());
    assertEquals(101L, node.getPathNodeKey());
    assertEquals(NodeKind.OBJECT_NAMED_STRING, node.getKind());
    assertTrue(node.hasParent());
    final byte[] actual = node.getRawValueWithoutDecompression();
    assertNotNull(actual);
    assertEquals(value.length, actual.length);
    for (int i = 0; i < value.length; i++) {
      assertEquals("byte[" + i + "]", value[i], actual[i]);
    }
  }

  // ==================== snapshot semantics ====================

  @Test
  public void testNamedBooleanSnapshotIndependence() {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final ObjectNamedBooleanNode a = new ObjectNamedBooleanNode(
        1L, 2L, 3L, 4L, 5, 6L, 0, 0, 0L, true, hashFunction, (byte[]) null);
    final ObjectNamedBooleanNode snap = a.toSnapshot();
    assertEquals(a, snap);
    assertEquals(1L, snap.getNodeKey());
  }

  @Test
  public void testNamedStringSnapshotIndependence() {
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final byte[] v = "hi".getBytes(Constants.DEFAULT_ENCODING);
    final ObjectNamedStringNode a = new ObjectNamedStringNode(
        1L, 2L, 3L, 4L, 5, 6L, 0, 0, 0L, v, hashFunction, (byte[]) null);
    final ObjectNamedStringNode snap = a.toSnapshot();
    assertEquals(a, snap);
    // ensure value array was cloned (independent mutation does not leak)
    final byte[] snapRaw = snap.getRawValueWithoutDecompression();
    assertNotNull(snapRaw);
    snapRaw[0] = (byte) 'X';
    final byte[] origRaw = a.getRawValueWithoutDecompression();
    assertEquals((byte) 'h', origRaw[0]);
  }

  // ==================== NodeKind registry ====================

  @Test
  public void testNodeKindRegistryLookup() {
    // iter#30: fused-kind byte IDs shifted from 44-47 -> 48-51 to avoid collision
    // with PROJECTION_INDEX_LEAF (byte 44) on the iter#22 FOR-BP campaign branch.
    assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, NodeKind.getKind((byte) 48));
    assertEquals(NodeKind.OBJECT_NAMED_NUMBER, NodeKind.getKind((byte) 49));
    assertEquals(NodeKind.OBJECT_NAMED_STRING, NodeKind.getKind((byte) 50));
    assertEquals(NodeKind.OBJECT_NAMED_NULL, NodeKind.getKind((byte) 51));
  }
}
