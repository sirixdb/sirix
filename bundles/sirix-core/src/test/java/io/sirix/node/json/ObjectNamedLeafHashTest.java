package io.sirix.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.page.FsstAwareSlotCopier;
import io.sirix.page.NodeFieldLayout;
import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Pins the existing leaf hash inputs independently of the record's storage representation. */
final class ObjectNamedLeafHashTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("leaf-hash").hashKind(HashType.ROLLING).build();
  private static final LongHashFunction HASH_FUNCTION = CONFIG.nodeHashFunction;

  static Stream<Arguments> leaves() {
    return Stream.of(Arguments.of(NodeKind.OBJECT_NAMED_BOOLEAN, false),
        Arguments.of(NodeKind.OBJECT_NAMED_BOOLEAN, true), Arguments.of(NodeKind.OBJECT_NAMED_NULL, null),
        Arguments.of(NodeKind.OBJECT_NAMED_STRING, ""), Arguments.of(NodeKind.OBJECT_NAMED_STRING, "Grüße 🌲"),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, Integer.MIN_VALUE), Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 10),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 10L), Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 10.0d),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 10.0f), Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, Long.MIN_VALUE),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 9_007_199_254_740_993L),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, -0.0d), Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, 1.25f),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, new BigInteger("123456789012345678901234567890")),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, new BigDecimal("10.0")),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, new BigDecimal("10.00")),
        Arguments.of(NodeKind.OBJECT_NAMED_NUMBER, new BigDecimal("12345678901234567890.00100")));
  }

  @Test
  void compressedStringHashUsesSemanticUtf8AndSurvivesSnapshot() {
    final byte[] symbols = {1, 2, 'a', 'b'};
    final byte[] compressed = {FSSTCompressor.HEADER_COMPRESSED, 0, 0, 0, 0};
    final ObjectNamedStringNode node = new ObjectNamedStringNode(13, 11, 15, 17, 42, 31, -1, 1, 0, compressed,
        HASH_FUNCTION, (byte[]) null, true, symbols);
    final long expected = expectedHash(node, "abababab");
    assertEquals(expected, node.getHash());
    final MemorySegment page = MemorySegment.ofArray(new byte[512]);
    final int size = node.serializeToHeap(page, 0);
    node.bind(page, 0, 13, 0);
    assertEquals(expected, node.getHash());
    assertEquals(expected, node.toSnapshot().getHash());
    node.unbind();
    assertEquals(expected, node.getHash());

    final byte[] decoded =
        new FsstAwareSlotCopier(symbols).decompressSlot(page.asSlice(0, size), NodeKind.OBJECT_NAMED_STRING.getId());
    assertNotNull(decoded, "version-fragment copying must decode the current leaf layout");
    node.bind(MemorySegment.ofArray(decoded), 0, 13, 0);
    assertEquals(expected, node.getHash());
    assertArrayEquals("abababab".getBytes(StandardCharsets.UTF_8), node.getRawValue());
  }

  @ParameterizedTest
  @MethodSource("leaves")
  void hashesSurviveEveryRepresentationWithoutAStoredField(final NodeKind kind, final Object value) {
    final StructNode node = create(kind, value);
    final long expected = expectedHash(node, value);
    assertEquals(expected, node.getHash());
    assertEquals(expected, node.computeHash(Bytes.threadLocalHashBuffer()));

    try (final BytesOut<?> encoded = Bytes.elasticHeapByteBuffer()) {
      kind.serialize(encoded, node, CONFIG);
      final byte[] wire = encoded.toByteArray();
      final ResourceConfiguration noHashes =
          ResourceConfiguration.newBuilder("no-hashes").hashKind(HashType.NONE).build();
      encoded.clear();
      kind.serialize(encoded, node, noHashes);
      assertArrayEquals(wire, encoded.toByteArray(), "leaf wire must not contain a configured hash field");

      final BytesIn<?> source = Bytes.wrapForRead(wire);
      final StructNode decoded = (StructNode) kind.deserialize(source, node.getNodeKey(), null, CONFIG);
      assertEquals(wire.length, source.position());
      assertEquals(expected, decoded.getHash());

      final StructNode lazy = create(kind, value);
      readLazy(lazy, Bytes.wrapForRead(wire));
      assertEquals(expected, lazy.getHash());
    }

    final FlyweightNode flyweight = (FlyweightNode) node;
    final MemorySegment page = MemorySegment.ofArray(new byte[1024]);
    final int size = flyweight.serializeToHeap(page, 0);
    assertEquals(expectedSize(kind, value), size, "hash and its directory entry must both be absent");
    assertEquals(-1, NodeFieldLayout.hashFieldIndexForKind(kind.getId()));
    flyweight.bind(page, 0, node.getNodeKey(), 0);
    assertEquals(expected, node.getHash());
    assertEquals(expected, ((StructNode) flyweight.toSnapshot()).getHash());

    // Same-width mutations exercise direct page writes and must never leave a stale cached hash.
    node.setParentKey(12);
    node.setLeftSiblingKey(14);
    node.setRightSiblingKey(16);
    ((NameNode) node).setLocalNameKey(43);
    final long changed = expectedHash(node, value);
    assertNotEquals(expected, changed);
    assertEquals(changed, node.getHash());
    node.setHash(expected);
    assertEquals(changed, node.getHash(), "a leaf hash cannot override the hash of its current inputs");
    flyweight.unbind();
    assertEquals(changed, node.getHash());
    assertEquals(changed, ((StructNode) flyweight.toSnapshot()).getHash());
  }

  private static StructNode create(final NodeKind kind, final Object value) {
    return switch (kind) {
      case OBJECT_NAMED_BOOLEAN ->
        new ObjectNamedBooleanNode(13, 11, 15, 17, 42, 31, -1, 1, 0, (boolean) value, HASH_FUNCTION, (byte[]) null);
      case OBJECT_NAMED_NUMBER ->
        new ObjectNamedNumberNode(13, 11, 15, 17, 42, 31, -1, 1, 0, (Number) value, HASH_FUNCTION, (byte[]) null);
      case OBJECT_NAMED_STRING -> new ObjectNamedStringNode(13, 11, 15, 17, 42, 31, -1, 1, 0,
          ((String) value).getBytes(StandardCharsets.UTF_8), HASH_FUNCTION, (byte[]) null);
      case OBJECT_NAMED_NULL -> new ObjectNamedNullNode(13, 11, 15, 17, 42, 31, -1, 1, 0, HASH_FUNCTION, (byte[]) null);
      default -> throw new AssertionError(kind);
    };
  }

  private static void readLazy(final StructNode node, final BytesIn<?> source) {
    switch (node) {
      case ObjectNamedBooleanNode leaf -> leaf.readFrom(source, 13, null, HASH_FUNCTION, CONFIG);
      case ObjectNamedNumberNode leaf -> leaf.readFrom(source, 13, null, HASH_FUNCTION, CONFIG);
      case ObjectNamedStringNode leaf -> leaf.readFrom(source, 13, null, HASH_FUNCTION, CONFIG);
      case ObjectNamedNullNode leaf -> leaf.readFrom(source, 13, null, HASH_FUNCTION, CONFIG);
      default -> throw new AssertionError(node);
    }
  }

  private static int expectedSize(final NodeKind kind, final Object value) {
    // Seven one-byte metadata varints; the name 42 occupies one byte. No hash bytes.
    final int metadata = 1 + NodeFieldLayout.fieldCountForKind(kind.getId()) + 7;
    return metadata + switch (kind) {
      case OBJECT_NAMED_BOOLEAN -> 1;
      case OBJECT_NAMED_NULL -> 0;
      case OBJECT_NAMED_STRING -> {
        final int length = ((String) value).getBytes(StandardCharsets.UTF_8).length;
        yield 2 + length;
      }
      case OBJECT_NAMED_NUMBER -> {
        try (final BytesOut<?> bytes = Bytes.elasticHeapByteBuffer()) {
          NodeKind.serializeNumber((Number) value, bytes);
          yield Math.toIntExact(bytes.position());
        }
      }
      default -> throw new AssertionError(kind);
    };
  }

  private static long expectedHash(final StructNode node, final Object value) {
    try (final BytesOut<?> bytes = Bytes.elasticHeapByteBuffer()) {
      bytes.writeLong(node.getNodeKey())
           .writeLong(node.getParentKey())
           .writeByte(node.getKind().getId())
           .writeLong(node.getLeftSiblingKey())
           .writeLong(node.getRightSiblingKey())
           .writeInt(((NameNode) node).getLocalNameKey());
      switch (value) {
        case null -> {
        }
        case Boolean booleanValue -> bytes.writeBoolean(booleanValue);
        case String stringValue -> bytes.write(stringValue.getBytes(StandardCharsets.UTF_8));
        case Float floatValue -> bytes.writeFloat(floatValue);
        case Double doubleValue -> bytes.writeDouble(doubleValue);
        case Integer intValue -> bytes.writeInt(intValue);
        case Long longValue -> bytes.writeLong(longValue);
        case BigInteger bigInteger -> bytes.writeBigInteger(bigInteger);
        case BigDecimal bigDecimal -> bytes.writeBigDecimal(bigDecimal);
        default -> throw new AssertionError(value);
      }
      return bytes.hashDirect(HASH_FUNCTION);
    }
  }
}
