package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NameKeyExpandedNameTest {
  @Test
  void keyIdentityIsUriAndLocalRegardlessOfPrefix() {
    for (final String prefix : List.of("", "p", "long".repeat(100), "前缀")) {
      final QNm a = new QNm("urn:a", prefix, "item");
      assertArrayEquals(encode(a), encode(new QNm("urn:a", "alias", "item")));
      assertFalse(Arrays.equals(encode(a), encode(new QNm("urn:b", prefix, "item"))));
      assertFalse(Arrays.equals(encode(a), encode(new QNm("item"))));
      final byte[] bytes = encode(a);
      assertEquals(a, NameKeySerializer.INSTANCE.deserialize(bytes, 0, bytes.length));
    }
  }

  @Test
  void offsetAndBoundCoverUnicodeLongUrisAndComponentBoundaries() {
    for (final String uri : List.of("", "urn:a", "urn:" + "長🚀".repeat(150), "urn:a\u0000b", "urn:a\u0000")) {
      final QNm name = new QNm(uri, "prefix", "名🚀");
      final byte[] bytes = new byte[NameKeySerializer.INSTANCE.maxSerializedLength(name) + 14];
      final int length = NameKeySerializer.INSTANCE.serialize(name, bytes, 7);
      assertTrue(length <= bytes.length - 14);
      assertEquals(name, NameKeySerializer.INSTANCE.deserialize(bytes, 7, length));
    }
    assertFalse(Arrays.equals(encode(new QNm("ab", "", "c")), encode(new QNm("a", "", "bc"))));
  }

  @Test
  void namespacesSortBeforeTheirExtensionsAndLocalNamesBreakTies() {
    final List<QNm> ordered = List.of(new QNm("z"), new QNm("urn:a", "p", "a"), new QNm("urn:a", "q", "b"),
        new QNm("urn:aa", "", "a"), new QNm("urn:b", "", "a"));
    for (int i = 1; i < ordered.size(); i++) {
      assertTrue(Arrays.compareUnsigned(encode(ordered.get(i - 1)), encode(ordered.get(i))) < 0);
    }
  }

  @Test
  void missingNamespaceTerminatorOrLocalNameIsRejected() {
    final byte[] key = encode(new QNm("urn:a", "", "x"));
    assertThrows(IllegalArgumentException.class, () -> NameKeySerializer.INSTANCE.deserialize(key, 0, key.length - 1));
    assertThrows(IllegalArgumentException.class, () -> NameKeySerializer.INSTANCE.deserialize(key, 0, 3));
  }

  private static byte[] encode(final QNm name) {
    final byte[] bytes = new byte[NameKeySerializer.INSTANCE.maxSerializedLength(name)];
    final int length = NameKeySerializer.INSTANCE.serialize(name, bytes, 0);
    return Arrays.copyOf(bytes, length);
  }
}
