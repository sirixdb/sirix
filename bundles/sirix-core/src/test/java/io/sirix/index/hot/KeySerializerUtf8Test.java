/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The CAS and NAME key serializers write the US-ASCII case straight into the destination buffer
 * instead of through a throwaway {@code byte[]}, because they run once per indexed node. The bytes
 * must be exactly what {@code String.getBytes(UTF_8)} would have produced — including where the
 * value is truncated to the key cap, and including the inputs that send the encoder down the
 * fallback: multi-byte code points, surrogate pairs, and the unpaired surrogate that
 * {@code getBytes} replaces with {@code '?'}.
 */
@DisplayName("Key serializer UTF-8 encoding")
final class KeySerializerUtf8Test {

  /** Header bytes a CAS key writes before the value: 8 for the path node key, 2 for the type. */
  private static final int CAS_HEADER_BYTES = 10;

  /** Value bytes a CAS key keeps at most; the serializer truncates past this. */
  private static final int CAS_MAX_VALUE_BYTES = 246;

  private static String[] samples() {
    return new String[] {"hello", "", "a", "Ünïcödé", "日本語のフィールド名", "emoji 🚀 rocket", "unpaired \uD800 surrogate",
        "low \uDC00 surrogate", "mixed ascii then Ü", "x".repeat(CAS_MAX_VALUE_BYTES - 1),
        "x".repeat(CAS_MAX_VALUE_BYTES), "x".repeat(CAS_MAX_VALUE_BYTES + 50), "x".repeat(CAS_MAX_VALUE_BYTES) + "Ü",
        "x".repeat(CAS_MAX_VALUE_BYTES - 1) + "Ü", "Ü" + "x".repeat(CAS_MAX_VALUE_BYTES)};
  }

  /** What the CAS serializer's value region held before the ASCII fast path existed. */
  private static byte[] referenceCasValueBytes(final String value, final int capacity) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    final int cap = Math.min(capacity - CAS_HEADER_BYTES, CAS_MAX_VALUE_BYTES);
    return Arrays.copyOf(utf8, Math.min(utf8.length, cap));
  }

  @Test
  @DisplayName("CAS string values encode exactly as getBytes(UTF_8), truncated to the key cap")
  void casStringValuesMatchReferenceEncoding() {
    for (final String value : samples()) {
      if (value.isEmpty()) {
        continue; // an empty value has no key; CASKeySerializer rejects it separately
      }
      for (final int capacity : new int[] {512, CAS_HEADER_BYTES + 32, CAS_HEADER_BYTES + CAS_MAX_VALUE_BYTES}) {
        final byte[] dest = new byte[capacity];
        final int length = CASKeySerializer.INSTANCE.serialize(new CASValue(new Str(value), Type.STR, 7), dest, 0);
        assertArrayEquals(referenceCasValueBytes(value, capacity), Arrays.copyOfRange(dest, CAS_HEADER_BYTES, length),
            "value bytes for \"" + value + "\" at capacity " + capacity);
      }
    }
  }

  @Test
  @DisplayName("NAME local names encode exactly as getBytes(UTF_8)")
  void nameLocalNamesMatchReferenceEncoding() {
    for (final String name : samples()) {
      if (name.isEmpty()) {
        continue; // NameKeySerializer rejects an empty local name
      }
      final byte[] dest = new byte[2048];
      final int length = NameKeySerializer.INSTANCE.serialize(new QNm(name), dest, 0);
      assertArrayEquals(name.getBytes(StandardCharsets.UTF_8), Arrays.copyOf(dest, length),
          "name bytes for \"" + name + '"');
    }
  }

  @Test
  @DisplayName("NAME prefixes encode exactly as getBytes(UTF_8)")
  void namePrefixesMatchReferenceEncoding() {
    for (final String prefix : new String[] {"ns", "nsÜ", "🚀", "unpaired \uD800"}) {
      final byte[] dest = new byte[2048];
      final int length = NameKeySerializer.INSTANCE.serialize(new QNm("http://example.org", prefix, "local"), dest, 0);

      final byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
      final byte[] expected = new byte[2 + prefixBytes.length + "local".length()];
      expected[0] = (byte) 0xFF;
      expected[1] = (byte) prefixBytes.length;
      System.arraycopy(prefixBytes, 0, expected, 2, prefixBytes.length);
      System.arraycopy("local".getBytes(StandardCharsets.UTF_8), 0, expected, 2 + prefixBytes.length, "local".length());

      assertArrayEquals(expected, Arrays.copyOf(dest, length), "prefixed name bytes for \"" + prefix + '"');
    }
  }
}
