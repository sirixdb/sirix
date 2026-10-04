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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /** Escaped value bytes a CAS key keeps at most. */
  private static final int CAS_MAX_VALUE_BYTES = (1 << Byte.SIZE) - CAS_HEADER_BYTES - 2 - 2 * Integer.BYTES;

  private static String[] samples() {
    return new String[] {"hello", "", "a", "a\0", "\0".repeat(CAS_MAX_VALUE_BYTES), "Ünïcödé", "日本語のフィールド名",
        "emoji 🚀 rocket", "unpaired \uD800 surrogate", "low \uDC00 surrogate", "mixed ascii then Ü",
        "x".repeat(CAS_MAX_VALUE_BYTES - 1), "x".repeat(CAS_MAX_VALUE_BYTES), "x".repeat(CAS_MAX_VALUE_BYTES + 50),
        "x".repeat(CAS_MAX_VALUE_BYTES) + "Ü", "x".repeat(CAS_MAX_VALUE_BYTES - 1) + "Ü",
        "Ü" + "x".repeat(CAS_MAX_VALUE_BYTES)};
  }

  /** Canonical CAS framing of the capped UTF-8 payload, independent of destination capacity. */
  private static byte[] referenceCasValueBytes(final String value) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    final ByteArrayOutputStream framed = new ByteArrayOutputStream();
    for (final byte valueByte : utf8) {
      final int width = valueByte == 0
          ? 2
          : 1;
      if (framed.size() + width > CAS_MAX_VALUE_BYTES) {
        break;
      }
      framed.write(valueByte);
      if (valueByte == 0) {
        framed.write(0xFF);
      }
    }
    framed.write(0);
    framed.write(0);
    return framed.toByteArray();
  }

  @Test
  void cappedEscapesKeepCompositeAndDeltaKeysAddressableAndOrdered() {
    final String prefix = "a".repeat(CAS_MAX_VALUE_BYTES - 1);
    final String[] values = {prefix, prefix + '\0', prefix + "\0a", prefix + 'a', prefix + "aa", prefix + 'b',
        "\0".repeat(130) + "200", "x".repeat(246)};
    for (final String value : values) {
      final CASValue key = new CASValue(new Str(value), Type.STR, 7);
      final byte[] bytes = new byte[1 << Byte.SIZE];
      final int baseLength = CASKeySerializer.INSTANCE.serializeWithChunkIdx(key, Integer.MAX_VALUE, bytes, 0);
      final int deltaLength = baseLength + Integer.BYTES;
      HOTKeySerializer.writeChunkIdxBE(bytes, baseLength, PostingDeltas.suffix(PostingDeltas.MAX_SEQ, true));
      assertTrue(deltaLength <= 1 << Byte.SIZE);
      assertEquals(baseLength - Integer.BYTES, CASKeySerializer.INSTANCE.logicalKeyLength(bytes, 0, deltaLength));
      for (final String other : values) {
        final CASValue otherKey = new CASValue(new Str(other), Type.STR, 7);
        final byte[] otherBytes = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(otherKey)];
        final int otherLength = CASKeySerializer.INSTANCE.serialize(otherKey, otherBytes, 0);
        final int keyOrder = Arrays.compareUnsigned(bytes, 0, baseLength - Integer.BYTES, otherBytes, 0, otherLength);
        assertTrue(Integer.signum(keyOrder) * Integer.signum(value.compareTo(other)) >= 0);
      }
    }
    final CASValue longest = new CASValue(new Str("x".repeat(CAS_MAX_VALUE_BYTES)), Type.STR, 7);
    final byte[] bytes = new byte[1 << Byte.SIZE];
    assertEquals(bytes.length - Integer.BYTES, CASKeySerializer.INSTANCE.serializeWithChunkIdx(longest, 0, bytes, 0));
    assertThrows(IllegalArgumentException.class, () -> NameKeySerializer.INSTANCE.serializeWithChunkIdx(
        new QNm("x".repeat((1 << Byte.SIZE) - Integer.BYTES + 1)), 0, new byte[(1 << Byte.SIZE) + 1], 0));
  }

  @Test
  @DisplayName("CAS values frame the capped UTF-8 bytes without capacity-dependent truncation")
  void casStringValuesMatchReferenceEncoding() {
    for (final String value : samples()) {
      final CASValue key = new CASValue(new Str(value), Type.STR, 7);
      final byte[] expected = referenceCasValueBytes(value);
      for (final int capacity : new int[] {CASKeySerializer.INSTANCE.maxSerializedLength(key),
          CAS_HEADER_BYTES + expected.length}) {
        final byte[] dest = new byte[capacity];
        final int length = CASKeySerializer.INSTANCE.serialize(key, dest, 0);
        assertArrayEquals(expected, Arrays.copyOfRange(dest, CAS_HEADER_BYTES, length),
            "value bytes at capacity " + capacity);
      }
      final byte[] tooSmall = new byte[CAS_HEADER_BYTES + expected.length - 1];
      assertThrows(IndexOutOfBoundsException.class, () -> CASKeySerializer.INSTANCE.serialize(key, tooSmall, 0));
    }
  }

  @Test
  @DisplayName("CAS MemorySegment serialization matches byte arrays, including long escaped values")
  void casStringValuesSerializeToMemorySegments() {
    try (final Arena arena = Arena.ofConfined()) {
      for (final String value : samples()) {
        final CASValue key = new CASValue(new Str(value), Type.STR, 7);
        final byte[] serialized = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(key)];
        final int length = CASKeySerializer.INSTANCE.serialize(key, serialized, 0);
        for (final int offset : new int[] {0, 7}) {
          final byte[] expected = new byte[offset + length + 1];
          Arrays.fill(expected, (byte) 0x5A);
          System.arraycopy(serialized, 0, expected, offset, length);
          for (final MemorySegment dest : new MemorySegment[] {MemorySegment.ofArray(new byte[expected.length]),
              arena.allocate(expected.length)}) {
            dest.fill((byte) 0x5A);
            assertEquals(length, CASKeySerializer.INSTANCE.serializeTo(key, dest, offset));
            assertArrayEquals(expected, dest.toArray(ValueLayout.JAVA_BYTE));
          }
        }
      }
    }
  }

  @Test
  @DisplayName("CAS MemorySegment serialization preserves argument and bounds validation")
  void casMemorySegmentSerializationRejectsInvalidArguments() {
    final CASValue key = new CASValue(new Str("x".repeat(CAS_MAX_VALUE_BYTES)), Type.STR, 7);
    final byte[] serialized = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(key)];
    final int length = CASKeySerializer.INSTANCE.serialize(key, serialized, 0);
    final MemorySegment dest = MemorySegment.ofArray(new byte[length]);
    assertThrows(NullPointerException.class, () -> CASKeySerializer.INSTANCE.serializeTo(null, dest, 0));
    assertThrows(NullPointerException.class, () -> CASKeySerializer.INSTANCE.serializeTo(key, null, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> CASKeySerializer.INSTANCE.serializeTo(key, dest, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> CASKeySerializer.INSTANCE.serializeTo(key, dest, 1));
    assertThrows(IndexOutOfBoundsException.class,
        () -> CASKeySerializer.INSTANCE.serializeTo(key, dest.asSlice(0, length - 1), 0));
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
  @DisplayName("NAME namespace URIs encode as UTF-8 independently of the prefix")
  void nameUrisMatchReferenceEncoding() {
    for (final String uri : new String[] {"urn:ns", "urn:nsÜ", "urn:🚀", "unpaired \uD800"}) {
      final byte[] dest = new byte[2048];
      final int length = NameKeySerializer.INSTANCE.serialize(new QNm(uri, "ignored", "local"), dest, 0);
      final byte[] uriBytes = uri.getBytes(StandardCharsets.UTF_8);
      final byte[] expected = new byte[3 + uriBytes.length + "local".length()];
      expected[0] = (byte) 0xFF;
      System.arraycopy(uriBytes, 0, expected, 1, uriBytes.length);
      System.arraycopy("local".getBytes(StandardCharsets.UTF_8), 0, expected, 3 + uriBytes.length, "local".length());
      assertArrayEquals(expected, Arrays.copyOf(dest, length), "URI bytes for \"" + uri + '\"');
    }
  }
}
