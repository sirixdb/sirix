/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot.page;

import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.IndexType;
import io.sirix.io.HashAlgorithm;
import io.sirix.io.PageHasher;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * A bounded immutable complete image. Its envelope and ordered side trailer are validated once;
 * lookups validate the slot headers they visit until cached routing is prepared once for reuse. No
 * native page, mutable reference, input array or borrowed frame escapes. A directory may retain
 * this routing under its own cache byte charge.
 */
public final class HOTProjectionImage {
  public static final int MAX_BYTES = 64 * 1024;
  private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private final byte[] bytes;
  private final int prefixStart;
  private final int prefixLength;
  private final int count;
  private final int offsets;
  private final int slots;
  private final int used;
  private final int references;
  private final int referencesStart;
  private final @Nullable HOTLeafBody packedBody;
  private static final long[] NO_WORD_SLOTS = new long[0];
  private static final VarHandle WORD_SLOTS = wordSlotsHandle();
  private volatile long @Nullable [] wordSlots;
  private int indexedKeyLength = Long.BYTES;

  private HOTProjectionImage(final byte[] bytes, final int start) {
    this.bytes = bytes;
    final ByteArrayBytesIn source = new ByteArrayBytesIn(bytes);
    source.position(start);
    try {
      final byte flags = PageKind.readVersionAndFlagsAllowing(source, HOTLeafPage.SLOT_DIRECTORY_FLAGS);
      final int mode = HOTLeafBody.readMode(source, flags);
      HOTLeafBody.requireIndexMode(mode, IndexType.PROJECTION);
      final long pageKey = HOTLeafBody.readPageKey(source);
      final int revision = source.readInt();
      if (pageKey < 0 || revision < 0 || IndexType.getType(source.readByte()) != IndexType.PROJECTION) {
        throw new IllegalStateException("Invalid projection leaf header");
      }
      prefixLength = Short.toUnsignedInt(source.readShort());
      prefixStart = (int) source.position();
      if (source.remaining() < (long) prefixLength + 2 * Integer.BYTES) {
        throw new IllegalStateException("Truncated projection leaf prefix");
      }
      source.skip(prefixLength);
      count = source.readInt() & Integer.MAX_VALUE;
      used = source.readInt();
      HOTLeafBody.requireDirectory(flags, count, used, source.remaining());
      offsets = (int) source.position();
      if (mode == 0) {
        packedBody = null;
        slots = offsets + count * Short.BYTES;
        source.position(slots + used);
      } else {
        packedBody = HOTLeafBody.read(source, mode, count, used);
        slots = (int) source.position() - used;
      }
      references = (flags & HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS) == 0
          ? 0
          : HOTLeafBody.readSideReferenceCount(source);
      referencesStart = (int) source.position();
      if (source.remaining() != references * (2L * Long.BYTES)) {
        throw new IllegalStateException("Trailing projection leaf bytes");
      }
      long previous = 0;
      for (int i = 0, at = referencesStart; i < references; i++, at += 2 * Long.BYTES) {
        final long composite = longAt(at);
        if (longAt(at + Long.BYTES) < 0 || (i > 0 && composite <= previous)) {
          throw new IllegalStateException("Invalid or unordered projection side reference");
        }
        previous = composite;
      }
    } catch (final IndexOutOfBoundsException e) {
      throw new IllegalStateException("Truncated projection leaf image", e);
    }
  }

  /** The caller verified the entire frame and consumed its HOT-leaf kind byte. */
  public static @Nullable HOTProjectionImage copyIfFits(final BytesIn<?> source) {
    final long length = source.remaining();
    if (length > MAX_BYTES) {
      return null;
    }
    final byte[] bytes = new byte[(int) length];
    source.read(bytes);
    return new HOTProjectionImage(bytes, 0);
  }

  /**
   * Copy and verify an uncompressed frame once, including its kind byte. Hashing the owned bytes
   * avoids checked native-memory accesses before making this same copy. Neither the supplied buffer's
   * position nor any alias of its storage is retained.
   */
  public static HOTProjectionImage copyVerifiedFrame(final ByteBuffer source, final PageReference reference,
      final ResourceConfiguration configuration) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(reference);
    Objects.requireNonNull(configuration);
    if (source.remaining() < 1 || source.remaining() > MAX_BYTES) {
      throw new IllegalArgumentException("Complete projection image exceeds its bounded frame size");
    }
    final byte[] bytes = new byte[source.remaining()];
    source.get(source.position(), bytes);
    if (configuration.verifyChecksumsOnRead && reference.hasHash()) {
      final long actual = PageHasher.computeLong(bytes, configuration.hashAlgorithm);
      final long expected = reference.getHashAsLong();
      if (actual != expected) {
        throw new SirixCorruptionException(reference.getKey(), "compressed", HashAlgorithm.longToBytes(expected),
            HashAlgorithm.longToBytes(actual));
      }
    }
    if (PageKind.getKind(bytes[0]) != PageKind.HOT_LEAF_PAGE) {
      throw new IllegalStateException("Complete projection image is not a HOT leaf");
    }
    return new HOTProjectionImage(bytes, 1);
  }

  public @Nullable HOTLeafEntry select(final byte[] key, final long sideReferenceKey) {
    Objects.requireNonNull(key);
    if (prefixLength != 0 && (key.length < prefixLength
        || !Arrays.equals(bytes, prefixStart, prefixStart + prefixLength, key, 0, prefixLength))) {
      return null;
    }
    final long[] indexed = wordSlots;
    if (indexed != null && indexed.length != 0 && key.length == indexedKeyLength) {
      final long encoded = orderedKey(key);
      int low = 0;
      int high = count - 1;
      while (low <= high) {
        final int middle = (low + high) >>> 1;
        final int at = middle << 1;
        final int comparison = Long.compareUnsigned(indexed[at], encoded);
        if (comparison < 0) {
          low = middle + 1;
        } else if (comparison > 0) {
          high = middle - 1;
        } else {
          final long range = indexed[at + 1];
          final int start = (int) (range >>> Integer.SIZE);
          final int length = (int) range;
          return new HOTLeafEntry(Arrays.copyOfRange(bytes, start, start + length), sideReference(sideReferenceKey));
        }
      }
      return null;
    }
    if (packedBody != null) {
      final long ref = indexed == null
          ? packedBody.findValueRef(key, prefixLength)
          : packedBody.findValidatedValueRef(key, prefixLength);
      return ref == HOTLeafPage.NO_VALUE_REF
          ? null
          : new HOTLeafEntry(packedBody.copyValue(ref), sideReference(sideReferenceKey));
    }
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int offset = slotOffset(middle);
      if (offset < 0 || offset > used - 2 * Short.BYTES) {
        throw new IllegalStateException("Invalid projection slot offset");
      }
      final int suffixLength = shortAt(slots + offset);
      if (suffixLength > used - offset - 2 * Short.BYTES) {
        throw new IllegalStateException("Invalid projection slot key length");
      }
      final int suffix = slots + offset + Short.BYTES;
      final int comparison =
          Arrays.compareUnsigned(bytes, suffix, suffix + suffixLength, key, prefixLength, key.length);
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle - 1;
      } else {
        final int valueStart = suffix + suffixLength + Short.BYTES;
        final int length = shortAt(valueStart - Short.BYTES);
        if (length > slots + used - valueStart) {
          throw new IllegalStateException("Invalid projection slot value length");
        }
        return new HOTLeafEntry(Arrays.copyOfRange(bytes, valueStart, valueStart + length),
            sideReference(sideReferenceKey));
      }
    }
    return null;
  }

  /**
   * Prepare derived routing before cache publication, never for an unretained cold image. The array
   * is fully built before publication, and contenders derive identical bounds from immutable bytes.
   */
  public void prepareForCache() {
    if (wordSlots == null) {
      final long[] prepared = indexWordSlots();
      WORD_SLOTS.compareAndSet(this, null, prepared == null
          ? NO_WORD_SLOTS
          : prepared);
    }
  }

  private static VarHandle wordSlotsHandle() {
    try {
      return MethodHandles.lookup().findVarHandle(HOTProjectionImage.class, "wordSlots", long[].class);
    } catch (final ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static long orderedKey(final byte[] key) {
    if (key.length == Long.BYTES) {
      return Long.reverseBytes((long) LONGS.get(key, 0));
    }
    if (key.length == 1) {
      return Byte.toUnsignedInt(key[0]);
    }
    long encoded = 0;
    for (final byte value : key) {
      encoded = (encoded << Byte.SIZE) | Byte.toUnsignedInt(value);
    }
    return encoded;
  }

  /**
   * Projection slot keys normally fit one word. Decode their immutable bounds once, storing adjacent
   * key/range lanes for cache locality. Variable keys or malformed slots keep the checked selector;
   * an invalid record cannot enter the prevalidated directory.
   */
  private long @Nullable [] indexWordSlots() {
    if (packedBody != null) {
      packedBody.validate(prefixLength, IndexType.PROJECTION);
      return indexPackedWordSlots();
    }
    if (prefixLength > Long.BYTES || count == 0) {
      return null;
    }
    final int firstOffset = slotOffset(0);
    if (firstOffset < 0 || firstOffset > used - 2 * Short.BYTES
        || shortAt(slots + firstOffset) != Long.BYTES - prefixLength) {
      return null;
    }
    long prefix = 0;
    for (int i = 0; i < prefixLength; i++) {
      prefix = (prefix << Byte.SIZE) | (bytes[prefixStart + i] & 255L);
    }
    final long[] indexed = new long[count * 2];
    long previous = 0;
    for (int i = 0; i < count; i++) {
      final int offset = slotOffset(i);
      if (offset < 0 || offset > used - 2 * Short.BYTES) {
        return null;
      }
      final int length = shortAt(slots + offset);
      if (length != Long.BYTES - prefixLength || length > used - offset - 2 * Short.BYTES) {
        return null;
      }
      final int suffix = slots + offset + Short.BYTES;
      long encoded = prefix;
      for (int at = suffix; at < suffix + length; at++) {
        encoded = (encoded << Byte.SIZE) | (bytes[at] & 255L);
      }
      if (i > 0 && Long.compareUnsigned(previous, encoded) >= 0) {
        return null;
      }
      final int valueStart = suffix + length + Short.BYTES;
      final int valueLength = shortAt(valueStart - Short.BYTES);
      if (valueLength > slots + used - valueStart) {
        return null;
      }
      indexed[i * 2] = encoded;
      indexed[i * 2 + 1] = ((long) valueStart << Integer.SIZE) | valueLength;
      previous = encoded;
    }
    return indexed;
  }

  private long @Nullable [] indexPackedWordSlots() {
    final int suffixLength = packedBody.uniformSuffixLength();
    final int keyLength = prefixLength + suffixLength;
    if (suffixLength < 0 || keyLength <= 0 || keyLength > Long.BYTES) {
      return null;
    }
    long prefix = 0;
    for (int i = 0; i < prefixLength; i++) {
      prefix = (prefix << Byte.SIZE) | (bytes[prefixStart + i] & 255L);
    }
    final long[] indexed = new long[count * 2];
    long previous = 0;
    for (int i = 0; i < count; i++) {
      final long encoded = suffixLength == 0
          ? prefix
          : (prefix << (suffixLength * Byte.SIZE))
              | (packedBody.keyTailWord(i, 0) >>> ((Long.BYTES - suffixLength) * Byte.SIZE));
      if (i > 0 && Long.compareUnsigned(previous, encoded) >= 0) {
        throw new IllegalStateException("Unordered packed projection slot keys");
      }
      final long ref = packedBody.valueRef(i);
      indexed[i * 2] = encoded;
      indexed[i * 2 + 1] = ref + ((long) slots << Integer.SIZE);
      previous = encoded;
    }
    indexedKeyLength = keyLength;
    return indexed;
  }

  private @Nullable PageReference sideReference(final long key) {
    int low = 0;
    int high = references - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int at = referencesStart + middle * 2 * Long.BYTES;
      final long composite = longAt(at);
      if (composite < key) {
        low = middle + 1;
      } else if (composite > key) {
        high = middle - 1;
      } else {
        return new PageReference().setKey(longAt(at + Long.BYTES));
      }
    }
    return null;
  }

  private int slotOffset(final int entry) {
    return shortAt(offsets + entry * Short.BYTES);
  }

  private int shortAt(final int at) {
    return (bytes[at] & 255) | (bytes[at + 1] & 255) << Byte.SIZE;
  }

  private long longAt(final int at) {
    return (long) LONGS.get(bytes, at);
  }

  public long retainedSizeBytes() {
    final long[] indexed = wordSlots;
    return 72 + (packedBody == null
        ? 0
        : 128) + ((16L + bytes.length + 7) & ~7L)
        + (indexed == null || indexed.length == 0
            ? 0
            : 16L + (long) indexed.length * Long.BYTES);
  }
}
