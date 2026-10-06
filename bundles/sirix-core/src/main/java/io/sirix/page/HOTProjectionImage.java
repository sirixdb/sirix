/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.IndexType;
import io.sirix.io.HashAlgorithm;
import io.sirix.io.PageHasher;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.Utils;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * A bounded immutable complete image. Its envelope and ordered side trailer are validated once;
 * each lookup validates the slot headers it visits. No native page, mutable reference, input array
 * or borrowed frame escapes. A directory may retain this routing under its own cache byte charge.
 */
public final class HOTProjectionImage {
  public static final int MAX_BYTES = 64 * 1024;
  private static final VarHandle INTS = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
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

  private HOTProjectionImage(final byte[] bytes, final int start) {
    this.bytes = bytes;
    final ByteArrayBytesIn source = new ByteArrayBytesIn(bytes);
    source.position(start);
    try {
      final byte flags = PageKind.readVersionAndFlagsAllowing(source, HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS);
      final long pageKey = Utils.getVarLong(source);
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
      if (count > HOTLeafPage.MAX_ENTRIES || used < 0 || used > HOTLeafPage.DEFAULT_SIZE
          || (long) count * Integer.BYTES + used > source.remaining()) {
        throw new IllegalStateException("Invalid projection leaf dimensions");
      }
      offsets = (int) source.position();
      slots = offsets + count * Integer.BYTES;
      source.position(slots + used);
      final long sideCount = (flags & HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS) == 0
          ? 0
          : Utils.getVarLong(source);
      // The bounded image is stricter than the format's MAX_ENTRIES * 65536 reference ceiling.
      if (sideCount < 0 || sideCount > source.remaining() / (2 * Long.BYTES)) {
        throw new IllegalStateException("Invalid projection side-reference count");
      }
      references = (int) sideCount;
      referencesStart = (int) source.position();
      if (source.remaining() != sideCount * 2 * Long.BYTES) {
        throw new IllegalStateException("Trailing projection leaf bytes");
      }
      long previous = 0;
      for (int i = 0, at = referencesStart; i < references; i++, at += 2 * Long.BYTES) {
        final long composite = longAt(at);
        if (longAt(at + Long.BYTES) < 0 || i > 0 && composite <= previous) {
          throw new IllegalStateException("Invalid or unordered projection side reference");
        }
        previous = composite;
      }
    } catch (final IndexOutOfBoundsException e) {
      throw new IllegalStateException("Truncated projection leaf image", e);
    }
  }

  /** The caller verified the entire frame and consumed its HOT-leaf kind byte. */
  static @Nullable HOTProjectionImage copyIfFits(final BytesIn<?> source) {
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
    if (key.length < prefixLength
        || !Arrays.equals(bytes, prefixStart, prefixStart + prefixLength, key, 0, prefixLength)) {
      return null;
    }
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int offset = (int) INTS.get(bytes, offsets + middle * Integer.BYTES);
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

  private int shortAt(final int at) {
    return (bytes[at] & 255) | (bytes[at + 1] & 255) << Byte.SIZE;
  }

  private long longAt(final int at) {
    return (long) LONGS.get(bytes, at);
  }

  public long retainedSizeBytes() {
    return 64 + ((16L + bytes.length + 7) & ~7L);
  }
}
