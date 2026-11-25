package io.sirix.page;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;

import java.util.Arrays;
import java.util.Objects;

/**
 * Optional page-envelope entries. Unknown optional entries describe only their containing image and
 * are deliberately discarded on rewrite. Required entries need an explicit decoder before use.
 * Instances are immutable and may be reused by writers; ordinary pages never construct one.
 */
public final class PageExtensions {
  public static final int FLAG_EXTENSION_BLOCK = 0x80;
  public static final int REQUIRED = 0x80;
  public static final int KIND_LOCAL = 0x40;
  /**
   * FMT-209: the revision root's last-revert revision, a kind-local optional entry carrying a
   * little-endian u32.
   */
  public static final int REVISION_ROOT_LAST_REVERT = KIND_LOCAL | 0x01;
  private static final int LAST_REVERT_PAYLOAD_BYTES = Integer.BYTES;
  private static final int MAX_WRITTEN_LENGTH = 255;
  public static final PageExtensions EMPTY = new PageExtensions(new byte[0]);

  private final byte[] entries;

  private PageExtensions(final byte[] entries) {
    this.entries = entries;
  }

  public static Builder builder() {
    return new Builder();
  }

  public boolean isEmpty() {
    return entries.length == 0;
  }

  void write(final BytesOut<?> sink) {
    sink.writeStopBit(entries.length);
    sink.write(entries);
  }

  /** Only entered from the envelope's existing unknown-flag branch: no added work when unused. */
  static byte readOrRefuse(final BytesIn<?> source, final byte flags, final byte allowedMask) {
    if (((flags & 0xFF) & ~((allowedMask & 0xFF) | FLAG_EXTENSION_BLOCK)) != 0) {
      throw new IllegalStateException("Unknown page envelope flags 0x" + Integer.toHexString(flags & 0xFF)
          + " (allowed mask 0x" + Integer.toHexString(allowedMask & 0xFF) + ") — page written by a newer version");
    }
    final long blockLength = readLength(source, source.remaining());
    if (blockLength > source.remaining()) {
      throw new IllegalStateException("Corrupt page extension block length " + blockLength);
    }
    final long end = source.position() + blockLength;
    int previousTag = -1;
    while (source.position() < end) {
      final int tag = source.readByte() & 0xFF;
      if (tag <= previousTag) {
        throw new IllegalStateException("Page extension tags must be unique and ascending: " + tag);
      }
      previousTag = tag;
      final long length = readLength(source, end - source.position());
      if (length > end - source.position()) {
        throw new IllegalStateException("Corrupt page extension entry length " + length + " for tag " + tag);
      }
      // No shared or kind-local tags are assigned yet. Add their decoders here when assigned.
      if ((tag & REQUIRED) != 0) {
        throw new IllegalStateException(
            "Unknown required page extension " + tag + " — page written by a newer version");
      }
      source.skip(length);
    }
    return (byte) (flags & ~FLAG_EXTENSION_BLOCK);
  }

  /**
   * The extension block of a revision root page (FMT-209 under FMT-203 framing): decodes the
   * last-revert entry, skips other optional entries and refuses required or malformed ones. Only
   * entered when the flags byte is nonzero, so a never-reverted resource pays nothing.
   *
   * @return the last-revert revision, {@code 0} when the entry is absent
   */
  static int readRevisionRootLastRevert(final BytesIn<?> source, final byte flags) {
    if ((flags & 0xFF) != FLAG_EXTENSION_BLOCK) {
      throw new IllegalStateException("Unknown page envelope flags 0x" + Integer.toHexString(flags & 0xFF)
          + " on a revision root page — page written by a newer version");
    }
    final long blockLength = readLength(source, source.remaining());
    if (blockLength > source.remaining()) {
      throw new IllegalStateException("Corrupt page extension block length " + blockLength);
    }
    final long end = source.position() + blockLength;
    int previousTag = -1;
    int lastRevertRevision = 0;
    while (source.position() < end) {
      final int tag = source.readByte() & 0xFF;
      if (tag <= previousTag) {
        throw new IllegalStateException("Page extension tags must be unique and ascending: " + tag);
      }
      previousTag = tag;
      final long length = readLength(source, end - source.position());
      if (length > end - source.position()) {
        throw new IllegalStateException("Corrupt page extension entry length " + length + " for tag " + tag);
      }
      if (tag == REVISION_ROOT_LAST_REVERT) {
        if (length != LAST_REVERT_PAYLOAD_BYTES) {
          throw new IllegalStateException("Last-revert revision entry must carry exactly 4 bytes, not " + length);
        }
        lastRevertRevision = (source.readByte() & 0xFF) | (source.readByte() & 0xFF) << 8
            | (source.readByte() & 0xFF) << 16 | (source.readByte() & 0xFF) << 24;
        // Written only for a resource that reverted, so 0 is malformed; revisions are non-negative ints.
        if (lastRevertRevision <= 0) {
          throw new IllegalStateException(
              "Corrupt last-revert revision " + Integer.toUnsignedString(lastRevertRevision));
        }
      } else if ((tag & REQUIRED) != 0) {
        throw new IllegalStateException(
            "Unknown required page extension " + tag + " — page written by a newer version");
      } else {
        source.skip(length);
      }
    }
    if (blockLength == 0) {
      throw new IllegalStateException("Empty page extension block on a revision root page");
    }
    return lastRevertRevision;
  }

  /**
   * The extension block of a revision root that published or carries a revert (FMT-209).
   *
   * @param lastRevertRevision the positive last-revert revision
   * @return the one-entry block
   */
  public static PageExtensions lastRevertRevision(final int lastRevertRevision) {
    if (lastRevertRevision <= 0) {
      throw new IllegalArgumentException("Last-revert revision must be positive: " + lastRevertRevision);
    }
    return new PageExtensions(
        new byte[] {(byte) REVISION_ROOT_LAST_REVERT, (byte) LAST_REVERT_PAYLOAD_BYTES, (byte) lastRevertRevision,
            (byte) (lastRevertRevision >>> 8), (byte) (lastRevertRevision >>> 16), (byte) (lastRevertRevision >>> 24)});
  }

  /** Bounded unsigned stop-bit length: the general node decoder does not guard shift overflow. */
  private static long readLength(final BytesIn<?> source, final long available) {
    long length = 0;
    for (int index = 0; index < Long.SIZE / 7; index++) {
      if (index >= available) {
        throw new IllegalStateException("Truncated page extension length");
      }
      final int value = source.readByte() & 0xFF;
      length |= (long) (value & 0x7F) << (index * 7);
      if ((value & 0x80) == 0) {
        return length;
      }
    }
    throw new IllegalStateException("Overflow in page extension length");
  }

  /**
   * Writer supplies only tags it understands; unknown entries from an older image are never copied.
   */
  public static final class Builder {
    private final byte[] entries = new byte[MAX_WRITTEN_LENGTH];
    private int size;
    private int previousTag = -1;

    private Builder() {}

    public Builder add(final int tag, final byte[] payload) {
      Objects.requireNonNull(payload);
      if (tag < 0 || tag > 255 || tag <= previousTag) {
        throw new IllegalArgumentException("Page extension tags must be unsigned, unique and ascending: " + tag);
      }
      final int lengthBytes = payload.length < 128
          ? 1
          : 2;
      if (payload.length > MAX_WRITTEN_LENGTH - size - 1 - lengthBytes) {
        throw new IllegalArgumentException("Page extension block must be shorter than 256 bytes");
      }
      entries[size++] = (byte) tag;
      if (lengthBytes == 2) {
        entries[size++] = (byte) ((payload.length & 0x7F) | 0x80);
        entries[size++] = (byte) (payload.length >>> 7);
      } else {
        entries[size++] = (byte) payload.length;
      }
      System.arraycopy(payload, 0, entries, size, payload.length);
      size += payload.length;
      previousTag = tag;
      return this;
    }

    public PageExtensions build() {
      return size == 0
          ? EMPTY
          : new PageExtensions(Arrays.copyOf(entries, size));
    }
  }
}
