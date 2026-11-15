package io.sirix.page;

import io.sirix.BinaryEncodingVersion;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.Utils;

/** Uber-only feature entry codec; ordinary page read paths are unaffected. */
final class UberPageFormat {
  private static final int EXTENSION_BLOCK = 0x80;
  private static final int FEATURES = 0xC1;
  private static final int FEATURE_BYTES = 12;

  private UberPageFormat() {}

  static UberPage read(final BytesIn<?> source) {
    BinaryEncodingVersion.fromByte(source.readByte());
    final int flags = Byte.toUnsignedInt(source.readByte());
    if ((flags & ~EXTENSION_BLOCK) != 0) {
      throw new IllegalStateException("Unknown uber page flags 0x" + Integer.toHexString(flags));
    }
    final StoreFeatures features = flags == 0
        ? StoreFeatures.NONE
        : readFeatures(source);
    return new UberPage(source.readInt(), features);
  }

  private static StoreFeatures readFeatures(final BytesIn<?> source) {
    final long length = readLength(source, source.position() + source.remaining() - Integer.BYTES);
    if (length > source.remaining() - Integer.BYTES) {
      throw new IllegalStateException("Corrupt uber extension block length " + length);
    }
    final long end = source.position() + length;
    int previousTag = -1;
    StoreFeatures features = StoreFeatures.NONE;
    while (source.position() < end) {
      final int tag = Byte.toUnsignedInt(source.readByte());
      if (tag <= previousTag || source.position() == end) {
        throw new IllegalStateException("Corrupt uber extension tag " + tag);
      }
      previousTag = tag;
      final long payloadLength = readLength(source, end);
      if (payloadLength > end - source.position()) {
        throw new IllegalStateException("Corrupt uber extension payload length " + payloadLength);
      }
      if (tag == FEATURES) {
        if (payloadLength != FEATURE_BYTES) {
          throw new IllegalStateException("Store feature words require exactly 12 bytes");
        }
        features = new StoreFeatures(source.readInt(), source.readInt(), source.readInt());
      } else if ((tag & 0x80) != 0) {
        throw new IllegalStateException("Unknown required uber extension 0x" + Integer.toHexString(tag));
      } else {
        source.skip(payloadLength);
      }
    }
    return features;
  }

  private static long readLength(final BytesIn<?> source, final long end) {
    long value = 0;
    for (int shift = 0; shift < 63; shift += 7) {
      if (source.position() >= end) {
        throw new IllegalStateException("Truncated uber extension length");
      }
      final int next = Byte.toUnsignedInt(source.readByte());
      value |= (long) (next & 0x7f) << shift;
      if ((next & 0x80) == 0) {
        return value;
      }
    }
    throw new IllegalStateException("Overflowing uber extension length");
  }

  static void write(final BytesOut<?> sink, final UberPage page) {
    final StoreFeatures features = page.getFeatures();
    sink.writeByte(PageKind.UBERPAGE.getID());
    sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
    sink.writeByte((byte) (features.isEmpty()
        ? 0
        : EXTENSION_BLOCK));
    if (!features.isEmpty()) {
      Utils.putVarLong(sink, 14);
      sink.writeByte((byte) FEATURES);
      Utils.putVarLong(sink, FEATURE_BYTES);
      sink.writeInt(features.incompat());
      sink.writeInt(features.readOnlyCompat());
      sink.writeInt(features.compat());
    }
    sink.writeInt(page.getRevisionCount());
    page.setBootstrap(false);
  }
}
