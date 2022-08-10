package org.sirix.node;

import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NodeSerializerImpl implements NodePersistenter {
  @Override
  public DataRecord deserialize(final Bytes<ByteBuffer> source, final @NonNegative long recordID, final byte[] deweyID,
      final PageReadOnlyTrx pageReadTrx) {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserialize(source, recordID, deweyID, pageReadTrx);
  }

  @Override
  public void serialize(final Bytes<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
    final NodeKind nodeKind = (NodeKind) record.getKind();
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serialize(sink, record, pageReadTrx);
  }

  @Override
  public byte[] deserializeDeweyID(Bytes<ByteBuffer> source, byte[] previousDeweyID,
      ResourceConfiguration resourceConfig) {
    if (resourceConfig.areDeweyIDsStored) {
      if (previousDeweyID != null) {
        final int cutOffSize = source.readByte();
        final int size = source.readByte();
        final byte[] deweyIDBytes = new byte[size];
        source.read(deweyIDBytes);

        final byte[] bytes = new byte[cutOffSize + deweyIDBytes.length];
        final ByteBuffer target = ByteBuffer.wrap(bytes);
        target.put(Arrays.copyOfRange(previousDeweyID, 0, cutOffSize));
        target.put(deweyIDBytes);

        return bytes;
      } else {
        final byte deweyIDLength = source.readByte();
        final byte[] deweyIDBytes = new byte[deweyIDLength];
        source.read(deweyIDBytes, 0, deweyIDLength);
        return deweyIDBytes;
      }
    }

    return null;
  }

  @Override
  public void serializeDeweyID(Bytes<ByteBuffer> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {
    if (resourceConfig.areDeweyIDsStored) {
      if (nextDeweyID != null) {
        final byte[] deweyIDBytes = deweyID;
        final byte[] nextDeweyIDBytes = nextDeweyID;

        //assert deweyIDBytes.length <= nextDeweyIDBytes.length;

        int i = 0;
        for (; i < deweyIDBytes.length && i < nextDeweyIDBytes.length; i++) {
          if (deweyIDBytes[i] != nextDeweyIDBytes[i]) {
            break;
          }
        }
        writeDeweyID(sink, nextDeweyIDBytes, i);
      } else {
        final byte[] deweyIDBytes = deweyID;
        sink.writeByte((byte) deweyIDBytes.length);
        sink.write(deweyIDBytes);
      }
    }
  }

  private static void writeDeweyID(final Bytes<ByteBuffer> sink, final byte[] deweyID, @NonNegative final int i) {
    sink.writeByte((byte) i);
    sink.writeByte((byte) (deweyID.length - i));
    final var bytes = Arrays.copyOfRange(deweyID, i, deweyID.length);
    sink.write(bytes);
  }
}
