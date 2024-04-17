package io.sirix.node;

import io.sirix.node.interfaces.DeweyIdSerializer;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.node.interfaces.DataRecord;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NodeSerializerImpl implements DeweyIdSerializer {
  @Override
  public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
      final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserialize(source, recordID, deweyID, resourceConfiguration);
  }

  @Override
  public void serialize(final BytesOut<?> sink, final DataRecord record,
      final ResourceConfiguration resourceConfiguration) {
    final NodeKind nodeKind = (NodeKind) record.getKind();
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serialize(sink, record, resourceConfiguration);
  }

  @Override
  public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
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
  public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {
    if (resourceConfig.areDeweyIDsStored) {
      if (nextDeweyID != null) {

        //assert deweyIDBytes.length <= nextDeweyIDBytes.length;

        int i = 0;
        for (; i < deweyID.length && i < nextDeweyID.length; i++) {
          if (deweyID[i] != nextDeweyID[i]) {
            break;
          }
        }
        writeDeweyID(sink, nextDeweyID, i);
      } else {
        sink.writeByte((byte) deweyID.length);
        sink.write(deweyID);
      }
    }
  }

  private static void writeDeweyID(final BytesOut<?> sink, final byte[] deweyID, @NonNegative final int i) {
    sink.writeByte((byte) i);
    sink.writeByte((byte) (deweyID.length - i));
    final var bytes = Arrays.copyOfRange(deweyID, i, deweyID.length);
    sink.write(bytes);
  }
}
