package org.sirix.node;

import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;

import javax.annotation.Nonnegative;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NodeSerializerImpl implements NodePersistenter {
  @Override
  public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
      final PageReadOnlyTrx pageReadTrx) throws IOException {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserialize(source, recordID, deweyID, pageReadTrx);
  }

  @Override
  public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
      throws IOException {
    final NodeKind nodeKind = (NodeKind) record.getKind();
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serialize(sink, record, pageReadTrx);
  }

  @Override
  public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException {
    if (resourceConfig.areDeweyIDsStored) {
      if (previousDeweyID != null) {
        final byte[] previousDeweyIDBytes = previousDeweyID.toBytes();
        final int cutOffSize = source.readByte();
        final int size = source.readByte();
        final byte[] deweyIDBytes = new byte[size];
        source.readFully(deweyIDBytes);

        final byte[] bytes = new byte[cutOffSize + deweyIDBytes.length];
        final ByteBuffer target = ByteBuffer.wrap(bytes);
        target.put(Arrays.copyOfRange(previousDeweyIDBytes, 0, cutOffSize));
        target.put(deweyIDBytes);

        return new SirixDeweyID(bytes);
      } else {
        final byte deweyIDLength = source.readByte();
        final byte[] deweyIDBytes = new byte[deweyIDLength];
        source.readFully(deweyIDBytes, 0, deweyIDLength);
        return new SirixDeweyID(deweyIDBytes);
      }
    }

    return null;
  }

  @Override
  public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException {
    if (resourceConfig.areDeweyIDsStored) {
      if (nextDeweyID != null) {
        final byte[] deweyIDBytes = deweyID.toBytes();
        final byte[] nextDeweyIDBytes = nextDeweyID.toBytes();

        assert deweyIDBytes.length <= nextDeweyIDBytes.length;

        int i = 0;
        for (; i < deweyIDBytes.length; i++) {
          if (deweyIDBytes[i] != nextDeweyIDBytes[i]) {
            break;
          }
        }
        writeDeweyID(sink, nextDeweyIDBytes, i);
      } else {
        final byte[] deweyIDBytes = deweyID.toBytes();
        sink.writeByte(deweyIDBytes.length);
        sink.write(deweyIDBytes);
      }
    }
  }

  private static void writeDeweyID(final DataOutput sink, final byte[] deweyID, @Nonnegative final int i)
      throws IOException {
    sink.writeByte(i);
    sink.writeByte(deweyID.length - i);
    sink.write(Arrays.copyOfRange(deweyID, i, deweyID.length));
  }
}
