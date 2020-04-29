package org.sirix.node;

import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;

import javax.annotation.Nonnegative;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodePersistenterImpl implements NodePersistenter {
  @Override
  public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
      final PageReadOnlyTrx pageReadTrx) throws IOException {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserialize(source, recordID, deweyID, pageReadTrx);
  }

  @Override
  public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) throws IOException {
    final NodeKind nodeKind = (NodeKind) record.getKind();
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serialize(sink, record, pageReadTrx);
  }

  @Override
  public SirixDeweyID deserializeDeweyID(final DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserializeDeweyID(source, previousDeweyID, resourceConfig);
  }

  @Override
  public void serializeDeweyID(final DataOutput sink, final NodeKind nodeKind, final SirixDeweyID deweyID,
      final SirixDeweyID previousDeweyID, ResourceConfiguration resourceConfig) throws IOException {
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serializeDeweyID(sink, nodeKind, deweyID, previousDeweyID, resourceConfig);
  }
}
