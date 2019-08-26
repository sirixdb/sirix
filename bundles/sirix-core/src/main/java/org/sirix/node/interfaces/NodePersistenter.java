package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import org.sirix.access.ResourceConfiguration;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;

public interface NodePersistenter extends RecordPersister {
  Optional<SirixDeweyID> deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;

  void serializeDeweyID(DataOutput sink, NodeKind nodeKind, SirixDeweyID deweyID, SirixDeweyID nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;
}
