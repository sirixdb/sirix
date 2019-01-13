package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;

public interface NodePersistenter extends RecordPersister {
  Optional<SirixDeweyID> deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;

  void serializeDeweyID(DataOutput sink, Kind nodeKind, SirixDeweyID deweyID, SirixDeweyID nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;
}
