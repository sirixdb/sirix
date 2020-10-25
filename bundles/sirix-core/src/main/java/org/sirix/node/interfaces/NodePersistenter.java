package org.sirix.node.interfaces;

import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.sirix.access.ResourceConfiguration;
import org.sirix.node.SirixDeweyID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface NodePersistenter extends RecordSerializer {
  IntegratedIntCompressor INTEGRATED_INT_COMPRESSOR = new IntegratedIntCompressor();

  SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;

  void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;
}
