package org.sirix.node.interfaces;

import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.sirix.access.ResourceConfiguration;
import org.sirix.node.SirixDeweyID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface NodePersistenter extends RecordSerializer {
  IntegratedIntCompressor INTEGRATED_INT_COMPRESSOR = new IntegratedIntCompressor();

  record DeweyIDData(SirixDeweyID deweyID, byte[] data) {}

  byte[] deserializeDeweyID(DataInput source, byte[] previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;

  void serializeDeweyID(DataOutput sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException;
}
