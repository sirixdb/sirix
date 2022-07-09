package org.sirix.node.interfaces;

import me.lemire.integercompression.differential.IntegratedIntCompressor;
import net.openhft.chronicle.bytes.Bytes;
import org.sirix.access.ResourceConfiguration;
import org.sirix.node.SirixDeweyID;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface NodePersistenter extends RecordSerializer {
  IntegratedIntCompressor INTEGRATED_INT_COMPRESSOR = new IntegratedIntCompressor();

  record DeweyIDData(SirixDeweyID deweyID, byte[] data) {}

  byte[] deserializeDeweyID(Bytes<ByteBuffer> source, byte[] previousDeweyID,
      ResourceConfiguration resourceConfig);

  void serializeDeweyID(Bytes<ByteBuffer> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig);
}
