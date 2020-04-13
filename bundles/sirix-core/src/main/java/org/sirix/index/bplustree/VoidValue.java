package org.sirix.index.bplustree;

import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.RecordPersister;

/**
 * Represents a void value, that is no value at all (for inner node pages in the BPlusTree).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class VoidValue implements DataRecord {
  @Override
  public long getNodeKey() {
    return 0;
  }

  @Override
  public RecordPersister getKind() {
    return null;
  }

  @Override
  public long getRevision() {
    return 0;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }
}
