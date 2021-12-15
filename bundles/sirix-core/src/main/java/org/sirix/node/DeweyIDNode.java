package org.sirix.node;

import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.RecordSerializer;

public final class DeweyIDNode implements DataRecord {

  private final long nodeKey;

  private final SirixDeweyID deweyId;

  public DeweyIDNode(long nodeKey, SirixDeweyID deweyId) {
    this.nodeKey = nodeKey;
    this.deweyId = deweyId;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return deweyId;
  }

  @Override
  public RecordSerializer getKind() {
    return NodeKind.DEWEY_ID_NODE;
  }

  @Override
  public long getRevision() {
    throw new UnsupportedOperationException();
  }
}
