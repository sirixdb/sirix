package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.RecordSerializer;

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
  public byte[] getDeweyIDAsBytes() {
    return deweyId.toBytes();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }
}
