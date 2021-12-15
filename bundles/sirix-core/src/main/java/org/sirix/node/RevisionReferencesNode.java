package org.sirix.node;

import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.RecordSerializer;

import java.util.Arrays;
import java.util.Objects;

public final class RevisionReferencesNode implements DataRecord {
  private final long nodeKey;

  private int[] revisions;

  public RevisionReferencesNode(final long nodeKey, final int[] revisions) {
    this.nodeKey = nodeKey;
    this.revisions = revisions;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public RecordSerializer getKind() {
    return NodeKind.REVISION_REFERENCES_NODE;
  }

  @Override
  public long getRevision() {
    throw new UnsupportedOperationException();
  }

  public RevisionReferencesNode addRevision(final int revision) {
    final int[] copy = new int[revisions.length + 1];
    System.arraycopy(revisions, 0, copy, 0, revisions.length);
    copy[copy.length - 1] = revision;
    revisions = copy;
    return this;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(nodeKey);
    result = 31 * result + Arrays.hashCode(revisions);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    RevisionReferencesNode that = (RevisionReferencesNode) o;
    return nodeKey == that.nodeKey && Arrays.equals(revisions, that.revisions);
  }

  public int[] getRevisions() {
    return revisions;
  }
}
