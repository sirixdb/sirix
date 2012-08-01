package org.sirix.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

import java.util.Set;

import javax.annotation.Nonnull;

import org.sirix.node.interfaces.INodeBase;

public class TextReferences implements INodeBase{
  private final Set<Long> mNodes;
  private final long mNodeKey;

  public TextReferences(final @Nonnull Set<Long> pNodes, final long pNodeKey) {
    mNodes = checkNotNull(pNodes);
    checkArgument(pNodeKey >= 0, "pNodeKey must be >= 0!");
    mNodeKey = pNodeKey;
  }
  
  public Set<Long> getNodes() {
    return mNodes;
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public EKind getKind() {
    return EKind.TEXT_REFERENCES;
  }
  
  @Override
  public String toString() {
    final ToStringHelper helper = Objects.toStringHelper(this).add("nodeKey", mNodeKey);
    for (final long nodeKey : mNodes) {
      helper.add("node ref", nodeKey);
    }
    return helper.toString();
  }
}
