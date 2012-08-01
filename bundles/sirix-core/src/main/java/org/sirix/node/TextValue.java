package org.sirix.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.node.interfaces.INodeBase;

public class TextValue implements Comparable<TextValue>, INodeBase {
  private final byte[] mValue;
  private final long mNodeKey;

  public TextValue(final @Nonnull byte[] pValue,
    final @Nonnegative long pNodeKey) {
    mValue = checkNotNull(pValue);
    checkArgument(pNodeKey >= 0, "pNodeKey must be >= 0!");
    mNodeKey = pNodeKey;
  }

  public byte[] getValue() {
    return mValue;
  }

  @SuppressWarnings("null")
  @Override
  public int compareTo(final @Nullable TextValue pOther) {
    return mValue.toString().compareTo(pOther.mValue.toString());
  }

  @Override
  public boolean equals(final @Nullable Object pObj) {
    if (pObj instanceof TextValue) {
      final TextValue otherValue = (TextValue)pObj;
      return otherValue.mValue.equals(mValue);
    }
    return false;
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public EKind getKind() {
    return EKind.TEXT_VALUE;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("nodeKey", mNodeKey).add("value",
      mValue.toString()).toString();
  }
}
