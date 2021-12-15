package org.sirix.node.interfaces.immutable;

import java.math.BigInteger;
import javax.annotation.Nullable;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.DataRecord;

/**
 * An immutable node.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface ImmutableNode extends DataRecord {

  @Override
  NodeKind getKind();

  /**
   * Determines if {@code pOther} is the same item.
   *
   * @param other the other node
   * @return {@code true}, if it is the same item, {@code false} otherwise
   */
  boolean isSameItem(@Nullable Node other);

  /**
   * Getting the stored hash.
   * @return the hash code
   */
  BigInteger getHash();

  /**
   * Compute the hash code.
   * @return the computed hash code
   */
  BigInteger computeHash();

  /**
   * Gets key of the context item's parent.
   *
   * @return parent key
   */
  long getParentKey();

  /**
   * Declares, whether the item has a parent.
   *
   * @return {@code true}, if item has a parent, {@code false} otherwise
   */
  boolean hasParent();
}
