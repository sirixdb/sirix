package org.sirix.node.interfaces;

import javax.annotation.Nonnull;

import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;

public interface IKind {

  /**
   * Deserializing a node using a {@link ITTSource}.
   * 
   * @param pSource
   *          input source
   * @return a {@link INode} instance
   */
  INode deserialize(@Nonnull final ITTSource pSource);

  /**
   * Serializing a node from a {@link ITTSink}.
   * 
   * @param pSink
   *          where the data should be serialized to
   * @param pToSerialize
   *          the node to serialize
   */
  void serialize(@Nonnull final ITTSink pSink, @Nonnull final INode pToSerialize);

  /**
   * Get the nodeKind.
   * 
   * @return the unique kind
   */
  byte getId();

  /**
   * Get class of node.
   * 
   * @return class of node
   */
  Class<? extends INode> getNodeClass();

}
