package org.sirix.node.interfaces;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnull;

public interface IKind {

  /**
   * Deserializing a node using a {@link ByteArrayDataInput}.
   * 
   * @param pSource
   *          input source
   * @return a {@link INode} instance
   */
  INode deserialize(@Nonnull final ByteArrayDataInput pSource);

  /**
   * Serializing a node from a {@link ByteArrayDataOutput}.
   * 
   * @param pSink
   *          where the data should be serialized to
   * @param pToSerialize
   *          the node to serialize
   */
  void serialize(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final INode pToSerialize);

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
