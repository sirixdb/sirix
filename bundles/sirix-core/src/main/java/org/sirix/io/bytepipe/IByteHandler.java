package org.sirix.io.bytepipe;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;

/**
 * Interface for the decorator, representing any byte representation to be
 * serialized or to serialize.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public interface IByteHandler {

  /**
   * Method to serialize any byte-chunk.
   * 
   * @param pToSerialize
   *          byte to be serialized
   * @return result of the serialization
   * @throws SirixIOException
   *           if a byte handling exception occurs
   */
  byte[] serialize(@Nonnull byte[] pToSerialize) throws SirixIOException;

  /**
   * Method to deserialize any byte-chunk.
   * 
   * @param pToDeserialize
   *          to deserialize
   * @return result of the deserialization
   * @throws SirixIOException
   *           if a byte handling exception occurs
   */
  byte[] deserialize(@Nonnull byte[] pToDeserialize)
    throws SirixIOException;
  
  /**
   * Method to retrieve a new instance.
   * 
   * @return new instance
   */
  IByteHandler getInstance();
}
