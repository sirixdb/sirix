/**
 * 
 */
package org.sirix.io.bytepipe;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.sirix.exception.TTIOException;

/**
 * 
 * Pipeline to handle Bytes before stored in the backends.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class ByteHandlePipeline implements IByteHandler {

  /** Pipeline hold over here. */
  private final List<IByteHandler> mParts;

  /**
   * 
   * Constructor.
   * 
   * @param pParts
   *          to be stored, Order is important!
   */
  public ByteHandlePipeline(final IByteHandler... pParts) {
    mParts = new ArrayList<>();
    for (final IByteHandler part : pParts) {
      mParts.add(part);
    }
  }

  @Override
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws TTIOException {
    byte[] pipeData = pToSerialize;
    for (final IByteHandler part : mParts) {
      pipeData = part.serialize(pipeData);
    }
    return pipeData;
  }

  @Override
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws TTIOException {
    byte[] pipeData = pToDeserialize;
    for (int i = mParts.size() - 1; i >= 0; i--) {
      pipeData = mParts.get(i).deserialize(pipeData);
    }
    return pipeData;
  }

}
