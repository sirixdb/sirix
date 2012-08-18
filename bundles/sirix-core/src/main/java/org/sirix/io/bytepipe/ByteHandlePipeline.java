/**
 * 
 */
package org.sirix.io.bytepipe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.sirix.exception.TTIOException;

/**
 * Pipeline to handle Bytes before stored in the backends.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class ByteHandlePipeline implements IByteHandler {

  /** Pipeline hold over here. */
  private final List<IByteHandler> mParts;

  /**
   * Copy constructor.
   * 
   * @param pPipeline
   *          pipeline to copy
   */
  public ByteHandlePipeline(final @Nonnull ByteHandlePipeline pPipeline) {
    mParts = new ArrayList<>(pPipeline.mParts.size());
    for (final IByteHandler handler : pPipeline.mParts) {
      mParts.add(handler.getInstance());
    }
  }

  /**
   * 
   * Constructor.
   * 
   * @param pParts
   *          to be stored, Order is important!
   */
  public ByteHandlePipeline(final @Nonnull IByteHandler... pParts) {
    mParts = new ArrayList<>();
    for (final IByteHandler part : pParts) {
      mParts.add(part);
    }
  }

  @Override
  public byte[] serialize(final @Nonnull byte[] pToSerialize)
    throws TTIOException {
    byte[] pipeData = pToSerialize;
    for (final IByteHandler part : mParts) {
      pipeData = part.serialize(pipeData);
    }
    return pipeData;
  }

  @Override
  public byte[] deserialize(final @Nonnull byte[] pToDeserialize)
    throws TTIOException {
    byte[] pipeData = pToDeserialize;
    for (int i = mParts.size() - 1; i >= 0; i--) {
      pipeData = mParts.get(i).deserialize(pipeData);
    }
    return pipeData;
  }

  /**
   * Get byte handler components.
   * 
   * @return all components
   */
  public List<IByteHandler> getComponents() {
    return Collections.unmodifiableList(mParts);
  }

  @Override
  public IByteHandler getInstance() {
    return new ByteHandlePipeline();
  }

}
