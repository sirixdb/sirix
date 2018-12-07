/**
 *
 */
package org.sirix.io.bytepipe;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pipeline to handle bytes before stored in the backend.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class ByteHandlePipeline implements ByteHandler {

  /** Pipeline hold over here. */
  private final List<ByteHandler> mParts;

  /**
   * Copy constructor.
   *
   * @param pipeline pipeline to copy
   */
  public ByteHandlePipeline(final ByteHandlePipeline pipeline) {
    mParts = new ArrayList<>(pipeline.mParts.size());
    for (final ByteHandler handler : pipeline.mParts) {
      mParts.add(handler.getInstance());
    }
  }

  /**
   *
   * Constructor.
   *
   * @param parts to be stored, Order is important!
   */
  public ByteHandlePipeline(final ByteHandler... parts) {
    mParts = new ArrayList<>();
    for (final ByteHandler part : parts) {
      mParts.add(part);
    }
  }

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    OutputStream pipeData = toSerialize;
    for (final ByteHandler part : mParts) {
      pipeData = part.serialize(pipeData);
    }
    return pipeData;
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    InputStream pipeData = toDeserialize;
    for (final ByteHandler part : mParts) {
      pipeData = part.deserialize(pipeData);
    }
    return pipeData;
  }

  /**
   * Get byte handler components.
   *
   * @return all components
   */
  public List<ByteHandler> getComponents() {
    return Collections.unmodifiableList(mParts);
  }

  @Override
  public ByteHandler getInstance() {
    return new ByteHandlePipeline();
  }
}
