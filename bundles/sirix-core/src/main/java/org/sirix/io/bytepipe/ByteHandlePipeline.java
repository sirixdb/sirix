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

  /** Pipeline for all byte handlers. */
  private final List<ByteHandler> byteHandlers;

  /**
   * Copy constructor.
   *
   * @param pipeline pipeline to copy
   */
  public ByteHandlePipeline(final ByteHandlePipeline pipeline) {
    byteHandlers = new ArrayList<>(pipeline.byteHandlers.size());
    for (final ByteHandler handler : pipeline.byteHandlers) {
      byteHandlers.add(handler.getInstance());
    }
  }

  /**
   *
   * Constructor.
   *
   * @param parts to be stored, Order is important!
   */
  public ByteHandlePipeline(final ByteHandler... parts) {
    byteHandlers = new ArrayList<>();

    if (parts != null) {
      Collections.addAll(byteHandlers, parts);
    }
  }

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    OutputStream pipeData = toSerialize;
    for (final ByteHandler byteHandler : byteHandlers) {
      pipeData = byteHandler.serialize(pipeData);
    }
    return pipeData;
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    InputStream pipeData = toDeserialize;
    for (final ByteHandler part : byteHandlers) {
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
    return Collections.unmodifiableList(byteHandlers);
  }

  @Override
  public ByteHandler getInstance() {
    return new ByteHandlePipeline();
  }
}
