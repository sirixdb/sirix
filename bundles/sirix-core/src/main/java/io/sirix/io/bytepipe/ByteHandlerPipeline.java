package io.sirix.io.bytepipe;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pipeline to handle bytes before stored in the backend.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class ByteHandlerPipeline implements ByteHandler {

  /** Pipeline for all byte handlers. */
  private final List<ByteHandler> byteHandlers;
  
  /** Cached result of supportsMemorySegments check. */
  private final boolean memorySegmentSupport;

  /**
   * Copy constructor.
   *
   * @param pipeline pipeline to copy
   */
  public ByteHandlerPipeline(final ByteHandlerPipeline pipeline) {
    byteHandlers = new ArrayList<>(pipeline.byteHandlers.size());
    for (final ByteHandler handler : pipeline.byteHandlers) {
      byteHandlers.add(handler.getInstance());
    }
    this.memorySegmentSupport = checkMemorySegmentSupport();
  }

  /**
   *
   * Constructor.
   *
   * @param parts to be stored, Order is important!
   */
  public ByteHandlerPipeline(final ByteHandler... parts) {
    byteHandlers = new ArrayList<>();

    if (parts != null) {
      Collections.addAll(byteHandlers, parts);
    }
    this.memorySegmentSupport = checkMemorySegmentSupport();
  }
  
  private boolean checkMemorySegmentSupport() {
    // Pipeline supports MemorySegment if all handlers support it
    // For single-handler pipelines (common case), this is straightforward
    if (byteHandlers.isEmpty()) {
      return false;
    }
    for (final ByteHandler handler : byteHandlers) {
      if (!handler.supportsMemorySegments()) {
        return false;
      }
    }
    return true;
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
  
  @Override
  public boolean supportsMemorySegments() {
    return memorySegmentSupport;
  }
  
  @Override
  public MemorySegment compress(MemorySegment source) {
    if (!memorySegmentSupport) {
      throw new UnsupportedOperationException("MemorySegment compression not supported - not all handlers support it");
    }
    
    // Apply handlers in order (compression pipeline)
    MemorySegment result = source;
    for (final ByteHandler handler : byteHandlers) {
      result = handler.compress(result);
    }
    return result;
  }
  
  @Override
  public MemorySegment decompress(MemorySegment compressed) {
    if (!memorySegmentSupport) {
      throw new UnsupportedOperationException("MemorySegment decompression not supported - not all handlers support it");
    }
    
    // Apply handlers in reverse order (decompression pipeline)
    MemorySegment result = compressed;
    for (int i = byteHandlers.size() - 1; i >= 0; i--) {
      result = byteHandlers.get(i).decompress(result);
    }
    return result;
  }
  
  @Override
  public DecompressionResult decompressScoped(MemorySegment compressed) {
    if (!memorySegmentSupport) {
      throw new UnsupportedOperationException("Scoped decompression not supported - not all handlers support it");
    }
    
    // For single-handler case (common), delegate directly
    if (byteHandlers.size() == 1) {
      return byteHandlers.getFirst().decompressScoped(compressed);
    }
    
    // For multi-handler pipeline, we need to chain decompressions
    // This is complex because intermediate buffers need management
    // For now, use the non-scoped approach for multi-handler
    MemorySegment result = decompress(compressed);
    return new DecompressionResult(result, null);
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
    return new ByteHandlerPipeline();
  }
}
