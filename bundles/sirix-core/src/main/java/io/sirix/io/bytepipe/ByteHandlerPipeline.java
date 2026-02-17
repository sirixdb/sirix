package io.sirix.io.bytepipe;

import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.utils.OS;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

  /**
   * Check if this pipeline is empty (no handlers). An empty pipeline acts as identity - no
   * transformation is applied.
   * 
   * @return true if no handlers are configured
   */
  public boolean isEmpty() {
    return byteHandlers.isEmpty();
  }

  private boolean checkMemorySegmentSupport() {
    // Empty pipeline = identity operation, supports MemorySegments (Umbra-style)
    if (byteHandlers.isEmpty()) {
      return true;
    }
    // Pipeline supports MemorySegment if all handlers support it
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
    // Empty pipeline = identity (Umbra-style: no transformation needed)
    if (byteHandlers.isEmpty()) {
      return source;
    }

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
    // Empty pipeline = identity (Umbra-style: no transformation needed)
    if (byteHandlers.isEmpty()) {
      return compressed;
    }

    if (!memorySegmentSupport) {
      throw new UnsupportedOperationException(
          "MemorySegment decompression not supported - not all handlers support it");
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
    // Empty pipeline = identity, but we MUST copy to a buffer the page can own!
    // The input segment may be a reusable buffer (e.g., FileChannelReader's striped buffers)
    // that will be overwritten after we return. KeyValueLeafPage's zero-copy path keeps
    // a reference to slotMemory, so we need a buffer the page can safely own.
    if (byteHandlers.isEmpty()) {
      int size = (int) compressed.byteSize();

      // Try to use pooled allocator if available, otherwise fall back to heap
      MemorySegmentAllocator allocator = OS.isWindows()
          ? WindowsMemorySegmentAllocator.getInstance()
          : LinuxMemorySegmentAllocator.getInstance();

      if (allocator.isInitialized()) {
        // Use pooled buffer - optimal path
        MemorySegment buffer = allocator.allocate(size);
        MemorySegment.copy(compressed, 0, buffer, 0, size);

        // Return buffer with proper releaser so it's returned to pool when page is evicted
        return new DecompressionResult(buffer.asSlice(0, size), // segment (correctly sized view)
            buffer, // backingBuffer (full allocation for release)
            () -> allocator.release(buffer), new AtomicBoolean(false));
      } else {
        // Fallback: use heap-backed segment (for tests or when allocator not initialized)
        byte[] data = new byte[size];
        MemorySegment.copy(compressed, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, data, 0, size);
        MemorySegment heapSegment = MemorySegment.ofArray(data);

        // No-op releaser since heap memory is GC'd
        return new DecompressionResult(heapSegment, heapSegment, () -> {
        }, new AtomicBoolean(false));
      }
    }

    if (!memorySegmentSupport) {
      throw new UnsupportedOperationException("Scoped decompression not supported - not all handlers support it");
    }

    // For single-handler case (common), delegate directly
    if (byteHandlers.size() == 1) {
      return byteHandlers.getFirst().decompressScoped(compressed);
    }

    // Multi-handler chaining: decompress in reverse order while reusing buffers.
    // We only return the final buffer; intermediates are released immediately.
    MemorySegment current = compressed;
    MemorySegment backingBuffer = null;
    Runnable releaser = null;

    for (int i = byteHandlers.size() - 1; i >= 0; i--) {
      ByteHandler handler = byteHandlers.get(i);
      DecompressionResult result = handler.decompressScoped(current);

      // Release previous buffer (if any), keep the latest
      if (releaser != null) {
        releaser.run();
      }

      current = result.segment();
      backingBuffer = result.backingBuffer();
      releaser = result.releaser();
    }

    final Runnable finalReleaser = releaser;
    final MemorySegment finalBackingBuffer = backingBuffer;
    return new DecompressionResult(current, // segment
        finalBackingBuffer, // backingBuffer (for zero-copy ownership transfer)
        finalReleaser, // releaser
        new AtomicBoolean(false) // ownershipTransferred
    );
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
