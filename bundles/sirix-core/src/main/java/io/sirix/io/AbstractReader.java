package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public abstract class AbstractReader implements Reader {

  protected final ByteHandler byteHandler;

  /**
   * The type of data to serialize.
   */
  protected final SerializationType type;

  /**
   * Used to serialize/deserialze pages.
   */
  protected final PagePersister pagePersister;

  public AbstractReader(ByteHandler byteHandler, PagePersister pagePersister, SerializationType type) {
    this.byteHandler = byteHandler;
    this.pagePersister = pagePersister;
    this.type = type;
  }

  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page) throws IOException {
    // Use MemorySegment path if supported (zero-copy decompression)
    if (byteHandler.supportsMemorySegments()) {
      MemorySegment segment = MemorySegment.ofArray(page);
      return deserializeFromSegment(resourceConfiguration, segment);
    }
    
    // Fallback to stream-based approach for non-MemorySegment ByteHandlers
    byte[] decompressedBytes;
    try (final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
      decompressedBytes = inputStream.readAllBytes();
    }
    
    // Zero-copy wrap: MemorySegment backed directly by the byte array
    final var deserializedPage = pagePersister.deserializePage(
        resourceConfiguration, 
        Bytes.wrapForRead(decompressedBytes), 
        type
    );
    
    // CRITICAL: Set database and resource IDs on all PageReferences in the deserialized page.
    // This follows PostgreSQL pattern where BufferTag context (tablespace, database, relation)
    // is combined with on-disk block numbers when pages are read.
    if (resourceConfiguration != null) {
      io.sirix.page.PageUtils.fixupPageReferenceIds(deserializedPage, resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
    }
    
    return deserializedPage;
  }

  /**
   * Zero-copy deserialization using MemorySegments with Loom-friendly buffer pooling.
   * 
   * <p>Uses the scoped decompression API to ensure decompression buffers are returned
   * to the pool after deserialization completes. This bounds memory usage by pool size
   * (typically 2×CPU cores) rather than thread count.
   *
   * @param resourceConfiguration resource configuration
   * @param compressedPage compressed page data
   * @return deserialized page
   * @throws IOException if deserialization fails
   */
  public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage) throws IOException {
    if (!byteHandler.supportsMemorySegments()) {
      throw new UnsupportedOperationException("ByteHandler does not support MemorySegment operations");
    }

    // Use scoped decompression - buffer returned to pool when try block exits
    try (var decompressionResult = byteHandler.decompressScoped(compressedPage)) {
      // Deserialize directly from MemorySegment
      Page deserializedPage = pagePersister.deserializePage(resourceConfiguration, 
                                            new MemorySegmentBytesIn(decompressionResult.segment()), 
                                            type);
      
      // CRITICAL: Set database and resource IDs on all PageReferences in the deserialized page
      if (resourceConfiguration != null) {
        io.sirix.page.PageUtils.fixupPageReferenceIds(deserializedPage, resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
      }
      
      return deserializedPage;
    }
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();
    uberPageReference.setKey(0);
    final UberPage page = (UberPage) read(uberPageReference, null);
    uberPageReference.setPage(page);
    return uberPageReference;
  }

  public ByteHandler getByteHandler() {
    return byteHandler;
  }
}
