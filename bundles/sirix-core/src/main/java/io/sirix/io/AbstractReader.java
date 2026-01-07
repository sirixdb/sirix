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

import static io.sirix.page.PageUtils.fixupPageReferenceIds;

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
      fixupPageReferenceIds(deserializedPage, resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
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
   * <p>For KeyValueLeafPages, the page may take ownership of the decompression buffer
   * via {@link ByteHandler.DecompressionResult#transferOwnership()}, enabling true
   * zero-copy where the decompressed data becomes the page's slotMemory directly.
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

    // Decompress - ownership may be transferred to page for zero-copy
    var decompressionResult = byteHandler.decompressScoped(compressedPage);
    
    try {
      // Pass DecompressionResult to enable zero-copy for KeyValueLeafPages
      Page deserializedPage = pagePersister.deserializePage(
          resourceConfiguration, 
          new MemorySegmentBytesIn(decompressionResult.segment()), 
          type,
          decompressionResult  // For zero-copy ownership transfer
      );
      
      // CRITICAL: Set database and resource IDs on all PageReferences in the deserialized page
      if (resourceConfiguration != null) {
        fixupPageReferenceIds(deserializedPage, resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
      }
      
      return deserializedPage;
    } finally {
      // Only release if ownership wasn't transferred (for non-KVLP pages or fallback path)
      // The close() method checks ownershipTransferred internally
      decompressionResult.close();
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
