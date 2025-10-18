package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public abstract class AbstractReader implements Reader {

  private final BytesOut<?> wrappedForRead = Bytes.allocateElasticOnHeap(10_000);

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
    // Use MemorySegment path if supported (zero-copy)
    if (byteHandler.supportsMemorySegments()) {
      MemorySegment segment = MemorySegment.ofArray(page);
      return deserializeFromSegment(resourceConfiguration, segment);
    }
    
    // Fallback to stream-based approach
    byte[] bytes;
    try (final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
      bytes = inputStream.readAllBytes();
    }
    wrappedForRead.clear(); // Clear before writing to ensure clean state
    wrappedForRead.write(bytes);
    final var deserializedPage = pagePersister.deserializePage(resourceConfiguration, wrappedForRead.asBytesIn(), type);
    wrappedForRead.clear();
    return deserializedPage;
  }

  /**
   * Zero-copy deserialization using MemorySegments.
   * Requires ByteHandler to support MemorySegment operations.
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

    // Decompress directly into MemorySegment (NO copies!)
    MemorySegment decompressed = byteHandler.decompress(compressedPage);

    // Deserialize directly from MemorySegment
    return pagePersister.deserializePage(resourceConfiguration, 
                                          new MemorySegmentBytesIn(decompressed), 
                                          type);
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
