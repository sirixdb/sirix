package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReader.class);

  public AbstractReader(ByteHandler byteHandler, PagePersister pagePersister, SerializationType type) {
    this.byteHandler = byteHandler;
    this.pagePersister = pagePersister;
    this.type = type;
  }

  /**
   * Verify page checksum for non-KeyValueLeafPage pages.
   * 
   * <p>For non-KVLP pages, the hash is computed on compressed bytes, so verification
   * happens BEFORE decompression. For KVLP pages, verification must happen after
   * decompression (handled in PageKind deserialization).</p>
   *
   * @param compressedData the compressed page data
   * @param reference the page reference containing expected hash
   * @param resourceConfig the resource configuration (for checking if verification is enabled)
   * @throws SirixCorruptionException if checksum mismatch is detected
   */
  protected void verifyChecksumIfNeeded(byte[] compressedData, PageReference reference, 
                                         ResourceConfiguration resourceConfig) {
    if (resourceConfig == null || !resourceConfig.verifyChecksumsOnRead) {
      return; // Verification disabled or no config
    }
    
    byte[] expectedHash = reference.getHash();
    if (expectedHash == null || expectedHash.length == 0) {
      return; // No hash to verify
    }
    
    // All page types use hash computed on compressed data
    HashAlgorithm hashAlgorithm = resourceConfig.hashAlgorithm;
    if (!PageHasher.verify(compressedData, expectedHash, hashAlgorithm)) {
      byte[] actualHash = PageHasher.computeActualHash(compressedData, hashAlgorithm);
      throw new SirixCorruptionException(reference.getKey(), "compressed", expectedHash, actualHash);
    }
    
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Checksum verified for page at key {}", reference.getKey());
    }
  }

  /**
   * Verify page checksum using MemorySegment (zero-copy for native segments).
   *
   * @param compressedSegment the compressed page data as MemorySegment
   * @param reference the page reference containing expected hash
   * @param resourceConfig the resource configuration
   * @throws SirixCorruptionException if checksum mismatch is detected
   */
  protected void verifyChecksumIfNeeded(MemorySegment compressedSegment, PageReference reference,
                                         ResourceConfiguration resourceConfig) {
    if (resourceConfig == null || !resourceConfig.verifyChecksumsOnRead) {
      return;
    }
    
    byte[] expectedHash = reference.getHash();
    if (expectedHash == null || expectedHash.length == 0) {
      return;
    }
    
    // All page types use hash computed on compressed data (zero-copy for native segments)
    HashAlgorithm hashAlgorithm = resourceConfig.hashAlgorithm;
    if (!PageHasher.verify(compressedSegment, expectedHash, hashAlgorithm)) {
      byte[] actualHash = PageHasher.computeActualHash(compressedSegment, hashAlgorithm);
      throw new SirixCorruptionException(reference.getKey(), "compressed", expectedHash, actualHash);
    }
    
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Checksum verified for page at key {}", reference.getKey());
    }
  }

  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page) throws IOException {
    return deserialize(resourceConfiguration, page, null);
  }

  /**
   * Deserialize page with optional KVLP checksum verification.
   * 
   * @param resourceConfiguration resource configuration
   * @param page compressed page data
   * @param reference page reference for KVLP hash verification (may be null)
   * @return deserialized page
   * @throws IOException if deserialization fails
   * @throws SirixCorruptionException if KVLP checksum verification fails
   */
  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page, 
                          PageReference reference) throws IOException {
    // Use MemorySegment path if supported (zero-copy decompression)
    if (byteHandler.supportsMemorySegments()) {
      MemorySegment segment = MemorySegment.ofArray(page);
      return deserializeFromSegment(resourceConfiguration, segment, reference);
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
    return deserializeFromSegment(resourceConfiguration, compressedPage, null);
  }

  /**
   * Zero-copy deserialization with optional KVLP checksum verification.
   *
   * @param resourceConfiguration resource configuration
   * @param compressedPage compressed page data
   * @param reference page reference for KVLP hash verification (may be null)
   * @return deserialized page
   * @throws IOException if deserialization fails
   * @throws SirixCorruptionException if KVLP checksum verification fails
   */
  public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage,
                                      PageReference reference) throws IOException {
    if (!byteHandler.supportsMemorySegments()) {
      throw new UnsupportedOperationException("ByteHandler does not support MemorySegment operations");
    }

    // Decompress - ownership may be transferred to page for zero-copy
    var decompressionResult = byteHandler.decompressScoped(compressedPage);
    
    try {
      MemorySegment uncompressedSegment = decompressionResult.segment();
      
      // Pass DecompressionResult to enable zero-copy for KeyValueLeafPages
      Page deserializedPage = pagePersister.deserializePage(
          resourceConfiguration, 
          new MemorySegmentBytesIn(uncompressedSegment), 
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
