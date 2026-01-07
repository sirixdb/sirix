package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PageKind;
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
    
    // Check if this is a KeyValueLeafPage by peeking at the page type byte
    // KVLP pages have hash computed on uncompressed data, so we skip verification here
    // (KVLP verification happens after decompression in PageKind)
    if (compressedData.length > 0) {
      byte pageTypeId = compressedData[0];
      if (pageTypeId == PageKind.KEYVALUELEAFPAGE.getID()) {
        // Skip - KVLP verification happens after decompression
        return;
      }
    }
    
    // Verify hash on compressed data
    if (!PageHasher.verify(compressedData, expectedHash)) {
      byte[] actualHash = PageHasher.computeActualHash(compressedData);
      throw new SirixCorruptionException(reference.getKey(), "compressed", expectedHash, actualHash);
    }
    
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Checksum verified for page at key {}", reference.getKey());
    }
  }

  /**
   * Verify page checksum for non-KeyValueLeafPage pages using MemorySegment (zero-copy).
   *
   * <p>For native (off-heap) segments with XXH3 hashes, verification is zero-copy.
   * For SHA-256 (legacy) hashes, a copy is required.</p>
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
    
    // Check page type
    if (compressedSegment.byteSize() > 0) {
      byte pageTypeId = compressedSegment.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
      if (pageTypeId == PageKind.KEYVALUELEAFPAGE.getID()) {
        return; // Skip - KVLP verification happens after decompression
      }
    }
    
    // Zero-copy verification for native segments with XXH3
    if (!PageHasher.verify(compressedSegment, expectedHash)) {
      byte[] actualHash = PageHasher.computeActualHash(compressedSegment);
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
    
    // Verify KVLP checksum on uncompressed bytes
    verifyKVLPChecksum(decompressedBytes, reference, resourceConfiguration);
    
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
   * Verify checksum for KeyValueLeafPage on uncompressed bytes.
   */
  private void verifyKVLPChecksum(byte[] uncompressedData, PageReference reference,
                                   ResourceConfiguration resourceConfig) {
    if (reference == null || resourceConfig == null || !resourceConfig.verifyChecksumsOnRead) {
      return;
    }
    
    byte[] expectedHash = reference.getHash();
    if (expectedHash == null || expectedHash.length == 0) {
      return;
    }
    
    // Only verify for KVLP pages (hash was computed on uncompressed bytes)
    if (uncompressedData.length > 0 && uncompressedData[0] == PageKind.KEYVALUELEAFPAGE.getID()) {
      if (!PageHasher.verify(uncompressedData, expectedHash)) {
        byte[] actualHash = PageHasher.computeActualHash(uncompressedData);
        throw new SirixCorruptionException(reference.getKey(), "uncompressed-KVLP", expectedHash, actualHash);
      }
      
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("KVLP checksum verified for page at key {}", reference.getKey());
      }
    }
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
      // Verify KVLP checksum on uncompressed bytes before deserialization
      MemorySegment uncompressedSegment = decompressionResult.segment();
      verifyKVLPChecksumFromSegment(uncompressedSegment, reference, resourceConfiguration);
      
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

  /**
   * Verify checksum for KeyValueLeafPage on uncompressed MemorySegment.
   */
  private void verifyKVLPChecksumFromSegment(MemorySegment uncompressedSegment, PageReference reference,
                                              ResourceConfiguration resourceConfig) {
    if (reference == null || resourceConfig == null || !resourceConfig.verifyChecksumsOnRead) {
      return;
    }
    
    byte[] expectedHash = reference.getHash();
    if (expectedHash == null || expectedHash.length == 0) {
      return;
    }
    
    // Only verify for KVLP pages (hash was computed on uncompressed bytes)
    if (uncompressedSegment.byteSize() > 0) {
      byte pageTypeId = uncompressedSegment.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
      if (pageTypeId == PageKind.KEYVALUELEAFPAGE.getID()) {
        // Zero-copy verification for native segments with XXH3
        if (!PageHasher.verify(uncompressedSegment, expectedHash)) {
          byte[] actualHash = PageHasher.computeActualHash(uncompressedSegment);
          throw new SirixCorruptionException(reference.getKey(), "uncompressed-KVLP", expectedHash, actualHash);
        }
        
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("KVLP checksum verified for page at key {}", reference.getKey());
        }
      }
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
