package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.Bytes;
import io.sirix.page.PageLayout;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;

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
   * Verify the parent reference's page hash against the payload as it sits on disk.
   *
   * <p>
   * The hash covers the compressed payload for every page kind, {@code KeyValueLeafPage} included —
   * the writer computes it over the serialized, byte-handler-processed bytes — so verification always
   * happens BEFORE the payload is decoded, and nothing re-verifies a page afterwards.
   * </p>
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

    if (!reference.hasHash()) {
      return; // No hash to verify
    }

    // All page types use hash computed on compressed data
    final long expectedHash = reference.getHashAsLong();
    final HashAlgorithm hashAlgorithm = resourceConfig.hashAlgorithm;
    final long actualHash = PageHasher.computeLong(compressedData, hashAlgorithm);
    if (actualHash != expectedHash) {
      throw new SirixCorruptionException(reference.getKey(), "compressed", HashAlgorithm.longToBytes(expectedHash),
          HashAlgorithm.longToBytes(actualHash));
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

    if (!reference.hasHash()) {
      return;
    }

    // MMFileReader can race a concurrent storage close, so arbitrary MemorySegments stay on the FFM
    // checked-access API instead of escaping to an unpinned raw address.
    final long expectedHash = reference.getHashAsLong();
    final HashAlgorithm hashAlgorithm = resourceConfig.hashAlgorithm;
    final long actualHash = PageHasher.computeLong(compressedSegment, hashAlgorithm);
    if (actualHash != expectedHash) {
      throw new SirixCorruptionException(reference.getKey(), "compressed", HashAlgorithm.longToBytes(expectedHash),
          HashAlgorithm.longToBytes(actualHash));
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Checksum verified for page at key {}", reference.getKey());
    }
  }

  /** Verify a FileChannel buffer while its borrower retains exclusive ownership. */
  protected void verifyChecksumIfNeeded(final ByteBuffer compressedBuffer, final PageReference reference,
      final ResourceConfiguration resourceConfig) {
    if (resourceConfig == null || !resourceConfig.verifyChecksumsOnRead || !reference.hasHash()) {
      return;
    }
    final long expectedHash = reference.getHashAsLong();
    final long actualHash = PageHasher.computeLong(compressedBuffer, resourceConfig.hashAlgorithm);
    if (actualHash != expectedHash) {
      throw new SirixCorruptionException(reference.getKey(), "compressed", HashAlgorithm.longToBytes(expectedHash),
          HashAlgorithm.longToBytes(actualHash));
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Checksum verified for page at key {}", reference.getKey());
    }
  }

  /**
   * Build the synthetic {@link PageReference} used to integrity-check a RevisionRootPage body on the
   * {@code readRevisionRootPage} path (which has no real parent reference). The stored page hash is a
   * {@code long} read from the revisions record; it is stored directly in the synthetic reference so
   * {@link #verifyChecksumIfNeeded} can compare primitives without allocating a byte array.
   *
   * <p>
   * This is the single place both readers (FileChannel + MemoryMapped) build the reference, so the
   * byte-order contract can never diverge between them. A {@code storedPageHash} of {@code 0} (legacy
   * beta1 record, or a backend that does not persist page bytes) yields a reference with no hash,
   * which {@link #verifyChecksumIfNeeded} treats as "nothing to verify".
   *
   * @param dataFileOffset the RevisionRootPage offset (becomes the reference key, for error msgs)
   * @param storedPageHash the record's hash field ({@code 0} = no hash / legacy)
   * @return a reference carrying the primitive hash, or no hash when {@code storedPageHash == 0}
   */
  protected static PageReference revisionRootReference(final long dataFileOffset, final long storedPageHash) {
    final PageReference reference = new PageReference();
    reference.setKey(dataFileOffset);
    if (storedPageHash != 0L) {
      reference.setHash(storedPageHash);
    }
    return reference;
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
  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page, PageReference reference)
      throws IOException {
    return deserialize(resourceConfiguration, page, reference, false);
  }

  /**
   * Deserialize a page, optionally leaving a record page's records unexpanded until read.
   *
   * @param lazyRecordPage request the lazy variant; see {@link Reader#readRecordPageLazily}. Ignored
   *        by every page kind but a chunk-framed record page.
   */
  public Page deserialize(ResourceConfiguration resourceConfiguration, byte[] page, PageReference reference,
      boolean lazyRecordPage) throws IOException {
    // Use MemorySegment path if supported (zero-copy decompression)
    if (byteHandler.supportsMemorySegments()) {
      MemorySegment segment = MemorySegment.ofArray(page);
      return deserializeFromSegment(resourceConfiguration, segment, reference, lazyRecordPage);
    }

    // Fallback to stream-based approach for non-MemorySegment ByteHandlers
    byte[] decompressedBytes;
    try (final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
      decompressedBytes = inputStream.readAllBytes();
    }

    // Zero-copy wrap: MemorySegment backed directly by the byte array
    final var source = Bytes.wrapForRead(decompressedBytes);
    final var deserializedPage = lazyRecordPage
        ? pagePersister.deserializePageLazily(resourceConfiguration, source, type, null)
        : pagePersister.deserializePage(resourceConfiguration, source, type);

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
   * <p>
   * Uses the scoped decompression API to ensure decompression buffers are returned to the pool after
   * deserialization completes. This bounds memory usage by pool size (typically 2×CPU cores) rather
   * than thread count.
   * 
   * <p>
   * For KeyValueLeafPages, the page may take ownership of the decompression buffer via
   * {@link ByteHandler.DecompressionResult#transferOwnership()}, enabling true zero-copy where the
   * decompressed data becomes the page's slotMemory directly.
   *
   * @param resourceConfiguration resource configuration
   * @param compressedPage compressed page data
   * @return deserialized page
   * @throws IOException if deserialization fails
   */
  public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage)
      throws IOException {
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
    return deserializeFromSegment(resourceConfiguration, compressedPage, reference, false);
  }

  /**
   * Zero-copy deserialization, optionally leaving a record page's records unexpanded until read.
   *
   * @param lazyRecordPage request the lazy variant; see {@link Reader#readRecordPageLazily}. Ignored
   *        by every page kind but a chunk-framed record page.
   */
  public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage,
      PageReference reference, boolean lazyRecordPage) throws IOException {
    if (!byteHandler.supportsMemorySegments()) {
      throw new UnsupportedOperationException("ByteHandler does not support MemorySegment operations");
    }

    // Decompress - ownership may be transferred to page for zero-copy
    var decompressionResult = byteHandler.decompressScoped(compressedPage);

    try {
      MemorySegment uncompressedSegment = decompressionResult.segment();

      // Pass DecompressionResult to enable zero-copy for KeyValueLeafPages
      final var source = new MemorySegmentBytesIn(uncompressedSegment);
      Page deserializedPage = lazyRecordPage
          ? pagePersister.deserializePageLazily(resourceConfiguration, source, type, decompressionResult)
          : pagePersister.deserializePage(resourceConfiguration, source, type, decompressionResult);

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
   * Decode only the PAX regions from an already-read page image — see
   * {@link Reader#readRegionsOnly(PageReference, ResourceConfiguration, int)}.
   *
   * <p>
   * Shares the outer decompression with the full path (the default pipeline is a no-op, so this is
   * typically a wrap rather than a copy); what it does not share is the body blob, which is stepped
   * over by its length prefix instead of being expanded into a record heap.
   *
   * @param resourceConfiguration resource configuration
   * @param compressedPage the page image as read from storage
   * @param regionKindMask bitmask of region kinds to read
   * @param regionDeferMask subset left compressed until first use
   * @return the decoded regions, or {@code null} when the page is not a record page
   */
  protected RegionsOnlyPage deserializeRegionsOnlyFromSegment(ResourceConfiguration resourceConfiguration,
      MemorySegment compressedPage, int regionKindMask, int regionDeferMask) throws IOException {
    if (!byteHandler.supportsMemorySegments()) {
      throw new UnsupportedOperationException("ByteHandler does not support MemorySegment operations");
    }
    var decompressionResult = byteHandler.decompressScoped(compressedPage);
    try {
      return pagePersister.deserializeRegionsOnlyPage(resourceConfiguration,
          new MemorySegmentBytesIn(decompressionResult.segment()), regionKindMask, regionDeferMask);
    } finally {
      decompressionResult.close();
    }
  }

  /**
   * Bytes fetched from the front of a page to learn where its region table starts. The header is ~190
   * bytes at most; the rest of this is slack so the probe is one read even if the format grows.
   */
  protected static final int REGION_PROBE_BYTES = 256;

  /**
   * Bytes fetched from the region table on the first attempt. A scan's columns — values, field names,
   * the dictionary sketch — sit at the front of the table (see {@code RegionTable}'s write order) and
   * run to about 1.5 KB on the reference corpus, so this covers them with room to spare while still
   * being a fraction of a ~26 KB page. Tunable for workloads with wider columns.
   */
  protected static final int REGION_CHUNK_BYTES = Integer.getInteger("sirix.page.regionChunkBytes", 4096);

  /**
   * Per-thread scratch holding the probe's
   * {@code [pageKey, revision, populatedCount, fsstDictId, hasCompleteColumnCoverage]}.
   *
   * <p>
   * The dictionary id is the reason a bounded read can ever serve an FSST resource: on a monolith
   * page it sits in the tail, behind everything the probe is trying not to read, so those pages have
   * to decline; a chunked page carries it in the body prefix, which the probe passes through anyway.
   */
  protected static final ThreadLocal<long[]> PROBE_OUT = ThreadLocal.withInitial(() -> new long[5]);

  /**
   * Per-thread scratch for the probe's slot bitmap.
   *
   * <p>
   * Copied out before it reaches a page, since the page outlives the call: the chunk path previously
   * discarded the bitmap entirely, which left every chunk-read fragment unusable for the versioned
   * column merge.
   */
  protected static final ThreadLocal<long[]> PROBE_BITMAP =
      ThreadLocal.withInitial(() -> new long[PageLayout.BITMAP_WORDS]);

  /** Column-only pages answered from a bounded chunk read. */
  private static final LongAdder REGION_CHUNK_HITS = new LongAdder();

  /** Column-only pages the chunk read declined, leaving the whole page image to be read. */
  private static final LongAdder REGION_CHUNK_FALLBACKS = new LongAdder();

  /**
   * Number of column-only pages served from a bounded chunk read since the last
   * {@link #resetRegionChunkStats()}.
   *
   * <p>
   * Unconditional rather than gated behind a diagnostic flag, unlike the byte accounting in
   * {@code PageKind}. A chunk read costs two positional reads; a striped counter increment is three
   * orders of magnitude below that, so the measurement is free at this granularity — and a flag
   * nobody sets is precisely how this path came to be silently disabled twice. The chunk read
   * declines by returning {@code null} and letting the whole-page read answer, which is correct,
   * produces identical results, and is therefore invisible except as a slowdown. This counter and
   * {@link #regionChunkFallbacks()} are what make "is the fast path actually on" answerable — by a
   * test, and by an operator staring at a benchmark that regressed for no visible reason.
   */
  public static long regionChunkHits() {
    return REGION_CHUNK_HITS.sum();
  }

  /** Column-only reads that fell back to the whole page — see {@link #regionChunkHits()}. */
  public static long regionChunkFallbacks() {
    return REGION_CHUNK_FALLBACKS.sum();
  }

  /** Reset both chunk-read counters. */
  public static void resetRegionChunkStats() {
    REGION_CHUNK_HITS.reset();
    REGION_CHUNK_FALLBACKS.reset();
  }

  /** Record the outcome of one column-only read attempt; {@code page} is the chunk read's result. */
  protected static @Nullable RegionsOnlyPage recordChunkOutcome(final @Nullable RegionsOnlyPage page) {
    if (page == null) {
      REGION_CHUNK_FALLBACKS.increment();
    } else {
      REGION_CHUNK_HITS.increment();
    }
    return page;
  }

  /**
   * Decode a region table out of a partial page image.
   *
   * <p>
   * {@code headerImage} must start at the page's first byte; {@code regionImage} must start at the
   * region table (the offset {@code probeRegionTableOffset} reported). Returns {@code null} when the
   * requested regions do not fit in {@code regionImage}, which the caller answers by fetching more —
   * the table is self-describing, so running out of bytes is detected, never misread.
   */
  protected RegionsOnlyPage deserializeRegionTableFromChunk(ResourceConfiguration resourceConfiguration,
      MemorySegment regionImage, long pageKey, int revision, int populatedCount, long fsstSymbolTableId,
      int regionKindMask, int regionDeferMask, final long @Nullable [] slotBitmap,
      final boolean hasCompleteColumnCoverage) {
    try {
      return pagePersister.deserializeRegionTableAt(resourceConfiguration, new MemorySegmentBytesIn(regionImage),
          pageKey, revision, populatedCount, fsstSymbolTableId, regionKindMask, regionDeferMask, slotBitmap,
          hasCompleteColumnCoverage);
    } catch (final IndexOutOfBoundsException | IllegalStateException e) {
      // Ran off the end of the chunk: the caller re-reads with the full page.
      return null;
    }
  }

  @Override
  public PageReference readUberPageReference() {
    try {
      return readVerifiedBeacon(IOStorage.PRIMARY_BEACON_OFFSET);
    } catch (final Exception primaryException) {
      LOGGER.warn("Primary UberPage beacon at offset {} is corrupt, attempting secondary beacon at offset {}",
          IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET, primaryException);

      try {
        final PageReference ref = readVerifiedBeacon(IOStorage.SECONDARY_BEACON_OFFSET);
        LOGGER.info("Successfully recovered UberPage from secondary beacon at offset {}",
            IOStorage.SECONDARY_BEACON_OFFSET);
        return ref;
      } catch (final Exception secondaryException) {
        LOGGER.error("Both UberPage beacons are corrupt — unrecoverable");
        primaryException.addSuppressed(secondaryException);
        // Rethrowing the primary's exception as-is buried the fact that BOTH copies failed —
        // wrap with a headline stating it, keeping the primary as cause (secondary suppressed).
        throw new SirixIOException("Both UberPage beacon copies are corrupt — unrecoverable (primary: "
            + primaryException.getMessage() + "; secondary: " + secondaryException.getMessage() + ")",
            primaryException);
      }
    }
  }

  /**
   * Reads, integrity-checks, and deserializes one uber beacon slot:
   * {@code [u32 len][payload][u64 xxh3]}. The beacons have no parent reference to carry a page hash,
   * so before this trailer existed "valid" meant "deserialization didn't throw" — about three
   * effective bytes of validation on the root of the whole resource.
   */
  private PageReference readVerifiedBeacon(final long offset) {
    final java.nio.ByteBuffer slot = readBeaconSlot(offset);
    // A data file truncated inside the first 4 beacon bytes must fail like every other corrupt
    // slot, not with a raw BufferUnderflowException from getInt().
    if (slot.remaining() < Integer.BYTES) {
      throw new SirixIOException("Truncated beacon slot at offset " + offset);
    }
    // The length prefix is written through the buffered page writer (platform byte order, like
    // every page record's prefix — the superblock's endianness check gates foreign hosts).
    slot.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    final int len = slot.getInt();
    if (len <= 0 || len > IOStorage.BEACON_SLOT_BYTES - Integer.BYTES - Long.BYTES) {
      throw new SirixIOException("Implausible beacon length " + len + " at offset " + offset);
    }
    if (slot.remaining() < len + Long.BYTES) {
      throw new SirixIOException("Truncated beacon slot at offset " + offset);
    }
    final byte[] payload = new byte[len];
    slot.get(payload);
    // The slot remains little-endian for its framing fields, while the checksum trailer preserves
    // the established canonical big-endian bytes. Reverse once at the primitive boundary.
    final long storedHash = Long.reverseBytes(slot.getLong());
    final long actualHash = PageHasher.computeLong(payload);
    if (storedHash != actualHash) {
      throw new SirixIOException(
          "Beacon checksum mismatch at offset " + offset + " — torn or corrupted uber-page copy");
    }
    try {
      final PageReference ref = new PageReference();
      ref.setKey(offset);
      final UberPage page = (UberPage) deserialize(null, payload, ref);
      ref.setPage(page);
      return ref;
    } catch (final java.io.IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Reads (at least) the {@code [u32 len][payload][u64 xxh3]} prefix of the beacon slot at the given
   * offset. Implementations may return the whole {@link IOStorage#BEACON_SLOT_BYTES} slot.
   */
  protected abstract java.nio.ByteBuffer readBeaconSlot(long offset);

  /**
   * The revision number advertised by the beacon slot at the given offset, or {@code -1} when the
   * slot is torn/corrupt/absent. Crash-recovery truncation uses this to detect (and repair) a slot
   * left advertising a revision the truncation just removed.
   */
  public final int beaconRevisionOrMinusOne(final long offset) {
    try {
      return ((UberPage) readVerifiedBeacon(offset).getPage()).getRevisionNumber();
    } catch (final RuntimeException e) {
      return -1;
    }
  }

  public ByteHandler getByteHandler() {
    return byteHandler;
  }
}
