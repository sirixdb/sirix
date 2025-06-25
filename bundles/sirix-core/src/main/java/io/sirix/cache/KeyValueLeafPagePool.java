package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.utils.OS;
import org.checkerframework.checker.index.qual.NonNegative;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.google.common.base.Preconditions.checkArgument;
import static io.sirix.cache.LinuxMemorySegmentAllocator.EIGHT_KB;
import static io.sirix.cache.MemorySegmentAllocator.SEGMENT_SIZES;
import static java.util.Objects.requireNonNull;

public final class KeyValueLeafPagePool {

  private final Deque<KeyValueLeafPage>[] pagePools;

  private MemorySegmentAllocator segmentAllocator =
      OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

  private static final KeyValueLeafPagePool INSTANCE = new KeyValueLeafPagePool();

  @SuppressWarnings("unchecked")
  private KeyValueLeafPagePool() {
    this.pagePools = new Deque[SEGMENT_SIZES.length];
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      pagePools[i] = new ConcurrentLinkedDeque<>();
    }
  }

  public static KeyValueLeafPagePool getInstance() {
    return INSTANCE;
  }

  // For unit tests.
  void setSegmentAllocator(MemorySegmentAllocator segmentAllocator) {
    this.segmentAllocator = segmentAllocator;
  }

  public KeyValueLeafPage borrowPage(final long recordPageKey, final int revisionNumber, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final boolean areDeweyIDsStored,
      final RecordSerializer recordPersister, final Map<Long, PageReference> references, final int slotMemorySize,
      final int deweyIdMemorySize, final int lastSlotIndex, final int lastDeweyIdIndex) {
    checkArgument(slotMemorySize > 0, "Size must be greater than zero.");
    requireNonNull(resourceConfig, "ResourceConfig must not be null.");
    checkArgument(revisionNumber >= 0, "Revision number must be greater than zero.");
    requireNonNull(indexType, "IndexType must not be null.");

    long roundedSize = SegmentAllocators.roundUpToPowerOfTwo(slotMemorySize);
    int index = SegmentAllocators.getIndexForSize(roundedSize);

    if (index < 0 || index >= pagePools.length) {
      throw new IllegalArgumentException("Unsupported size: " + slotMemorySize);
    }

    // Attempt to reuse a page from the pool
    KeyValueLeafPage page = pagePools[index].poll();

    if (page == null) {
      // No reusable page, allocate a new one
      MemorySegment slotSegment = segmentAllocator.allocate(roundedSize);
      // Initialize memory with zeros (proper initialization)
      slotSegment.fill((byte) 0x00);

      MemorySegment deweyIdSegment = null;
      if (resourceConfig.areDeweyIDsStored) {
        deweyIdSegment = segmentAllocator.allocate(deweyIdMemorySize);
        deweyIdSegment.fill((byte) 0x00);
      }

      page = new KeyValueLeafPage(recordPageKey,
                                  revisionNumber,
                                  indexType,
                                  resourceConfig,
                                  areDeweyIDsStored,
                                  recordPersister,
                                  references,
                                  slotSegment,
                                  deweyIdSegment,
                                  lastSlotIndex,
                                  lastDeweyIdIndex);
    } else {
      // Reset the page state completely
      page.clear();
      page.resetForReuse(recordPageKey,
                         revisionNumber,
                         indexType,
                         resourceConfig,
                         areDeweyIDsStored,
                         recordPersister,
                         references,
                         page.slots(),
                         page.deweyIds(),
                         lastSlotIndex,
                         lastDeweyIdIndex);
    }

    return page;
  }

  /**
   * Borrow a page of the given size (rounded to the nearest power of two).
   *
   * @param size           the size of the page to borrow.
   * @param recordPageKey  the record page key.
   * @param indexType      the type of index.
   * @param resourceConfig the resource configuration.
   * @param revisionNumber the revision number.
   * @return a reused or newly allocated KeyValueLeafPage.
   */
  public KeyValueLeafPage borrowPage(int size, final @NonNegative long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber) {
    checkArgument(size > 0, "Size must be greater than zero.");
    requireNonNull(resourceConfig, "ResourceConfig must not be null.");
    checkArgument(revisionNumber >= 0, "Revision number must be greater than zero.");
    requireNonNull(indexType, "IndexType must not be null.");

//    long roundedSize = SegmentAllocators.roundUpToPowerOfTwo(size);
//    int index = SegmentAllocators.getIndexForSize(roundedSize);
//
//    if (index < 0 || index >= pagePools.length) {
//      throw new IllegalArgumentException("Unsupported size: " + size);
//    }

    // Attempt to reuse a page from the pool
    KeyValueLeafPage page; // = pagePools[index].poll();

//    if (page == null) {
      // No reusable page, allocate a new one
      MemorySegment slotSegment = segmentAllocator.allocate(size);
      // Initialize memory with zeros (proper initialization)
      slotSegment.fill((byte) 0x00);

      MemorySegment deweyIdSegment = null;
      if (resourceConfig.areDeweyIDsStored) {
        deweyIdSegment = segmentAllocator.allocate(EIGHT_KB);
        deweyIdSegment.fill((byte) 0x00);
      }

      page =
          new KeyValueLeafPage(recordPageKey, indexType, resourceConfig, revisionNumber, slotSegment, deweyIdSegment);
//    } else {
//      // Reset the page state completely
//      page.resetForReuse(recordPageKey,
//                         revisionNumber,
//                         indexType,
//                         resourceConfig,
//                         resourceConfig.areDeweyIDsStored,
//                         resourceConfig.recordPersister,
//                         page.getReferencesMap(),
//                         page.slots(),
//                         page.deweyIds(),
//                         -1,
//                         -1);
//    }

    return page;
  }

  /**
   * Return a page to the pool for future reuse.
   *
   * @param page the page to return.
   */
  public void returnPage(KeyValueLeafPage page) {
    if (page == null)
      return;
//
//    long size = page.getSlotMemoryByteSize();
//
//    // DEBUG: Add validation
//    assert size > 0 : "Attempting to return page with zero slot memory size";
//
//    // Check if the size is in the predefined PAGE_SIZES
//    int index = -1;
//    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
//      if (SEGMENT_SIZES[i] == size) {
//        index = i;
//        break;
//      }
//    }
//
//    if (index == -1) {
//      throw new IllegalStateException(
//          "Returned page with invalid size: " + size + ". Valid sizes: " + Arrays.toString(SEGMENT_SIZES));
//    }
//
//    // Clear the page and return it to the pool
//    page.clear();
//
//    // DEBUG: Validate after clear
//    long sizeAfterClear = page.getSlotMemoryByteSize();
//    assert sizeAfterClear == size : "Page size changed after clear: " + sizeAfterClear + " vs " + size;
//
//    pagePools[index].offer(page);
  }
}
