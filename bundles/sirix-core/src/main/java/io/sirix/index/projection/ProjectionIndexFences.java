/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Persistent local-order and normal-backbone routing metadata for projection row groups.
 *
 * <p>
 * Stable document node keys are identities, not order labels. Every live physical leaf is therefore
 * linked explicitly in document order. Only rows whose KEYS exception bit is clear take part in
 * numeric fence routing; those rows form a globally strictly-increasing backbone. Sparse exceptions
 * are resolved by {@link ProjectionRecordLocator} before this structure is consulted. Initial build
 * leaves are immutable base/sentinel heads. Local split leaves belong to one base and are linked
 * into that base's numeric skip list only while they contain a normal row.
 * </p>
 */
public final class ProjectionIndexFences {

  /** First reserved positive slot for fence chunks. */
  static final long CHUNK_SLOT_BASE = 1L << 42;

  /**
   * Physical leaves per persistent unit. An entry carries explicit document links, owner and the
   * bounded numeric skip tower, so 32 entries keep a touched chunk near one ordinary page instead of
   * rewriting a 64+ KiB blob for a one-leaf change.
   */
  static final int CHUNK_LEAVES = 32;

  static final long ORDER_HEADER_SLOT = CHUNK_SLOT_BASE + (1L << 20);

  private static final int ORDER_MAGIC = 0x4F464950;
  private static final byte ORDER_VERSION = 1;
  private static final int SKIP_LEVELS = 25;
  private static final int DOCUMENT_SKIP_TAILS_OFFSET = 32;
  private static final int ORDER_HEADER_BYTES = DOCUMENT_SKIP_TAILS_OFFSET + SKIP_LEVELS * Integer.BYTES;

  private static final int FIRST_OFFSET = 0;
  private static final int LAST_OFFSET = 8;
  private static final int DOC_NEXT_OFFSET = 16;
  private static final int DOC_PREV_OFFSET = 20;
  private static final int OWNER_BASE_OFFSET = 24;
  private static final int NUMERIC_SKIP_OFFSET = 28;
  private static final int BASE_UPPER_OFFSET = NUMERIC_SKIP_OFFSET + SKIP_LEVELS * Integer.BYTES;
  private static final int FREE_NEXT_OFFSET = BASE_UPPER_OFFSET + Long.BYTES;
  private static final int DOCUMENT_BACK_SKIP_OFFSET = 144;
  private static final int ENTRY_BYTES = DOCUMENT_BACK_SKIP_OFFSET + SKIP_LEVELS * Integer.BYTES;

  private ProjectionIndexFences() {}

  public static int chunkCount(final int physicalRowGroupCount) {
    return (physicalRowGroupCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
  }

  /**
   * Initialise the V0 metadata for an explicit build. {@code first}/{@code last} are normal- backbone
   * bounds, so a nonempty exception-only leaf legitimately carries MAX/MIN sentinels.
   */
  public static void write(final ProjectionIndexHOTStorage storage, final int rowGroupCount, final long[] first,
      final long[] last) {
    if (storage == null) {
      throw new NullPointerException("storage is required");
    }
    checkRowGroupCount(rowGroupCount);
    if (first.length != rowGroupCount || last.length != rowGroupCount) {
      throw new IllegalArgumentException("fence arrays must carry exactly rowGroupCount " + rowGroupCount
          + " entries, got " + first.length + "/" + last.length);
    }
    final BuildWriter writer = new BuildWriter();
    for (int index = 0; index < rowGroupCount; index++) {
      writer.append(storage, first[index], last[index]);
    }
    writer.finish(storage);
  }

  /** Read physical-slot-aligned normal fence arrays, or {@code null} for malformed/missing chunks. */
  public static long @Nullable [][] read(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
    if (storage == null) {
      throw new NullPointerException("storage is required");
    }
    final OrderHeader header;
    try {
      header = readOrderHeader(storage.getBlob(ORDER_HEADER_SLOT), rowGroupCount);
    } catch (final IllegalStateException malformed) {
      return null;
    }
    final long[] first = new long[header.physicalRowGroupCount()];
    final long[] last = new long[header.physicalRowGroupCount()];
    for (int chunkId = 0; chunkId < chunkCount(header.physicalRowGroupCount()); chunkId++) {
      final int start = chunkId * CHUNK_LEAVES;
      final int entries = Math.min(CHUNK_LEAVES, header.physicalRowGroupCount() - start);
      final byte[] bytes = storage.getBlob(CHUNK_SLOT_BASE + chunkId);
      if (bytes == null || bytes.length != entries * ENTRY_BYTES) {
        return null;
      }
      for (int local = 0; local < entries; local++) {
        final int offset = local * ENTRY_BYTES;
        first[start + local] = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset + FIRST_OFFSET);
        last[start + local] = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset + LAST_OFFSET);
      }
    }
    return new long[][] {first, last};
  }

  public static int[] readPhysicalOrder(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount) {
    if (reader == null) {
      throw new NullPointerException("reader is required");
    }
    return readPhysicalOrder(slot -> ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slot), rowGroupCount);
  }

  static int[] readPhysicalOrder(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
    if (storage == null) {
      throw new NullPointerException("storage is required");
    }
    return readPhysicalOrder(storage::getBlob, rowGroupCount);
  }

  private static int[] readPhysicalOrder(final BlobReader reader, final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    final OrderHeader header = readOrderHeader(reader.read(ORDER_HEADER_SLOT), rowGroupCount);
    final int physicalCount = header.physicalRowGroupCount();
    final OrderEntryReader entries = new OrderEntryReader(reader, physicalCount);
    final int[] order = new int[rowGroupCount];
    int count = 0;
    int previous = 0;
    int slot = header.documentHead();
    while (slot != 0) {
      if (slot < 1 || slot > physicalCount || count == rowGroupCount
          || entries.intValue(slot, DOC_PREV_OFFSET) != previous || entries.intValue(slot, OWNER_BASE_OFFSET) < 1
          || entries.intValue(slot, OWNER_BASE_OFFSET) > header.baseRowGroupCount()) {
        throw new IllegalStateException("malformed projection document order at physical leaf " + slot);
      }
      order[count++] = slot;
      previous = slot;
      slot = entries.intValue(slot, DOC_NEXT_OFFSET);
    }
    if (previous != header.documentTail() || count != rowGroupCount) {
      throw new IllegalStateException(
          "projection document order reaches " + count + " of " + rowGroupCount + " live physical leaves");
    }
    return order;
  }

  private static final class OrderEntryReader {
    private final BlobReader reader;
    private final int physicalCount;
    private int cachedChunkId = -1;
    private byte @Nullable [] cachedChunk;

    private OrderEntryReader(final BlobReader reader, final int physicalCount) {
      this.reader = reader;
      this.physicalCount = physicalCount;
    }

    private int intValue(final int slot, final int fieldOffset) {
      if (slot < 1 || slot > physicalCount) {
        throw new IllegalStateException("projection physical leaf is out of range: " + slot);
      }
      final int chunkId = (slot - 1) / CHUNK_LEAVES;
      byte[] chunk = cachedChunk;
      if (chunkId != cachedChunkId) {
        chunk = reader.read(CHUNK_SLOT_BASE + chunkId);
        final int start = chunkId * CHUNK_LEAVES;
        final int expectedBytes = Math.min(CHUNK_LEAVES, physicalCount - start) * ENTRY_BYTES;
        if (chunk == null || chunk.length != expectedBytes) {
          throw new IllegalStateException("missing or malformed projection fence chunk " + chunkId);
        }
        cachedChunkId = chunkId;
        cachedChunk = chunk;
      }
      return ProjectionIndexRowGroupCodec.getIntLE(chunk, ((slot - 1) % CHUNK_LEAVES) * ENTRY_BYTES + fieldOffset);
    }
  }

  private static byte[] orderHeader(final int baseRowGroupCount, final int physicalRowGroupCount,
      final int liveRowGroupCount, final int freeHead, final int documentHead, final int documentTail,
      final int[] documentSkipTails) {
    final byte[] bytes = new byte[ORDER_HEADER_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, ORDER_MAGIC);
    bytes[Integer.BYTES] = ORDER_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 8, baseRowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 12, physicalRowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 16, liveRowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 20, freeHead);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 24, documentHead);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 28, documentTail);
    for (int level = 0; level < SKIP_LEVELS; level++) {
      ProjectionIndexRowGroupCodec.putIntLEAt(bytes, DOCUMENT_SKIP_TAILS_OFFSET + level * Integer.BYTES,
          documentSkipTails[level]);
    }
    return bytes;
  }

  private static OrderHeader readOrderHeader(final byte @Nullable [] bytes, final int rowGroupCount) {
    if (bytes == null || bytes.length != ORDER_HEADER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != ORDER_MAGIC || bytes[Integer.BYTES] != ORDER_VERSION
        || ProjectionIndexRowGroupCodec.getIntLE(bytes, 16) != rowGroupCount) {
      throw new IllegalStateException("missing or malformed projection row-group order header");
    }
    final int baseCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, 8);
    final int physicalCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, 12);
    final int freeHead = ProjectionIndexRowGroupCodec.getIntLE(bytes, 20);
    final int documentHead = ProjectionIndexRowGroupCodec.getIntLE(bytes, 24);
    final int documentTail = ProjectionIndexRowGroupCodec.getIntLE(bytes, 28);
    final int[] documentSkipTails = new int[SKIP_LEVELS];
    for (int level = 0; level < SKIP_LEVELS; level++) {
      final int tail = ProjectionIndexRowGroupCodec.getIntLE(bytes, DOCUMENT_SKIP_TAILS_OFFSET + level * Integer.BYTES);
      if (tail < 0 || tail > physicalCount) {
        throw new IllegalStateException("projection document skip tail is out of range at level " + level);
      }
      documentSkipTails[level] = tail;
    }
    if (rowGroupCount == 0) {
      for (final int tail : documentSkipTails) {
        if (tail != 0) {
          throw new IllegalStateException("empty projection document order retains a skip tail");
        }
      }
    }
    if ((rowGroupCount == 0 && documentSkipTails[0] != 0)
        || (rowGroupCount > 0 && documentSkipTails[0] != documentTail)) {
      throw new IllegalStateException("projection level-zero document skip tail is malformed");
    }
    if ((rowGroupCount == 0 && (documentHead != 0 || documentTail != 0))
        || (rowGroupCount > 0 && (baseCount < 1 || baseCount > physicalCount || physicalCount < rowGroupCount
            || documentHead < 1 || documentHead > physicalCount || documentTail < 1 || documentTail > physicalCount))
        || baseCount < 0 || baseCount > physicalCount || physicalCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS
        || freeHead < 0 || freeHead > physicalCount) {
      throw new IllegalStateException("projection row-group order header is out of range");
    }
    return new OrderHeader(baseCount, physicalCount, freeHead, documentHead, documentTail, documentSkipTails);
  }

  private record OrderHeader(int baseRowGroupCount, int physicalRowGroupCount, int freeHead, int documentHead,
      int documentTail, int[] documentSkipTails) {
  }

  private static void checkRowGroupCount(final int rowGroupCount) {
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("rowGroupCount out of range: " + rowGroupCount);
    }
  }

  /** @return true iff the range contains at least one normal-backbone key. */
  private static boolean validateNormalRange(final long first, final long last) {
    if (first == Long.MAX_VALUE && last == Long.MIN_VALUE) {
      return false;
    }
    if (first < 0 || last < first) {
      throw new IllegalStateException("invalid projection normal fence [" + first + ", " + last + "]");
    }
    return true;
  }

  private static boolean hasNormalRange(final long first, final long last) {
    return first != Long.MAX_VALUE || last != Long.MIN_VALUE;
  }

  static final class BuildWriter {
    private final byte[] chunk = new byte[CHUNK_LEAVES * ENTRY_BYTES];
    private int chunkEntries;
    private int chunksWritten;
    private int rowGroupCount;
    private long baseUpper = Long.MIN_VALUE;
    private final int[] documentSkipTails = new int[SKIP_LEVELS];
    private boolean finished;

    int rowGroupCount() {
      return rowGroupCount;
    }

    int chunksWritten() {
      return chunksWritten;
    }

    void append(final ProjectionIndexHOTStorage storage, final long first, final long last) {
      Objects.requireNonNull(storage, "storage must not be null");
      if (finished) {
        throw new IllegalStateException("projection fence build writer is already finished");
      }
      if (rowGroupCount == ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        throw new IllegalStateException("projection row-group limit reached");
      }
      final boolean normal = validateNormalRange(first, last);
      if (normal && first <= baseUpper) {
        throw new IllegalStateException("projection normal backbone is not strictly increasing at leaf "
            + (rowGroupCount + 1) + ": " + first + " <= " + baseUpper);
      }
      final int nextSlot = rowGroupCount + 1;
      if (chunkEntries > 0) {
        ProjectionIndexRowGroupCodec.putIntLEAt(chunk, (chunkEntries - 1) * ENTRY_BYTES + DOC_NEXT_OFFSET, nextSlot);
      }
      final int offset = chunkEntries * ENTRY_BYTES;
      Arrays.fill(chunk, offset, offset + ENTRY_BYTES, (byte) 0);
      RowGroupDescriptor.putLongLE(chunk, offset + FIRST_OFFSET, first);
      RowGroupDescriptor.putLongLE(chunk, offset + LAST_OFFSET, last);
      ProjectionIndexRowGroupCodec.putIntLEAt(chunk, offset + DOC_PREV_OFFSET, rowGroupCount);
      ProjectionIndexRowGroupCodec.putIntLEAt(chunk, offset + OWNER_BASE_OFFSET, nextSlot);
      final int height = documentSkipHeight(nextSlot);
      for (int level = 0; level < SKIP_LEVELS; level++) {
        ProjectionIndexRowGroupCodec.putIntLEAt(chunk, offset + DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES,
            documentSkipTails[level]);
        if (level < height) {
          documentSkipTails[level] = nextSlot;
        }
      }
      if (normal) {
        baseUpper = last;
      }
      RowGroupDescriptor.putLongLE(chunk, offset + BASE_UPPER_OFFSET, baseUpper);
      if (chunkEntries + 1 == CHUNK_LEAVES && nextSlot < ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        ProjectionIndexRowGroupCodec.putIntLEAt(chunk, offset + DOC_NEXT_OFFSET, nextSlot + 1);
      }
      chunkEntries++;
      rowGroupCount++;
      if (chunkEntries == CHUNK_LEAVES) {
        flushChunk(storage);
      }
    }

    void finish(final ProjectionIndexHOTStorage storage) {
      Objects.requireNonNull(storage, "storage must not be null");
      if (finished) {
        throw new IllegalStateException("projection fence build writer is already finished");
      }
      if (chunkEntries > 0) {
        flushChunk(storage);
      } else if (rowGroupCount > 0 && rowGroupCount % CHUNK_LEAVES == 0) {
        if (chunksWritten == 0) {
          throw new IllegalStateException("projection fence writer lost its completed chunks");
        }
        final long finalChunkSlot = CHUNK_SLOT_BASE + chunksWritten - 1L;
        final byte[] persistedFinalChunk = storage.getBlob(finalChunkSlot);
        if (persistedFinalChunk == null || persistedFinalChunk.length != CHUNK_LEAVES * ENTRY_BYTES) {
          throw new IllegalStateException("projection final fence chunk is unavailable at finish");
        }
        final byte[] finalChunk = persistedFinalChunk.clone();
        ProjectionIndexRowGroupCodec.putIntLEAt(finalChunk, (CHUNK_LEAVES - 1) * ENTRY_BYTES + DOC_NEXT_OFFSET, 0);
        storage.putBlob(finalChunkSlot, finalChunk);
      }
      storage.putBlob(ORDER_HEADER_SLOT, orderHeader(rowGroupCount, rowGroupCount, rowGroupCount, 0, rowGroupCount == 0
          ? 0
          : 1, rowGroupCount, documentSkipTails));
      finished = true;
    }

    private void flushChunk(final ProjectionIndexHOTStorage storage) {
      storage.putBlob(CHUNK_SLOT_BASE + chunksWritten, Arrays.copyOf(chunk, chunkEntries * ENTRY_BYTES));
      chunksWritten++;
      chunkEntries = 0;
    }
  }

  @FunctionalInterface
  private interface BlobReader {
    byte @Nullable [] read(long slot);
  }

  public static Accessor open(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
    return new Accessor(storage, rowGroupCount);
  }

  record DocumentPosition(int[] predecessors, int[] successors) {
    DocumentPosition {
      if (predecessors.length != SKIP_LEVELS || successors.length != SKIP_LEVELS) {
        throw new IllegalArgumentException("projection document position has the wrong skip height");
      }
    }
  }

  public static final class Accessor {
    private final ProjectionIndexHOTStorage storage;
    private final int priorPhysicalRowGroupCount;
    private final int priorBaseRowGroupCount;
    private final int priorLiveRowGroupCount;
    private final int priorFreeHead;
    private final int priorDocumentHead;
    private final int priorDocumentTail;
    private final int[] priorDocumentSkipTails;
    private int baseRowGroupCount;
    private int currentPhysicalRowGroupCount;
    private int liveRowGroupCount;
    private int freeHead;
    private int documentHead;
    private int documentTail;
    private final int[] documentSkipTails;
    private final Int2ObjectOpenHashMap<byte[]> chunks = new Int2ObjectOpenHashMap<>();
    private final IntOpenHashSet changedChunks = new IntOpenHashSet();
    private final IntOpenHashSet reusedSlots = new IntOpenHashSet();
    private final IntOpenHashSet allocatedSlots = new IntOpenHashSet();
    /** Owner-confined skip-search scratch, reused by every local numeric mutation/validation. */
    private final int[] numericPredecessors = new int[SKIP_LEVELS];
    private int chunksRead;
    private int chunksWritten;
    private long bytesRead;
    private long bytesWritten;

    private Accessor(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
      if (storage == null) {
        throw new NullPointerException("storage is required");
      }
      checkRowGroupCount(rowGroupCount);
      this.storage = storage;
      final OrderHeader header = readOrderHeader(storage.getBlob(ORDER_HEADER_SLOT), rowGroupCount);
      priorPhysicalRowGroupCount = header.physicalRowGroupCount();
      priorBaseRowGroupCount = header.baseRowGroupCount();
      priorLiveRowGroupCount = rowGroupCount;
      priorFreeHead = header.freeHead();
      priorDocumentHead = header.documentHead();
      priorDocumentTail = header.documentTail();
      priorDocumentSkipTails = header.documentSkipTails().clone();
      currentPhysicalRowGroupCount = priorPhysicalRowGroupCount;
      baseRowGroupCount = priorBaseRowGroupCount;
      liveRowGroupCount = rowGroupCount;
      freeHead = priorFreeHead;
      documentHead = priorDocumentHead;
      documentTail = priorDocumentTail;
      documentSkipTails = priorDocumentSkipTails.clone();
    }

    public long first(final int slot) {
      return longValue(slot, FIRST_OFFSET);
    }

    public long last(final int slot) {
      return longValue(slot, LAST_OFFSET);
    }

    public int next(final int slot) {
      return intValue(slot, DOC_NEXT_OFFSET);
    }

    public int previous(final int slot) {
      return intValue(slot, DOC_PREV_OFFSET);
    }

    public int ownerBase(final int slot) {
      return intValue(slot, OWNER_BASE_OFFSET);
    }

    public int documentHead() {
      return documentHead;
    }

    DocumentPosition documentPosition(final byte[] orderLabel, final ProjectionPersistedRecordLookup lookup) {
      Objects.requireNonNull(orderLabel, "orderLabel must not be null");
      Objects.requireNonNull(lookup, "lookup must not be null");
      if (orderLabel.length == 0) {
        throw new IllegalArgumentException("projection Dewey order label must not be empty");
      }
      final int[] predecessors = new int[SKIP_LEVELS];
      final int[] successors = new int[SKIP_LEVELS];
      int successor = 0;
      int traversed = 0;
      for (int level = SKIP_LEVELS - 1; level >= 0; level--) {
        int cursor = successor == 0
            ? documentSkipTails[level]
            : successor;
        while (cursor != 0 && compareFirstOrderLabel(cursor, orderLabel, lookup) >= 0) {
          successor = cursor;
          cursor = checkedDocumentBack(cursor, level);
          if (++traversed > currentPhysicalRowGroupCount + SKIP_LEVELS) {
            throw new IllegalStateException("projection document skip routing did not advance");
          }
        }
        predecessors[level] = cursor;
        successors[level] = successor;
      }
      return new DocumentPosition(predecessors, successors);
    }

    DocumentPosition documentTailPosition() {
      return new DocumentPosition(documentSkipTails.clone(), new int[SKIP_LEVELS]);
    }

    public int lastPhysicalSlot() {
      return documentTail;
    }

    public int findSlot(final long recordKey) {
      if (recordKey < 0 || baseRowGroupCount == 0) {
        return -1;
      }
      int low = 1;
      int high = baseRowGroupCount;
      while (low <= high) {
        final int middle = (low + high) >>> 1;
        if (baseUpper(middle) < recordKey) {
          low = middle + 1;
        } else {
          high = middle - 1;
        }
      }
      if (low > baseRowGroupCount) {
        return -1;
      }
      final int base = low;
      if (containsNormal(base, recordKey)) {
        return base;
      }
      int predecessor = base;
      for (int level = SKIP_LEVELS - 1; level >= 0; level--) {
        int candidate;
        while ((candidate = checkedNumericSuccessor(predecessor, level, base)) != 0 && last(candidate) < recordKey) {
          predecessor = candidate;
        }
      }
      final int candidate = checkedNumericSuccessor(predecessor, 0, base);
      return candidate != 0 && containsNormal(candidate, recordKey)
          ? candidate
          : -1;
    }

    /** Compatibility helper; document-relative insertion should use an explicitly located neighbor. */
    public int insertionSlot(final long recordKey) {
      final int exact = findSlot(recordKey);
      if (exact >= 1) {
        return exact;
      }
      if (liveRowGroupCount == 0) {
        return -1;
      }
      int low = 1;
      int high = baseRowGroupCount;
      while (low <= high) {
        final int middle = (low + high) >>> 1;
        if (baseUpper(middle) < recordKey) {
          low = middle + 1;
        } else {
          high = middle - 1;
        }
      }
      return low > baseRowGroupCount
          ? documentTail
          : low;
    }

    public long maxRecordKey() {
      return baseRowGroupCount == 0
          ? Long.MIN_VALUE
          : baseUpper(baseRowGroupCount);
    }

    /** Whether a new document-tail row can safely extend the final base's numeric backbone. */
    public boolean canExtendLastBaseFrom(final int physicalSlot, final long recordKey) {
      return baseRowGroupCount > 0 && isLivePhysicalSlot(physicalSlot) && ownerBase(physicalSlot) == baseRowGroupCount
          && recordKey > baseUpper(baseRowGroupCount);
    }

    /** Extend only the immutable last base's ownership boundary for a proven document-tail append. */
    public void extendLastBaseUpper(final long recordKey) {
      if (baseRowGroupCount == 0 || recordKey <= baseUpper(baseRowGroupCount)) {
        throw new IllegalArgumentException("tail backbone key does not extend the last base: " + recordKey);
      }
      setLong(baseRowGroupCount, BASE_UPPER_OFFSET, recordKey);
    }

    /**
     * Establish slot 1 as the first immutable base after an explicitly built empty projection. This is
     * the only transition that grows the base set during ordinary maintenance; every later local
     * overflow uses {@link #allocateSlot()} and {@link #linkAfter(int, int)}.
     */
    public int bootstrapFirstBase() {
      if (baseRowGroupCount != 0 || currentPhysicalRowGroupCount != 0 || liveRowGroupCount != 0 || freeHead != 0
          || documentHead != 0 || documentTail != 0 || !allocatedSlots.isEmpty() || !reusedSlots.isEmpty()) {
        throw new IllegalStateException("first projection base can only be bootstrapped from an empty store");
      }
      baseRowGroupCount = 1;
      currentPhysicalRowGroupCount = 1;
      liveRowGroupCount = 1;
      documentHead = 1;
      documentTail = 1;
      allocatedSlots.add(1);
      clearEntry(1);
      setInt(1, OWNER_BASE_OFFSET, 1);
      setLong(1, BASE_UPPER_OFFSET, Long.MIN_VALUE);
      for (int level = 0; level < documentSkipHeight(1); level++) {
        documentSkipTails[level] = 1;
      }
      return 1;
    }

    int bootstrapDocumentBase(final long recordKey) {
      if (liveRowGroupCount != 0 || documentHead != 0 || documentTail != 0) {
        throw new IllegalStateException("a projection document base can only be bootstrapped from an empty order");
      }
      if (baseRowGroupCount == 0) {
        return bootstrapFirstBase();
      }
      int low = 1;
      int high = baseRowGroupCount;
      while (low <= high) {
        final int middle = (low + high) >>> 1;
        if (baseUpper(middle) < recordKey) {
          low = middle + 1;
        } else {
          high = middle - 1;
        }
      }
      final int slot = Math.min(low, baseRowGroupCount);
      if (ownerBase(slot) != 0 || previous(slot) != 0 || next(slot) != 0) {
        throw new IllegalStateException("projection dormant base " + slot + " is not empty");
      }
      setInt(slot, OWNER_BASE_OFFSET, slot);
      for (int level = 0; level < SKIP_LEVELS; level++) {
        if (documentSkipTails[level] != 0) {
          throw new IllegalStateException("projection empty document order retains a skip tail at level " + level);
        }
        setInt(slot, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES, 0);
        if (level < documentSkipHeight(slot)) {
          documentSkipTails[level] = slot;
        }
      }
      documentHead = slot;
      documentTail = slot;
      liveRowGroupCount = 1;
      return slot;
    }

    public int allocateSlot() {
      final int slot;
      if (freeHead != 0) {
        slot = freeHead;
        if (slot <= baseRowGroupCount || slot > currentPhysicalRowGroupCount || allocatedSlots.contains(slot)
            || ownerBase(slot) != 0 || previous(slot) != 0 || next(slot) != 0) {
          throw new IllegalStateException("malformed projection row-group free head " + slot);
        }
        final int nextFree = freeNext(slot);
        if (nextFree < 0 || nextFree > currentPhysicalRowGroupCount || nextFree == slot) {
          throw new IllegalStateException("malformed projection row-group free edge " + slot + " -> " + nextFree);
        }
        freeHead = nextFree;
        reusedSlots.add(slot);
      } else {
        if (currentPhysicalRowGroupCount == ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
          throw new IllegalStateException("projection row-group limit reached");
        }
        slot = ++currentPhysicalRowGroupCount;
      }
      allocatedSlots.add(slot);
      liveRowGroupCount++;
      clearEntry(slot);
      return slot;
    }

    public boolean wasReused(final int slot) {
      return reusedSlots.contains(slot);
    }

    public boolean isLivePhysicalSlot(final int slot) {
      if (slot < 1 || slot > currentPhysicalRowGroupCount) {
        return false;
      }
      if (allocatedSlots.contains(slot)) {
        return true;
      }
      return ownerBase(slot) != 0;
    }

    public boolean canRecycle(final int slot) {
      return slot > baseRowGroupCount && slot <= currentPhysicalRowGroupCount;
    }

    void recycle(final int slot, final DocumentPosition positionAfterSlot) {
      if (!canRecycle(slot) || !isLivePhysicalSlot(slot)) {
        throw new IllegalArgumentException("projection row group is not a live recyclable split leaf: " + slot);
      }
      validateDocumentLinks(slot);
      if (hasNormal(slot)) {
        unlinkNumeric(slot);
      }
      unlinkDocumentSkip(slot, positionAfterSlot);
      final int prev = previous(slot);
      final int next = next(slot);
      if (prev == 0) {
        documentHead = next;
      } else {
        setInt(prev, DOC_NEXT_OFFSET, next);
      }
      if (next == 0) {
        documentTail = prev;
      } else {
        setInt(next, DOC_PREV_OFFSET, prev);
      }
      clearEntry(slot);
      setInt(slot, FREE_NEXT_OFFSET, freeHead);
      freeHead = slot;
      reusedSlots.remove(slot);
      allocatedSlots.remove(slot);
      liveRowGroupCount--;
    }

    void retireEmptyBase(final int slot, final DocumentPosition positionAfterSlot) {
      if (slot < 1 || slot > baseRowGroupCount || !isLivePhysicalSlot(slot)) {
        throw new IllegalArgumentException("projection row group is not a live base leaf: " + slot);
      }
      validateDocumentLinks(slot);
      unlinkDocumentSkip(slot, positionAfterSlot);
      final int predecessor = previous(slot);
      final int successor = next(slot);
      if (predecessor == 0) {
        documentHead = successor;
      } else {
        setInt(predecessor, DOC_NEXT_OFFSET, successor);
      }
      if (successor == 0) {
        documentTail = predecessor;
      } else {
        setInt(successor, DOC_PREV_OFFSET, predecessor);
      }
      setLong(slot, FIRST_OFFSET, Long.MAX_VALUE);
      setLong(slot, LAST_OFFSET, Long.MIN_VALUE);
      setInt(slot, OWNER_BASE_OFFSET, 0);
      setInt(slot, DOC_PREV_OFFSET, 0);
      setInt(slot, DOC_NEXT_OFFSET, 0);
      for (int level = 0; level < SKIP_LEVELS; level++) {
        setInt(slot, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES, 0);
      }
      liveRowGroupCount--;
    }

    public int liveRowGroupCount() {
      return liveRowGroupCount;
    }

    public int physicalRowGroupCount() {
      return currentPhysicalRowGroupCount;
    }

    /** Set a physical leaf's normal-backbone fence; rows themselves remain in document order. */
    public void set(final int slot, final long first, final long last) {
      if (slot < 1 || slot > currentPhysicalRowGroupCount) {
        throw new IllegalArgumentException("fence slot outside current physical range: " + slot);
      }
      validateNormalRange(first, last);
      // A membership rewrite can change only sparse exception rows while leaving the normal
      // backbone byte-identical. Do not unlink/relink a split or dirty its shared fence chunk in
      // that case: the row-group payload is the unit that changed, not its routing metadata.
      if (first(slot) == first && last(slot) == last) {
        return;
      }
      final boolean linkedSplit = slot > baseRowGroupCount && ownerBase(slot) != 0;
      if (linkedSplit && hasNormal(slot)) {
        unlinkNumeric(slot);
      }
      setLong(slot, FIRST_OFFSET, first);
      setLong(slot, LAST_OFFSET, last);
      if (linkedSplit && hasNormalRange(first, last)) {
        insertNumeric(slot);
      }
    }

    /** Splice a freshly allocated local split at its explicitly located document position. */
    void linkAfter(final int slot, final int newSlot, final DocumentPosition position) {
      if (!isLivePhysicalSlot(slot) || !allocatedSlots.contains(newSlot) || ownerBase(newSlot) != 0) {
        throw new IllegalArgumentException("invalid projection row-group link " + slot + " -> " + newSlot);
      }
      validateDocumentLinks(slot);
      if (previous(newSlot) != 0 || next(newSlot) != 0) {
        throw new IllegalStateException("fresh projection row group already has document links: " + newSlot);
      }
      final int owner = ownerBase(slot);
      if (owner < 1 || owner > baseRowGroupCount) {
        throw new IllegalStateException("projection row group has no valid base owner: " + slot);
      }
      final int successor = next(slot);
      linkDocumentSkip(slot, newSlot, position);
      setInt(newSlot, OWNER_BASE_OFFSET, owner);
      setInt(newSlot, DOC_PREV_OFFSET, slot);
      setInt(newSlot, DOC_NEXT_OFFSET, successor);
      setInt(slot, DOC_NEXT_OFFSET, newSlot);
      if (successor == 0) {
        documentTail = newSlot;
      } else {
        setInt(successor, DOC_PREV_OFFSET, newSlot);
      }
      if (hasNormal(newSlot)) {
        insertNumeric(newSlot);
      }
    }

    /**
     * Validate one live leaf's local document-order position without walking the whole projection. Both
     * adjacent identities must be live, carry valid base ownership, and point back to this leaf; a
     * boundary leaf must agree with the persisted head/tail fields.
     */
    void validateDocumentLinks(final int slot) {
      validateLiveOwner(slot);
      final int predecessor = previous(slot);
      if (predecessor == 0) {
        if (documentHead != slot) {
          throw new IllegalStateException("projection document head does not name leaf " + slot);
        }
      } else {
        validateLiveOwner(predecessor);
        if (next(predecessor) != slot) {
          throw new IllegalStateException(
              "projection predecessor " + predecessor + " does not point back to leaf " + slot);
        }
      }

      final int successor = next(slot);
      if (successor == 0) {
        if (documentTail != slot) {
          throw new IllegalStateException("projection document tail does not name leaf " + slot);
        }
      } else {
        validateLiveOwner(successor);
        if (previous(successor) != slot) {
          throw new IllegalStateException("projection successor " + successor + " does not point back to leaf " + slot);
        }
      }
    }

    /**
     * Validate one touched leaf against the persistent normal-key routing structure without walking
     * through document-order exception-only leaves.
     *
     * <p>
     * Exception-only leaves are deliberately absent from numeric routing. Their only local invariant is
     * therefore the reciprocal document link checked above; scanning across a run of such leaves adds
     * no routing evidence and makes a one-leaf update proportional to unrelated document history. A
     * normal base is checked against its immutable owner interval and its first level-zero successor. A
     * normal split is found through the bounded numeric skip structure and its immediate
     * predecessor/successor edges are revalidated. The numeric mutation methods already use the same
     * predecessor search when a split enters, leaves, or moves in that chain.
     * </p>
     */
    void validateTouchedNormalBounds(final int slot) {
      validateDocumentLinks(slot);
      final long first = first(slot);
      final long last = last(slot);
      if (!validateNormalRange(first, last)) {
        return;
      }

      final int owner = ownerBase(slot);
      final long ownerLowerExclusive = owner == 1
          ? Long.MIN_VALUE
          : baseUpper(owner - 1);
      final long ownerUpperInclusive = baseUpper(owner);
      if (first <= ownerLowerExclusive || last > ownerUpperInclusive) {
        throw new IllegalStateException("projection normal fence [" + first + ", " + last + "] escapes base " + owner
            + " ownership (" + ownerLowerExclusive + ", " + ownerUpperInclusive + "] at physical leaf " + slot);
      }

      if (slot == owner) {
        final int successor = checkedNumericSuccessor(owner, 0, owner);
        if (successor != 0 && last >= first(successor)) {
          throw new IllegalStateException(
              "projection normal base " + owner + " overlaps its first numeric split " + successor);
        }
        return;
      }

      final int[] predecessors = predecessorsFor(owner, first);
      final int predecessor = predecessors[0];
      if (checkedNumericSuccessor(predecessor, 0, owner) != slot) {
        throw new IllegalStateException(
            "projection normal split " + slot + " is not linked at its numeric position for base " + owner);
      }
      // checkedNumericSuccessor proves strict key progression on both adjacent level-zero edges.
      checkedNumericSuccessor(slot, 0, owner);
    }

    public void flush(final int rowGroupCount) {
      if (rowGroupCount != liveRowGroupCount) {
        throw new IllegalArgumentException(
            "live rowGroupCount mismatch: " + rowGroupCount + " != " + liveRowGroupCount);
      }
      final int[] changedChunkIds = changedChunks.toIntArray();
      Arrays.sort(changedChunkIds);
      for (final int chunkId : changedChunkIds) {
        final int start = chunkId * CHUNK_LEAVES;
        if (start >= currentPhysicalRowGroupCount) {
          storage.tombstoneBlob(CHUNK_SLOT_BASE + chunkId);
          chunksWritten++;
          continue;
        }
        final int entries = Math.min(CHUNK_LEAVES, currentPhysicalRowGroupCount - start);
        final byte[] bytes = Arrays.copyOf(chunks.get(chunkId), entries * ENTRY_BYTES);
        storage.putBlob(CHUNK_SLOT_BASE + chunkId, bytes);
        chunksWritten++;
        bytesWritten += bytes.length;
      }
      for (int chunkId =
          chunkCount(currentPhysicalRowGroupCount); chunkId < chunkCount(priorPhysicalRowGroupCount); chunkId++) {
        storage.tombstoneBlob(CHUNK_SLOT_BASE + chunkId);
        chunksWritten++;
      }
      if (baseRowGroupCount != priorBaseRowGroupCount || currentPhysicalRowGroupCount != priorPhysicalRowGroupCount
          || liveRowGroupCount != priorLiveRowGroupCount || freeHead != priorFreeHead
          || documentHead != priorDocumentHead || documentTail != priorDocumentTail
          || !Arrays.equals(documentSkipTails, priorDocumentSkipTails)) {
        storage.putBlob(ORDER_HEADER_SLOT, orderHeader(baseRowGroupCount, currentPhysicalRowGroupCount,
            liveRowGroupCount, freeHead, documentHead, documentTail, documentSkipTails));
      }
    }

    int chunksRead() {
      return chunksRead;
    }

    int chunksWritten() {
      return chunksWritten;
    }

    long bytesRead() {
      return bytesRead;
    }

    long bytesWritten() {
      return bytesWritten;
    }

    private boolean containsNormal(final int slot, final long recordKey) {
      return hasNormal(slot) && first(slot) <= recordKey && recordKey <= last(slot);
    }

    private boolean hasNormal(final int slot) {
      return hasNormalRange(first(slot), last(slot));
    }

    private long baseUpper(final int base) {
      return longValue(base, BASE_UPPER_OFFSET);
    }

    private int numericSkip(final int slot, final int level) {
      if (level < 0 || level >= SKIP_LEVELS) {
        throw new IllegalArgumentException("projection numeric skip level out of range: " + level);
      }
      return intValue(slot, NUMERIC_SKIP_OFFSET + level * Integer.BYTES);
    }

    private int compareFirstOrderLabel(final int slot, final byte[] orderLabel,
        final ProjectionPersistedRecordLookup lookup) {
      validateLiveOwner(slot);
      final ProjectionIndexColumnSegmentCodec.KeysView keys = lookup.keys(slot).view();
      if (keys.recordKeys().length == 0) {
        throw new IllegalStateException("projection document skip points to empty leaf " + slot);
      }
      return keys.compareOrderLabelAt(0, orderLabel);
    }

    private int documentBack(final int slot, final int level) {
      if (level < 0 || level >= SKIP_LEVELS) {
        throw new IllegalArgumentException("projection document skip level out of range: " + level);
      }
      return intValue(slot, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES);
    }

    private int checkedDocumentBack(final int slot, final int level) {
      final int candidate = documentBack(slot, level);
      if (candidate == 0) {
        return 0;
      }
      if (candidate == slot || !isLivePhysicalSlot(candidate)) {
        throw new IllegalStateException(
            "malformed projection document skip edge " + slot + " -> " + candidate + " at level " + level);
      }
      validateLiveOwner(candidate);
      return candidate;
    }

    private void linkDocumentSkip(final int predecessorSlot, final int newSlot, final DocumentPosition position) {
      if (position.predecessors()[0] != predecessorSlot) {
        throw new IllegalStateException("projection document skip position does not follow leaf " + predecessorSlot);
      }
      final int height = documentSkipHeight(newSlot);
      for (int level = 0; level < SKIP_LEVELS; level++) {
        final int predecessor = position.predecessors()[level];
        setInt(newSlot, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES, predecessor);
        if (level >= height) {
          continue;
        }
        final int successor = position.successors()[level];
        if (successor == 0) {
          if (documentSkipTails[level] != predecessor) {
            throw new IllegalStateException("projection document skip tail drift at level " + level);
          }
          documentSkipTails[level] = newSlot;
        } else {
          if (checkedDocumentBack(successor, level) != predecessor) {
            throw new IllegalStateException("projection document skip splice is not adjacent at level " + level);
          }
          setInt(successor, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES, newSlot);
        }
      }
    }

    private void unlinkDocumentSkip(final int slot, final DocumentPosition positionAfterSlot) {
      final int height = documentSkipHeight(slot);
      for (int level = 0; level < height; level++) {
        if (positionAfterSlot.predecessors()[level] != slot) {
          throw new IllegalStateException("projection document skip removal is not adjacent at level " + level);
        }
        final int predecessor = checkedDocumentBack(slot, level);
        final int successor = positionAfterSlot.successors()[level];
        if (successor == 0) {
          if (documentSkipTails[level] != slot) {
            throw new IllegalStateException("projection document skip tail does not name removed leaf " + slot);
          }
          documentSkipTails[level] = predecessor;
        } else {
          if (checkedDocumentBack(successor, level) != slot) {
            throw new IllegalStateException("projection document skip successor does not name removed leaf " + slot);
          }
          setInt(successor, DOCUMENT_BACK_SKIP_OFFSET + level * Integer.BYTES, predecessor);
        }
      }
    }

    private void insertNumeric(final int slot) {
      if (slot <= baseRowGroupCount || !hasNormal(slot)) {
        return;
      }
      final int base = ownerBase(slot);
      final int[] predecessors = predecessorsFor(base, first(slot));
      final int successor = checkedNumericSuccessor(predecessors[0], 0, base);
      if ((predecessors[0] == base && hasNormal(base) && last(base) >= first(slot))
          || (predecessors[0] != base && last(predecessors[0]) >= first(slot))
          || (successor != 0 && first(successor) <= last(slot))) {
        throw new IllegalStateException("overlapping projection normal fences while linking leaf " + slot);
      }
      final int height = skipHeight(slot);
      for (int level = 0; level < height; level++) {
        final int predecessor = predecessors[level];
        setInt(slot, NUMERIC_SKIP_OFFSET + level * Integer.BYTES, checkedNumericSuccessor(predecessor, level, base));
        setInt(predecessor, NUMERIC_SKIP_OFFSET + level * Integer.BYTES, slot);
      }
    }

    private void unlinkNumeric(final int slot) {
      final int base = ownerBase(slot);
      final int[] predecessors = predecessorsFor(base, first(slot));
      if (checkedNumericSuccessor(predecessors[0], 0, base) != slot) {
        throw new IllegalStateException("projection normal leaf is not linked from its base: " + slot);
      }
      for (int level = 0; level < SKIP_LEVELS; level++) {
        final int predecessor = predecessors[level];
        if (checkedNumericSuccessor(predecessor, level, base) == slot) {
          setInt(predecessor, NUMERIC_SKIP_OFFSET + level * Integer.BYTES, checkedNumericSuccessor(slot, level, base));
        }
        setInt(slot, NUMERIC_SKIP_OFFSET + level * Integer.BYTES, 0);
      }
    }

    private int[] predecessorsFor(final int base, final long firstRecordKey) {
      int slot = base;
      for (int level = SKIP_LEVELS - 1; level >= 0; level--) {
        int candidate;
        while ((candidate = checkedNumericSuccessor(slot, level, base)) != 0 && last(candidate) < firstRecordKey) {
          slot = candidate;
        }
        numericPredecessors[level] = slot;
      }
      return numericPredecessors;
    }

    /**
     * Read one numeric skip edge and prove that it advances strictly inside the same base-owned normal
     * chain. The strict key progression makes a cycle impossible, so corrupt self/back links fail
     * immediately instead of hanging a commit-time lookup.
     */
    private int checkedNumericSuccessor(final int slot, final int level, final int base) {
      final int candidate = numericSkip(slot, level);
      if (candidate == 0) {
        return 0;
      }
      if (base < 1 || base > baseRowGroupCount || candidate <= baseRowGroupCount
          || candidate > currentPhysicalRowGroupCount || candidate == slot || ownerBase(candidate) != base) {
        throw new IllegalStateException("malformed projection numeric skip edge " + slot + " -> " + candidate
            + " at level " + level + " for base " + base);
      }
      final long candidateFirst = first(candidate);
      final long candidateLast = last(candidate);
      if (!validateNormalRange(candidateFirst, candidateLast)) {
        throw new IllegalStateException("projection numeric skip points to exception-only leaf " + candidate);
      }
      if (slot == base) {
        if (hasNormal(slot) && last(slot) >= candidateFirst) {
          throw new IllegalStateException("projection numeric skip does not advance beyond base " + base);
        }
      } else if (ownerBase(slot) != base || !hasNormal(slot) || last(slot) >= candidateFirst) {
        throw new IllegalStateException(
            "projection numeric skip does not advance from leaf " + slot + " within base " + base);
      }
      return candidate;
    }

    private int freeNext(final int slot) {
      return intValue(slot, FREE_NEXT_OFFSET);
    }

    private void validateLiveOwner(final int slot) {
      if (!isLivePhysicalSlot(slot)) {
        throw new IllegalStateException("projection document link names non-live physical leaf " + slot);
      }
      final int owner = ownerBase(slot);
      if (owner < 1 || owner > baseRowGroupCount) {
        throw new IllegalStateException("projection document leaf " + slot + " has invalid base owner " + owner);
      }
    }

    private void clearEntry(final int slot) {
      final byte[] chunk = chunk(slot, true);
      final int offset = entryOffset(slot);
      Arrays.fill(chunk, offset, offset + ENTRY_BYTES, (byte) 0);
      RowGroupDescriptor.putLongLE(chunk, offset + FIRST_OFFSET, Long.MAX_VALUE);
      RowGroupDescriptor.putLongLE(chunk, offset + LAST_OFFSET, Long.MIN_VALUE);
      changedChunks.add(chunkId(slot));
    }

    private long longValue(final int slot, final int fieldOffset) {
      checkPhysicalSlot(slot);
      return ProjectionIndexRowGroupCodec.getLongLE(chunk(slot, false), entryOffset(slot) + fieldOffset);
    }

    private int intValue(final int slot, final int fieldOffset) {
      checkPhysicalSlot(slot);
      return ProjectionIndexRowGroupCodec.getIntLE(chunk(slot, false), entryOffset(slot) + fieldOffset);
    }

    private void setLong(final int slot, final int fieldOffset, final long value) {
      final byte[] chunk = chunk(slot, true);
      RowGroupDescriptor.putLongLE(chunk, entryOffset(slot) + fieldOffset, value);
      changedChunks.add(chunkId(slot));
    }

    private void setInt(final int slot, final int fieldOffset, final int value) {
      final byte[] chunk = chunk(slot, true);
      ProjectionIndexRowGroupCodec.putIntLEAt(chunk, entryOffset(slot) + fieldOffset, value);
      changedChunks.add(chunkId(slot));
    }

    private void checkPhysicalSlot(final int slot) {
      if (slot < 1 || slot > currentPhysicalRowGroupCount) {
        throw new IllegalArgumentException("projection physical slot outside current snapshot: " + slot);
      }
    }

    private byte[] chunk(final int slot, final boolean allowAppend) {
      final int chunkId = chunkId(slot);
      final byte[] cached = chunks.get(chunkId);
      final int neededEntries = allowAppend
          ? CHUNK_LEAVES
          : Math.min(CHUNK_LEAVES, priorPhysicalRowGroupCount - chunkId * CHUNK_LEAVES);
      if (cached != null) {
        if (cached.length >= neededEntries * ENTRY_BYTES) {
          return cached;
        }
        final byte[] expanded = Arrays.copyOf(cached, neededEntries * ENTRY_BYTES);
        chunks.put(chunkId, expanded);
        return expanded;
      }
      final byte[] stored = storage.getBlob(CHUNK_SLOT_BASE + chunkId);
      final int priorEntries = Math.max(0, Math.min(CHUNK_LEAVES, priorPhysicalRowGroupCount - chunkId * CHUNK_LEAVES));
      if (priorEntries > 0 && (stored == null || stored.length != priorEntries * ENTRY_BYTES)) {
        throw new IllegalStateException("missing or malformed projection fence chunk " + chunkId);
      }
      if (stored != null) {
        chunksRead++;
        bytesRead += stored.length;
      }
      final byte[] loaded = allowAppend
          ? Arrays.copyOf(stored == null
              ? new byte[0]
              : stored, neededEntries * ENTRY_BYTES)
          : stored;
      chunks.put(chunkId, loaded);
      return loaded;
    }

    private static int chunkId(final int slot) {
      return (slot - 1) / CHUNK_LEAVES;
    }

    private static int entryOffset(final int slot) {
      return ((slot - 1) % CHUNK_LEAVES) * ENTRY_BYTES;
    }

    private static int skipHeight(final int physicalSlot) {
      long mixed = physicalSlot * 0x9E3779B97F4A7C15L;
      mixed ^= mixed >>> 33;
      mixed *= 0xC2B2AE3D27D4EB4FL;
      mixed ^= mixed >>> 29;
      return Math.min(SKIP_LEVELS, Long.numberOfTrailingZeros(mixed | (1L << (SKIP_LEVELS - 1))) + 1);
    }
  }

  private static int documentSkipHeight(final int physicalSlot) {
    long mixed = physicalSlot * 0x9E3779B97F4A7C15L;
    mixed ^= mixed >>> 33;
    mixed *= 0xC2B2AE3D27D4EB4FL;
    mixed ^= mixed >>> 29;
    return Math.min(SKIP_LEVELS, Long.numberOfTrailingZeros(mixed | (1L << (SKIP_LEVELS - 1))) + 1);
  }
}
