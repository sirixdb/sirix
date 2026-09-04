/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageLayout;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * Build-side half of the global projection value dictionary: interns values during a build and
 * materialises the namespace at the end of it.
 *
 * <h2>Why interning is done in memory and not against the trie</h2>
 *
 * The build asks "what is this value's id" once per ROW — ten million times on a ten-million-row
 * corpus. Answering that from the persistent forward directory would cost a binary search per row,
 * which is not a slower build, it is an impossible one. So the mapping is held in memory for the
 * duration of the build and the persistent radix structures are produced from it in one pass at the
 * end.
 *
 * <p>
 * Nothing here allocates per row: {@link #intern} takes a byte range and compares it against the
 * chunked arena in place. One append generation admits at most
 * {@link #MAX_DISTINCT_ENTRIES_PER_APPEND} distinct values, keeps every geometrically grown backing
 * array at or below a 256 KiB payload, and stores value bytes in fixed 64 KiB chunks. An individual
 * UTF-8 value is likewise capped at {@link #MAX_VALUE_BYTES} bytes before copying or String
 * materialisation. AUTO receives a typed decline and abandons the optional projection; forced
 * global encoding fails the owning operation. {@link #release()} drops the transient generation.
 *
 * <h2>Structures written</h2>
 *
 * <ul>
 * <li>immutable reverse-id buckets reached through a persistent radix directory;</li>
 * <li>immutable hash buckets reached through a second persistent radix directory;</li>
 * <li>one {@link ValueDictionaryHeaderNode} saying how much of each exists.</li>
 * </ul>
 */
public final class GlobalValueDictionaryWriter implements GlobalValueDictionaryEncoder {

  enum AdmissionPolicy {
    /** Typed refusal: AUTO abandons the optional projection while the ingest continues. */
    DECLINE,
    /** Forced-global mode: refusing the requested encoding fails the owning operation. */
    FAIL_CLOSED
  }

  /**
   * Payload cap for any geometrically grown primitive/reference array. G1's minimum region is 1 MiB
   * and its humongous threshold is half a region; 256 KiB plus an array header stays well below the
   * minimum 512 KiB threshold.
   */
  private static final int MAX_SAFE_ARRAY_PAYLOAD_BYTES = 256 << 10;

  private static final int MAX_SAFE_LONG_OR_REFERENCE_ARRAY_LENGTH = MAX_SAFE_ARRAY_PAYLOAD_BYTES / Long.BYTES;

  /**
   * Largest append admitted by the current in-memory interner. The next id would double the half-full
   * hash table from 32,768 to 65,536 longs, creating a 512 KiB payload before its object header and
   * therefore crossing the minimum G1 humongous boundary.
   */
  public static final int MAX_DISTINCT_ENTRIES_PER_APPEND = MAX_SAFE_LONG_OR_REFERENCE_ARRAY_LENGTH / 2;

  /** Largest individual UTF-8 value admitted without a humongous materialisation. */
  public static final int MAX_VALUE_BYTES = ValueDictionaryEntryNode.MAX_VALUE_LENGTH;

  /** Largest chunk-directory length under a worst-case eight-byte reference. */
  private static final int MAX_ARENA_CHUNKS = MAX_SAFE_LONG_OR_REFERENCE_ARRAY_LENGTH;

  /**
   * Value bytes are retained in fixed-size chunks. A single geometrically-grown byte array is a
   * particularly bad fit for high-cardinality columns: growing a 1 GiB arena briefly needs the old 1
   * GiB array and a new 2 GiB array at the same time, and both are humongous GC objects. Fixed chunks
   * make every value-byte allocation bounded and make the preflight delta exact.
   */
  static final int ARENA_CHUNK_BYTES = 1 << 16;

  private static final int INITIAL_ARENA_CHUNK_CAPACITY = 16;

  /** Initial open-addressing capacity; a power of two, grown by doubling at half load. */
  private static final int INITIAL_TABLE_CAPACITY = 1 << 12;

  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

  /** Concatenated value bytes, split into non-humongous fixed-size chunks. */
  private byte[][] arenaChunks = new byte[INITIAL_ARENA_CHUNK_CAPACITY][];

  private int allocatedArenaChunks;

  private long arenaLength;

  /** Start of each value in the logical chunked arena, indexed by id. */
  private long[] offsets = new long[64];

  /** Length of each value in {@link #arena}, indexed by id. */
  private int[] lengths = new int[64];

  /** Value hash per id, kept so the directory can be built without rehashing the arena. */
  private long[] hashes = new long[64];

  /** Independent digest used to split full primary-hash buckets. */
  private long[] secondaryHashes = new long[64];

  /** Open-addressed slots: the value hash, meaningful only where {@link #tableIds} is non-zero. */
  private long[] tableHashes = new long[INITIAL_TABLE_CAPACITY];

  /** Open-addressed slots: the id, {@code 0} marking a free slot. */
  private int[] tableIds = new int[INITIAL_TABLE_CAPACITY];

  /** How many distinct values have been interned; the highest id in use. */
  private int entryCount;

  private int maxValueLength;

  private boolean released;

  /**
   * The column this dictionary belongs to, carried only so a breach can name it.
   */
  private final int column;

  /**
   * Hard ceiling on the largest simultaneously live build or flush allocation set, or
   * {@link Long#MAX_VALUE} for unbounded.
   *
   * <p>
   * The original implementation treated distinct-count scaling as affordable. That was false for
   * ClickBench URL/Referer/Title: at 100M rows the monolithic arena wanted more than a 16 GB heap and
   * doubled until the collector took every core. This component bound complements the mandatory
   * structural caps above: it accounts for simultaneous build/flush workspace and turns pressure into
   * an admission decision before planner object graphs or persistent output are allocated. The
   * projection planner assigns it a disjoint share of the build-wide aggregate when another resident
   * structure exists beside it.
   * </p>
   */
  private final long budgetBytes;

  private final AdmissionPolicy admissionPolicy;

  /**
   * No byte budget, for standalone callers and tests. The structural entry, value and array ceilings
   * remain mandatory; "unbounded" never means "may allocate a humongous array".
   *
   * <p>
   * PUBLIC deliberately: before the budget existed this class had only the implicit public no-arg
   * constructor, and narrowing it here would break every caller outside this package for no reason —
   * the bound is opt-in, and adding it must not change who may construct one.
   * </p>
   */
  public GlobalValueDictionaryWriter() {
    this(-1, Long.MAX_VALUE, AdmissionPolicy.DECLINE);
  }

  /**
   * What an EMPTY writer already retains: the initial arena plus the initial index and table arrays.
   *
   * <p>
   * A budget below this can never be satisfied — the structure is over it before a single value is
   * interned, so it would refuse its first {@link #intern} with an entry count of zero and leave the
   * column permanently unusable rather than merely un-promoted. Callers must therefore decline the
   * column instead of constructing one; see the election in {@code ProjectionIndexBuilder}. Derived
   * from the same initial sizes as the fields, so it cannot drift from them.
   * </p>
   */
  private static final long EMPTY_RETAINED_BYTES =
      (long) INITIAL_ARENA_CHUNK_CAPACITY * Long.BYTES + 64L * Integer.BYTES + 192L * Long.BYTES
          + (long) INITIAL_TABLE_CAPACITY * Long.BYTES + (long) INITIAL_TABLE_CAPACITY * Integer.BYTES;

  public static final long MINIMUM_BUDGET_BYTES =
      EMPTY_RETAINED_BYTES + KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY + 2L * PageLayout.INITIAL_PAGE_SIZE;

  /**
   * @param column the column this dictionary encodes, for the breach message
   * @param budgetBytes ceiling on simultaneously live dictionary bytes, at least
   *        {@link #MINIMUM_BUDGET_BYTES}; {@link Long#MAX_VALUE} disables the check
   */
  GlobalValueDictionaryWriter(final int column, final long budgetBytes) {
    this(column, budgetBytes, AdmissionPolicy.DECLINE);
  }

  GlobalValueDictionaryWriter(final int column, final long budgetBytes, final AdmissionPolicy admissionPolicy) {
    if (budgetBytes < MINIMUM_BUDGET_BYTES) {
      throw new IllegalArgumentException(
          "budgetBytes must be at least MINIMUM_BUDGET_BYTES (" + MINIMUM_BUDGET_BYTES + "), got " + budgetBytes
              + " — an empty dictionary already retains that much, so a smaller budget refuses its own first value."
              + " Decline the column instead of constructing a writer that cannot hold anything.");
    }
    this.column = column;
    this.budgetBytes = budgetBytes;
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
  }

  /**
   * Everything this dictionary holds on the heap: the value arena plus the six index and table
   * arrays. Counted from array LENGTHS, not from the data written into them, because the doubling is
   * what actually costs — a half-full arena of 4 GB is 4 GB.
   */
  public long retainedBytes() {
    return (long) arenaChunks.length * Long.BYTES + (long) allocatedArenaChunks * ARENA_CHUNK_BYTES
        + (long) offsets.length * Long.BYTES + (long) lengths.length * Integer.BYTES + (long) hashes.length * Long.BYTES
        + (long) secondaryHashes.length * Long.BYTES + (long) tableHashes.length * Long.BYTES
        + (long) tableIds.length * Integer.BYTES;
  }

  /** How many distinct values have been interned. */
  public int entryCount() {
    return entryCount;
  }

  /** Total bytes of distinct value data held, for the build's memory accounting. */
  public long valueBytes() {
    return arenaLength;
  }

  long budgetBytes() {
    return budgetBytes;
  }

  AdmissionPolicy admissionPolicy() {
    return admissionPolicy;
  }

  long hashAt(final int id) {
    if (id < 1 || id > entryCount)
      throw new IllegalArgumentException("invalid dictionary id");
    return hashes[id];
  }

  long secondaryHashAt(final int id) {
    if (id < 1 || id > entryCount)
      throw new IllegalArgumentException("invalid dictionary id");
    return secondaryHashes[id];
  }

  /**
   * Intern a value and return its id, minting one if the value is new.
   *
   * <p>
   * Ids count from 1 so that {@code 0} can mean "no id" everywhere downstream without a separate
   * presence check.
   *
   * @param src the buffer holding the value's UTF-8 bytes
   * @param off offset into {@code src}
   * @param len length in {@code src}
   * @return the value's id, {@code >= 1}
   */
  @Override
  public int intern(final byte[] src, final int off, final int len) {
    checkInternArguments(src, off, len);
    final long hash = GlobalValueDictionary.valueHash(src, off, len);
    final int slot = findSlot(hash, src, off, len);
    final int existingId = tableIds[slot];
    if (existingId != 0) {
      return existingId;
    }
    preflightStructuralNewEntry(len);
    return insertAt(slot, hash, GlobalValueDictionary.secondaryValueHash(src, off, len), src, off, len);
  }

  int findId(final byte[] src, final int off, final int len) {
    checkInternArguments(src, off, len);
    final long hash = GlobalValueDictionary.valueHash(src, off, len);
    return tableIds[findSlot(hash, src, off, len)];
  }

  @Override
  public int intern(final String value) {
    Objects.requireNonNull(value, "value must not be null");
    final int encodedLength = GlobalValueDictionaryEncoder.utf8LengthCapped(value, MAX_VALUE_BYTES);
    if (encodedLength > MAX_VALUE_BYTES) {
      refuseAdmission("UTF-8 value exceeds the safe V0 limit of " + MAX_VALUE_BYTES + " bytes");
    }
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    return intern(utf8, 0, utf8.length);
  }

  /**
   * Return the largest allocation the next {@link #intern(byte[], int, int)} call can make.
   *
   * <p>
   * The method is deliberately allocation-free. A transaction-wide memory ledger can reserve this
   * delta before it lets the writer mutate; a duplicate value returns zero. The delta counts complete
   * newly allocated arrays (not merely their retained-size difference), because an array copy keeps
   * the old array live until the copy has completed.
   */
  public long reservationBytesForIntern(final byte[] src, final int off, final int len) {
    checkInternArguments(src, off, len);
    final long hash = GlobalValueDictionary.valueHash(src, off, len);
    if (tableIds[findSlot(hash, src, off, len)] != 0) {
      return 0L;
    }
    preflightStructuralNewEntry(len);
    final int id = nextId();
    final long requiredArenaLength = Math.addExact(arenaLength, len);
    final int requiredChunks = requiredArenaChunks(requiredArenaLength);
    final int arenaChunkCapacity = requiredChunks > arenaChunks.length
        ? grownCapacity(arenaChunks.length, requiredChunks)
        : arenaChunks.length;
    final int idArrayCapacity = id >= offsets.length
        ? grownCapacity(offsets.length, Math.addExact((long) id, 1L))
        : offsets.length;
    final int tableCapacity = tableCapacityFor(id);
    return allocationBytes(requiredChunks, arenaChunkCapacity, idArrayCapacity, tableCapacity);
  }

  private void checkInternArguments(final byte[] src, final int off, final int len) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    Objects.checkFromIndexSize(off, len, src.length);
    if (len > MAX_VALUE_BYTES) {
      refuseAdmission("value length " + len + " exceeds the safe V0 limit of " + MAX_VALUE_BYTES + " bytes");
    }
  }

  private void preflightStructuralNewEntry(final int length) {
    final int id = nextId();
    if (id > MAX_DISTINCT_ENTRIES_PER_APPEND) {
      refuseAdmission(String.format(Locale.ROOT, "append entry %,d exceeds the safe per-append limit of %,d", id,
          MAX_DISTINCT_ENTRIES_PER_APPEND));
    }
    final long requiredArenaLength = Math.addExact(arenaLength, length);
    final int requiredChunks = requiredArenaChunks(requiredArenaLength);
    if (requiredChunks > MAX_ARENA_CHUNKS) {
      refuseAdmission(
          "chunk directory would need " + requiredChunks + " references, above its safe limit of " + MAX_ARENA_CHUNKS);
    }
  }

  private int findSlot(final long hash, final byte[] src, final int off, final int len) {
    final int mask = tableIds.length - 1;
    int slot = ((int) (hash ^ (hash >>> 32))) & mask;
    while (true) {
      final int id = tableIds[slot];
      if (id == 0 || tableHashes[slot] == hash && regionMatches(id, src, off, len)) {
        return slot;
      }
      slot = (slot + 1) & mask;
    }
  }

  private boolean regionMatches(final int id, final byte[] src, final int off, final int len) {
    if (lengths[id] != len) {
      return false;
    }
    long arenaOffset = offsets[id];
    int sourceOffset = off;
    int remaining = len;
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(arenaOffset / ARENA_CHUNK_BYTES);
      final int chunkOffset = (int) (arenaOffset & (ARENA_CHUNK_BYTES - 1));
      final int compared = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      if (!Arrays.equals(arenaChunks[chunkIndex], chunkOffset, chunkOffset + compared, src, sourceOffset,
          sourceOffset + compared)) {
        return false;
      }
      arenaOffset += compared;
      sourceOffset += compared;
      remaining -= compared;
    }
    return true;
  }

  /** Compare caller-owned bytes to an interned value without materialising the arena slice. */
  int compareCandidateUnsigned(final byte[] candidate, final int offset, final int length, final int id) {
    Objects.checkFromIndexSize(offset, length, candidate.length);
    if (id < 1 || id > entryCount) {
      throw new IllegalArgumentException("invalid dictionary id");
    }
    final int commonLength = Math.min(length, lengths[id]);
    long arenaOffset = offsets[id];
    for (int index = 0; index < commonLength; index++) {
      final int chunkIndex = Math.toIntExact(arenaOffset / ARENA_CHUNK_BYTES);
      final int chunkOffset = (int) (arenaOffset & (ARENA_CHUNK_BYTES - 1));
      final int comparison = Integer.compare(Byte.toUnsignedInt(candidate[offset + index]),
          Byte.toUnsignedInt(arenaChunks[chunkIndex][chunkOffset]));
      if (comparison != 0) {
        return comparison;
      }
      arenaOffset++;
    }
    return Integer.compare(length, lengths[id]);
  }

  private int insertAt(final int slot, final long hash, final long secondaryHash, final byte[] src, final int off,
      final int len) {
    final long requiredArenaLength = Math.addExact(arenaLength, len);
    final int requiredChunks = requiredArenaChunks(requiredArenaLength);
    final int id = nextId();
    if (id > MAX_DISTINCT_ENTRIES_PER_APPEND || requiredChunks > MAX_ARENA_CHUNKS) {
      throw new IllegalStateException("value dictionary structural preflight was bypassed");
    }
    final int idArrayCapacity = id >= offsets.length
        ? grownCapacity(offsets.length, Math.addExact((long) id, 1L))
        : offsets.length;
    final int arenaChunkCapacity = requiredChunks > arenaChunks.length
        ? grownCapacity(arenaChunks.length, requiredChunks)
        : arenaChunks.length;
    final int tableCapacity = tableCapacityFor(id);
    final long allocationBytes = allocationBytes(requiredChunks, arenaChunkCapacity, idArrayCapacity, tableCapacity);
    final long futureRetained = saturatedAdd(retainedBytes(),
        retainedGrowthBytes(requiredChunks, arenaChunkCapacity, idArrayCapacity, tableCapacity));
    final long flushPeak = flushPeakBytes(futureRetained, id, requiredArenaLength, Math.max(maxValueLength, len));
    final long retainedPlusPending = saturatedAdd(retainedBytes(), allocationBytes);
    if (budgetBytes != Long.MAX_VALUE && Math.max(retainedPlusPending, flushPeak) > budgetBytes) {
      // Report whichever of the two terms the max() picked, so the notice quotes the number that
      // actually breached rather than a smaller one the reader cannot reconcile with the budget.
      if (retainedPlusPending >= flushPeak) {
        refuseBudget(retainedPlusPending, "retained+pending");
      } else {
        refuseBudget(flushPeak, "flush-peak");
      }
    }

    final long[] nextOffsets = idArrayCapacity == offsets.length
        ? offsets
        : Arrays.copyOf(offsets, idArrayCapacity);
    final int[] nextLengths = idArrayCapacity == lengths.length
        ? lengths
        : Arrays.copyOf(lengths, idArrayCapacity);
    final long[] nextHashes = idArrayCapacity == hashes.length
        ? hashes
        : Arrays.copyOf(hashes, idArrayCapacity);
    final long[] nextSecondaryHashes = idArrayCapacity == secondaryHashes.length
        ? secondaryHashes
        : Arrays.copyOf(secondaryHashes, idArrayCapacity);
    final byte[][] nextArenaChunks = arenaChunkCapacity == arenaChunks.length
        ? arenaChunks
        : Arrays.copyOf(arenaChunks, arenaChunkCapacity);
    for (int chunk = allocatedArenaChunks; chunk < requiredChunks; chunk++) {
      nextArenaChunks[chunk] = new byte[ARENA_CHUNK_BYTES];
    }
    final long[] nextTableHashes;
    final int[] nextTableIds;
    final int targetSlot;
    if (tableCapacity != tableIds.length) {
      nextTableHashes = new long[tableCapacity];
      nextTableIds = new int[tableCapacity];
      rehashInto(nextTableHashes, nextTableIds);
      targetSlot = findEmptySlot(nextTableHashes, nextTableIds, hash);
    } else {
      nextTableHashes = tableHashes;
      nextTableIds = tableIds;
      targetSlot = slot;
    }

    copyIntoArena(nextArenaChunks, arenaLength, src, off, len);
    nextOffsets[id] = arenaLength;
    nextLengths[id] = len;
    nextHashes[id] = hash;
    nextSecondaryHashes[id] = secondaryHash;
    nextTableHashes[targetSlot] = hash;
    nextTableIds[targetSlot] = id;

    offsets = nextOffsets;
    lengths = nextLengths;
    hashes = nextHashes;
    secondaryHashes = nextSecondaryHashes;
    arenaChunks = nextArenaChunks;
    allocatedArenaChunks = requiredChunks;
    tableHashes = nextTableHashes;
    tableIds = nextTableIds;
    arenaLength = requiredArenaLength;
    entryCount = id;
    maxValueLength = Math.max(maxValueLength, len);
    return id;
  }

  private int nextId() {
    if (entryCount == Integer.MAX_VALUE) {
      throw new IllegalStateException("value dictionary entry capacity exhausted");
    }
    return entryCount + 1;
  }

  private int tableCapacityFor(final int id) {
    // Filling the last slot at the 50% load boundary is safe. Grow only for the following id so
    // the canonical 16,384-mutation maintenance chunk fits its existing 32,768-slot table, while
    // structural admission rejects id 16,385 before this method can request a humongous array.
    if (id <= (tableIds.length >>> 1)) {
      return tableIds.length;
    }
    if (tableIds.length >= (1 << 30)) {
      throw new IllegalStateException("value dictionary hash table capacity exhausted");
    }
    return tableIds.length << 1;
  }

  private static int requiredArenaChunks(final long requiredArenaLength) {
    if (requiredArenaLength < 0) {
      throw new IllegalStateException("value dictionary byte count overflow");
    }
    final long chunks = requiredArenaLength == 0
        ? 0L
        : 1L + (requiredArenaLength - 1L) / ARENA_CHUNK_BYTES;
    if (chunks > MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("value dictionary chunk index capacity exhausted");
    }
    return (int) chunks;
  }

  private long allocationBytes(final int requiredChunks, final int arenaChunkCapacity, final int idArrayCapacity,
      final int tableCapacity) {
    long bytes = Math.multiplyExact((long) requiredChunks - allocatedArenaChunks, ARENA_CHUNK_BYTES);
    if (arenaChunkCapacity != arenaChunks.length) {
      bytes = saturatedAdd(bytes, (long) arenaChunkCapacity * Long.BYTES);
    }
    if (idArrayCapacity != offsets.length) {
      bytes = saturatedAdd(bytes, (long) idArrayCapacity * (Integer.BYTES + 3L * Long.BYTES));
    }
    if (tableCapacity != tableIds.length) {
      bytes = saturatedAdd(bytes, (long) tableCapacity * (Long.BYTES + Integer.BYTES));
    }
    return bytes;
  }

  private long retainedGrowthBytes(final int requiredChunks, final int arenaChunkCapacity, final int idArrayCapacity,
      final int tableCapacity) {
    long bytes = Math.multiplyExact((long) requiredChunks - allocatedArenaChunks, ARENA_CHUNK_BYTES);
    bytes = saturatedAdd(bytes, (long) (arenaChunkCapacity - arenaChunks.length) * Long.BYTES);
    bytes = saturatedAdd(bytes, (long) (idArrayCapacity - offsets.length) * (Integer.BYTES + 3L * Long.BYTES));
    return saturatedAdd(bytes, (long) (tableCapacity - tableIds.length) * (Long.BYTES + Integer.BYTES));
  }

  private static void copyIntoArena(final byte[][] destination, long destinationOffset, final byte[] source,
      int sourceOffset, int remaining) {
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(destinationOffset / ARENA_CHUNK_BYTES);
      final int chunkOffset = (int) (destinationOffset & (ARENA_CHUNK_BYTES - 1));
      final int copied = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      System.arraycopy(source, sourceOffset, destination[chunkIndex], chunkOffset, copied);
      destinationOffset += copied;
      sourceOffset += copied;
      remaining -= copied;
    }
  }

  private static long flushPeakBytes(final long retained, final int entries, final long valueBytes,
      final int largestValueBytes) {
    return saturatedAdd(retained,
        GlobalValueDictionaryRadix.reservationBytesForAppend(0, entries, valueBytes, largestValueBytes));
  }

  private static long saturatedAdd(final long left, final long right) {
    return left > Long.MAX_VALUE - right
        ? Long.MAX_VALUE
        : left + right;
  }

  private static int grownCapacity(final int current, final long required) {
    if (required > MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("value dictionary array capacity exhausted");
    }
    final long doubled = (long) current << 1;
    return (int) Math.min(MAX_ARRAY_LENGTH, Math.max(required, doubled));
  }

  static int grownCapacityForTest(final int current, final long required) {
    return grownCapacity(current, required);
  }

  int hashTableCapacityForTest() {
    return tableIds.length;
  }

  int largestBackingArrayPayloadBytesForTest() {
    int largest = ARENA_CHUNK_BYTES;
    largest = Math.max(largest, Math.multiplyExact(arenaChunks.length, Long.BYTES));
    largest = Math.max(largest, Math.multiplyExact(offsets.length, Long.BYTES));
    largest = Math.max(largest, Math.multiplyExact(lengths.length, Integer.BYTES));
    largest = Math.max(largest, Math.multiplyExact(hashes.length, Long.BYTES));
    largest = Math.max(largest, Math.multiplyExact(secondaryHashes.length, Long.BYTES));
    largest = Math.max(largest, Math.multiplyExact(tableHashes.length, Long.BYTES));
    return Math.max(largest, Math.multiplyExact(tableIds.length, Integer.BYTES));
  }

  private void rehashInto(final long[] newHashes, final int[] newIds) {
    final int mask = newIds.length - 1;
    for (int i = 0; i < tableIds.length; i++) {
      final int id = tableIds[i];
      if (id == 0) {
        continue;
      }
      final long hash = tableHashes[i];
      int slot = ((int) (hash ^ (hash >>> 32))) & mask;
      while (newIds[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      newHashes[slot] = hash;
      newIds[slot] = id;
    }
  }

  private static int findEmptySlot(final long[] hashes, final int[] ids, final long hash) {
    final int mask = ids.length - 1;
    int slot = ((int) (hash ^ (hash >>> 32))) & mask;
    while (ids[slot] != 0) {
      slot = (slot + 1) & mask;
    }
    return slot;
  }

  /**
   * The bytes behind an id, copied out. Build-time only; the query path reads the persisted record
   * instead.
   *
   * @param id the value id
   * @return a fresh copy of the value's UTF-8 bytes
   */
  public byte[] valueBytes(final int id) {
    final byte[] value = new byte[valueLengthAt(id)];
    copyFromArena(offsets[id], value, 0, value.length);
    return value;
  }

  int valueLengthAt(final int id) {
    checkValueId(id);
    return lengths[id];
  }

  /** Copy a bounded slice of one value without materialising the whole value. */
  void copyValueRegion(final int id, final int valueOffset, final byte[] destination, final int destinationOffset,
      final int length) {
    checkValueId(id);
    Objects.requireNonNull(destination, "destination must not be null");
    Objects.checkFromIndexSize(valueOffset, length, lengths[id]);
    Objects.checkFromIndexSize(destinationOffset, length, destination.length);
    copyFromArena(offsets[id] + valueOffset, destination, destinationOffset, length);
  }

  /** Seed a probe front by direct arena-to-arena transfer; the front retains its own byte copy. */
  void copyValueToProbeFront(final int id, final GlobalValueDictionaryProbeFront front) {
    checkValueId(id);
    Objects.requireNonNull(front, "front must not be null");
    front.putDistinctFromWriter(hashes[id], secondaryHashes[id], this, id, id);
  }

  private void checkValueId(final int id) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    if (id < 1 || id > entryCount) {
      throw new IllegalArgumentException("no such value dictionary id: " + id);
    }
  }

  private void copyFromArena(long sourceOffset, final byte[] destination, int destinationOffset, int remaining) {
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(sourceOffset / ARENA_CHUNK_BYTES);
      final int chunkOffset = (int) (sourceOffset & (ARENA_CHUNK_BYTES - 1));
      final int copied = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      System.arraycopy(arenaChunks[chunkIndex], chunkOffset, destination, destinationOffset, copied);
      sourceOffset += copied;
      destinationOffset += copied;
      remaining -= copied;
    }
  }

  /**
   * Write the dictionary: every value entry, the forward directory over all of them, and the header
   * that ties the two together.
   *
   * <p>
   * Every key comes out of one contiguous run reserved up front, because that is the only key shape
   * the sub-trie's indirect-page traversal can address — see
   * {@link NamePage#reserveProjectionValueDictionaryKeys}. Records are written in ascending key order
   * so each record page is prepared in turn, which is what lets the trie grow its levels at the
   * boundaries it expects.
   *
   * <p>
   * The header is written last so a dictionary is never advertised as complete before the structures
   * it describes exist; a torn write leaves an older header, or none, which readers treat as "cannot
   * say" rather than as an empty dictionary.
   *
   * @param namePage the name page of the revision being built
   * @param databaseType the database type, which fixes the dictionary offset
   * @param storageEngineWriter the writer for the revision being built
   * @param log the transaction intent log of the revision being built
   * @return the header's node key, which is what a reader needs to find this dictionary again
   */
  /**
   * Set by the rank pass, which feeds a MERGED SORTED stream so {@code intern} mints {@code id ==
   * rank}. It changes two things and nothing else: the forward hash index is not built (§3.3.2), and
   * the header records the whole dictionary as ordered.
   */
  private boolean rankOrdered;

  /**
   * Declare that every value handed to this writer arrives in ascending collation order.
   *
   * <p>
   * Package-private: only the rank pass can make this claim, because only a merged sorted stream can
   * honour it. It is checked where it can be — the header refuses a missing forward index on a
   * dictionary that is not fully ordered — but the ORDER itself is the caller's obligation.
   * </p>
   */
  void markRankOrdered() {
    if (entryCount != 0) {
      throw new IllegalStateException("rank order must be declared before the first value is interned");
    }
    rankOrdered = true;
  }

  public long flush(final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    ensureFlushFitsBudget(reservationBytesForFlush());
    try {
      namePage.createProjectionValueDictionaryTree(databaseType, storageEngineWriter, log);

      final long headerKey = namePage.reserveProjectionValueDictionaryKeys(databaseType, 1L);
      final GlobalValueDictionaryRadix.Roots roots = GlobalValueDictionaryRadix.append(0L, 0L, 0, this, namePage,
          databaseType, storageEngineWriter, log, !rankOrdered);
      final ValueDictionaryHeaderNode header = new ValueDictionaryHeaderNode(headerKey,
          ValueDictionaryHeaderNode.VERSION, entryCount, roots.forward(), roots.reverse(), 0, rankOrdered
              ? entryCount
              : 0);
      namePage.putProjectionValueDictionaryRecord(header, databaseType, storageEngineWriter, log);
      return headerKey;
    } catch (final RuntimeException | Error failure) {
      poisonOwningTransaction(storageEngineWriter, failure);
      throw failure;
    }
  }

  /**
   * Append this writer's values as one immutable segment and update only the stable base header.
   */
  public long flushAppend(final ValueDictionaryHeaderNode baseHeader, final NamePage namePage,
      final DatabaseType databaseType, final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    validateAppendHeader(baseHeader);
    if (entryCount == 0) {
      return baseHeader.getNodeKey();
    }
    final int totalEntries = Math.toIntExact(Math.addExact((long) baseHeader.getEntryCount(), entryCount));
    ensureFlushFitsBudget(reservationBytesForFlushAppend(baseHeader));
    try {
      // The result is ordered only if BOTH the base and this generation are: a rank pass appending to
      // an intern-ordered base would produce a sorted run above an unsorted one, which is exactly the
      // state orderedPrefixCount exists to describe rather than to claim away.
      final boolean ordered = rankOrdered && baseHeader.isFullyOrdered();
      final GlobalValueDictionaryRadix.Roots roots =
          GlobalValueDictionaryRadix.append(baseHeader.getForwardRootKey(), baseHeader.getReverseRootKey(),
              baseHeader.getEntryCount(), this, namePage, databaseType, storageEngineWriter, log, !ordered);
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryHeaderNode(baseHeader.getNodeKey(), ValueDictionaryHeaderNode.VERSION, totalEntries,
              roots.forward(), roots.reverse(), Math.addExact(baseHeader.getGeneration(), 1), ordered
                  ? totalEntries
                  : baseHeader.getOrderedPrefixCount()),
          databaseType, storageEngineWriter, log);
      return baseHeader.getNodeKey();
    } catch (final RuntimeException | Error failure) {
      poisonOwningTransaction(storageEngineWriter, failure);
      throw failure;
    }
  }

  /** Preserve the dictionary failure as the transaction's authoritative rollback cause. */
  private static void poisonOwningTransaction(final StorageEngineWriter storageEngineWriter,
      final Throwable primaryFailure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(primaryFailure);
    } catch (final RuntimeException | Error poisonFailure) {
      if (poisonFailure == primaryFailure) {
        return;
      }
      try {
        primaryFailure.addSuppressed(poisonFailure);
      } catch (final RuntimeException | Error ignored) {
        // Preserve the authoritative dictionary failure even when poisoning runs under VM pressure.
      }
    }
  }

  /**
   * Allocation-free reservation for a new dictionary flush. Persistent node-key reservation is
   * intentionally not part of this number; keys are allocated exactly by the radix writer.
   */
  public long reservationBytesForFlush() {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    return GlobalValueDictionaryRadix.reservationBytesForAppend(0, entryCount, arenaLength, maxValueLength);
  }

  /**
   * Allocation-free reservation for appending this writer to {@code baseHeader}.
   */
  public long reservationBytesForFlushAppend(final ValueDictionaryHeaderNode baseHeader) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    validateAppendHeader(baseHeader);
    Math.toIntExact(Math.addExact((long) baseHeader.getEntryCount(), entryCount));
    return GlobalValueDictionaryRadix.reservationBytesForAppend(baseHeader.getEntryCount(), entryCount, arenaLength,
        maxValueLength);
  }

  private static void validateAppendHeader(final ValueDictionaryHeaderNode baseHeader) {
    if (baseHeader == null || baseHeader.getVersion() != ValueDictionaryHeaderNode.VERSION
        || !baseHeader.isDirectoryComplete()) {
      throw new IllegalArgumentException("a complete current value dictionary header is required");
    }
  }

  private void ensureFlushFitsBudget(final long reservationBytes) {
    final long retainedPlusReservation = saturatedAdd(retainedBytes(), reservationBytes);
    if (budgetBytes != Long.MAX_VALUE && retainedPlusReservation > budgetBytes) {
      refuseBudget(retainedPlusReservation, "retained+flush-reservation");
    }
  }

  long estimatedFlushPeakBytes() {
    return saturatedAdd(retainedBytes(), reservationBytesForFlush());
  }

  void ensureAppendWorkspaceFitsBudget(final long workspaceBytes) {
    if (workspaceBytes < 0) {
      throw new IllegalArgumentException("workspaceBytes must be non-negative");
    }
    final long retainedPlusWorkspace = saturatedAdd(retainedBytes(), workspaceBytes);
    if (budgetBytes != Long.MAX_VALUE && retainedPlusWorkspace > budgetBytes) {
      refuseBudget(retainedPlusWorkspace, "retained+append-workspace");
    }
  }

  /**
   * Refuse a STRUCTURAL admission — a ceiling that is not weighed in bytes against the budget, so the
   * decline must not claim any byte quantity exceeded it.
   */
  private void refuseAdmission(final String detail) {
    throw policyDecline(GlobalDictionaryBudgetExceededException.structuralDecline(column, retainedBytes(), budgetBytes,
        entryCount, detail), detail);
  }

  /**
   * Refuse a BYTE-BUDGET admission, carrying the term the guard actually compared against the budget.
   *
   * <p>
   * Every guard here weighs retention plus a reservation, never retention alone, so the decline has
   * to carry that sum: quoting {@link #retainedBytes()} instead would report a figure BELOW the
   * budget it announces as breached — the guard's own arithmetic, contradicted by its own message.
   * </p>
   *
   * @param breachingBytes the quantity that exceeded {@link #budgetBytes}
   * @param breachingTerm what that quantity is, for a message that explains itself
   */
  private void refuseBudget(final long breachingBytes, final String breachingTerm) {
    throw policyDecline(GlobalDictionaryBudgetExceededException.budgetBreach(column, retainedBytes(), breachingBytes,
        breachingTerm, budgetBytes, entryCount, null), null);
  }

  private RuntimeException policyDecline(final GlobalDictionaryBudgetExceededException decline, final String detail) {
    if (admissionPolicy == AdmissionPolicy.FAIL_CLOSED) {
      return new IllegalStateException("Forced global value dictionary for column " + column
          + " cannot continue safely: " + (detail == null
              ? "configured dictionary-component budget exhausted"
              : detail),
          decline);
    }
    return decline;
  }

  /**
   * The same policy-correct refusal {@link #refuseAdmission} raises, for encoders that reject a value
   * BEFORE any generation writer exists to raise it themselves.
   *
   * <p>
   * The V0 value-length ceiling is an admission decision like every other bound here, so it has to
   * honour the same {@link AdmissionPolicy}: an AUTO build receives the typed decline and abandons
   * its optional projection while the ingest completes, and only a forced dictionary fails the owning
   * operation. Raising a bare {@code IllegalStateException} instead would kill a legal load over an
   * optional index.
   * </p>
   *
   * @param column the column whose dictionary refused the value
   * @param length the refused UTF-8 length, in bytes
   * @param retainedBytes bytes the dictionary retains at the point of refusal
   * @param budgetBytes the configured writer/component budget
   * @param entryCount distinct values admitted before the refusal
   * @param admissionPolicy the refusing dictionary's policy
   * @return the exception to throw; never {@code null}
   */
  static RuntimeException oversizedValueRefusal(final int column, final int length, final long retainedBytes,
      final long budgetBytes, final int entryCount, final AdmissionPolicy admissionPolicy) {
    final String detail = "value length " + length + " exceeds the safe V0 limit of " + MAX_VALUE_BYTES + " bytes";
    final GlobalDictionaryBudgetExceededException decline = GlobalDictionaryBudgetExceededException.structuralDecline(
        column, retainedBytes, budgetBytes, entryCount, detail);
    if (admissionPolicy == AdmissionPolicy.FAIL_CLOSED) {
      return new IllegalStateException(
          "Forced global value dictionary for column " + column + " cannot continue safely: " + detail, decline);
    }
    return decline;
  }

  long logicalPersistedBytes() {
    return saturatedAdd(arenaLength, (long) entryCount * 64L);
  }

  /** Drop the in-memory arena and index. The writer cannot be used afterwards. */
  public void release() {
    released = true;
    arenaChunks = null;
    allocatedArenaChunks = 0;
    offsets = null;
    lengths = null;
    hashes = null;
    secondaryHashes = null;
    tableHashes = null;
    tableIds = null;
  }
}
