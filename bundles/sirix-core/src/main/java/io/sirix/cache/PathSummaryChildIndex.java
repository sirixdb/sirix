package io.sirix.cache;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import net.openhft.hashing.LongHashFunction;

/**
 * Open-addressed map from a path summary's {@code (parentPathNodeKey, childName, childKind)} triple
 * to the child's path node key, replacing the {@code Long2LongOpenHashMap} that was keyed on a
 * LOSSY pack of that triple.
 *
 * <p>The old key was {@code (parentNodeKey << 32) | ((name.hashCode() & 0xFFFFFF) << 8) | kind} and
 * the value it found was returned as the authoritative child with no verification, so two sibling
 * names colliding in those 24 bits merged into a single path node. {@code "Aa"} and {@code "BB"}
 * both hash to 2112: on {@code {"Aa":1,"BB":2}} the summary held ONE node (name {@code Aa},
 * references 2), {@code PathSummaryReader.match("BB")} was empty, and every {@code BB} record was
 * filed under {@code Aa}'s path class. A hash-keyed map cannot fix that by hashing harder — the
 * table has to hold the real key.
 *
 * <p>So it does: each slot stores the full triple, and a probe compares it. Both directions are
 * then exact — a hit is the child that was actually inserted, and a miss really means the parent has
 * no such child, which a lossy key could not promise either (removing one of two colliding names
 * dropped the shared entry and orphaned the survivor).
 *
 * <p>Layout and probing are chosen for the lookup, which runs once per named record shredded:
 * <ul>
 * <li>parallel primitive arrays plus one reference array, so a probe touches contiguous memory and
 * boxes nothing;</li>
 * <li>linear probing with a power-of-two capacity — the slot is a mask, not a modulo;</li>
 * <li>the mixed 64-bit hash is stored per slot and compared FIRST, so {@code QNm.equals} runs only
 * on the slot that already matched everything else;</li>
 * <li>backward-shift deletion, so there are no tombstones to degrade later probes;</li>
 * <li>load factor 0.5 — path summaries are small (one node per distinct path, not per record), so
 * short probe chains are worth far more than the halved footprint.</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> none, exactly like the map it replaces. It is owned by a single
 * {@code PathSummaryReader} and by the {@link PathSummaryData} snapshot that reader shares.
 */
public final class PathSummaryChildIndex {

  /** Returned by {@link #get} when the parent has no such child. */
  public static final long NO_VALUE = -1L;

  /**
   * 64-bit hash of a name. {@code hashChars} reads the {@code String}'s characters in place, so no
   * intermediate {@code char[]} or {@code byte[]} is materialized per lookup.
   */
  private static final LongHashFunction NAME_HASH = LongHashFunction.xx3();

  /** Odd 64-bit multiplier (the golden-ratio constant) used to fold the components together. */
  private static final long MIX = 0x9E3779B97F4A7C15L;

  /** Smallest table capacity; keeps the mask arithmetic valid for an empty index. */
  private static final int MIN_CAPACITY = 8;

  /** Slots occupied before growing, as a fraction of capacity. */
  private static final float LOAD_FACTOR = 0.5f;

  private long[] hashes;
  private long[] parentKeys;
  private QNm[] names;
  private byte[] kinds;
  private long[] values;

  private int mask;
  private int size;
  private int threshold;

  /**
   * @param expectedEntries how many entries the index should hold without growing; non-positive is
   *        treated as "unknown" and yields the minimum capacity
   */
  public PathSummaryChildIndex(final int expectedEntries) {
    allocate(capacityFor(expectedEntries));
  }

  /** Copy constructor — the copy shares no state with the original. */
  public PathSummaryChildIndex(final PathSummaryChildIndex other) {
    this.hashes = other.hashes.clone();
    this.parentKeys = other.parentKeys.clone();
    this.names = other.names.clone();
    this.kinds = other.kinds.clone();
    this.values = other.values.clone();
    this.mask = other.mask;
    this.size = other.size;
    this.threshold = other.threshold;
  }

  /**
   * Look up the path node key of {@code parentNodeKey}'s child named {@code childName} of kind
   * {@code childKind}.
   *
   * @param parentNodeKey the parent path node key
   * @param childName the child name, never {@code null}
   * @param childKind the child node kind, never {@code null}
   * @return the child's path node key, or {@link #NO_VALUE} if the parent has no such child
   */
  public long get(final long parentNodeKey, final QNm childName, final NodeKind childKind) {
    final long hash = hash(parentNodeKey, childName, childKind);
    final byte kindOrdinal = (byte) childKind.ordinal();
    final QNm[] slotNames = names;
    int slot = (int) hash & mask;
    while (true) {
      final QNm slotName = slotNames[slot];
      if (slotName == null) {
        return NO_VALUE; // An empty slot ends the probe chain: the key is not present.
      }
      if (hashes[slot] == hash && parentKeys[slot] == parentNodeKey && kinds[slot] == kindOrdinal
          && childName.equals(slotName)) {
        return values[slot];
      }
      slot = (slot + 1) & mask;
    }
  }

  /**
   * Record (or overwrite) the child path node key for a triple.
   *
   * @param parentNodeKey the parent path node key
   * @param childName the child name, never {@code null}
   * @param childKind the child node kind, never {@code null}
   * @param childNodeKey the child's path node key
   */
  public void put(final long parentNodeKey, final QNm childName, final NodeKind childKind,
      final long childNodeKey) {
    final long hash = hash(parentNodeKey, childName, childKind);
    final byte kindOrdinal = (byte) childKind.ordinal();
    int slot = (int) hash & mask;
    while (true) {
      final QNm slotName = names[slot];
      if (slotName == null) {
        break;
      }
      if (hashes[slot] == hash && parentKeys[slot] == parentNodeKey && kinds[slot] == kindOrdinal
          && childName.equals(slotName)) {
        values[slot] = childNodeKey; // Overwrite in place; size is unchanged.
        return;
      }
      slot = (slot + 1) & mask;
    }

    hashes[slot] = hash;
    parentKeys[slot] = parentNodeKey;
    names[slot] = childName;
    kinds[slot] = kindOrdinal;
    values[slot] = childNodeKey;
    if (++size > threshold) {
      grow();
    }
  }

  /**
   * Drop the entry for a triple, if present.
   *
   * @param parentNodeKey the parent path node key
   * @param childName the child name, never {@code null}
   * @param childKind the child node kind, never {@code null}
   */
  public void remove(final long parentNodeKey, final QNm childName, final NodeKind childKind) {
    final long hash = hash(parentNodeKey, childName, childKind);
    final byte kindOrdinal = (byte) childKind.ordinal();
    int slot = (int) hash & mask;
    while (true) {
      final QNm slotName = names[slot];
      if (slotName == null) {
        return;
      }
      if (hashes[slot] == hash && parentKeys[slot] == parentNodeKey && kinds[slot] == kindOrdinal
          && childName.equals(slotName)) {
        shiftKeys(slot);
        return;
      }
      slot = (slot + 1) & mask;
    }
  }

  /** @return how many triples the index currently holds */
  public int size() {
    return size;
  }

  /**
   * Mix a triple into the 64-bit value the table probes on.
   *
   * <p>Hashes exactly the fields {@code QNm.hashCode()} covers — nsURI and localName. The prefix is
   * deliberately excluded: were it included, two names that are EQUAL but spelled with different
   * prefixes would land in different chains and the second would read as absent. Slot identity may
   * be coarser than equality (that is just a collision, resolved by the stored key), never finer.
   */
  private static long hash(final long parentNodeKey, final QNm childName, final NodeKind childKind) {
    final String localName = childName.getLocalName();
    long hash = localName == null ? 0L : NAME_HASH.hashChars(localName);
    final String nsUri = childName.getNamespaceURI();
    if (nsUri != null && !nsUri.isEmpty()) {
      hash = (hash + NAME_HASH.hashChars(nsUri)) * MIX;
    }
    hash = (hash ^ parentNodeKey) * MIX;
    hash = (hash ^ childKind.ordinal()) * MIX;

    // fmix64 finalizer: the slot is the LOW bits of the hash, so the avalanche has to push the
    // high-order entropy down before the table ever sees it.
    hash ^= hash >>> 33;
    hash *= 0xFF51AFD7ED558CCDL;
    hash ^= hash >>> 33;
    return hash;
  }

  /**
   * Backward-shift deletion (the fastutil algorithm): close the hole by pulling forward any later
   * entry whose ideal slot is at or before it, so every probe chain stays unbroken and no tombstone
   * is left behind.
   */
  private void shiftKeys(int free) {
    while (true) {
      int slot = free;
      while (true) {
        slot = (slot + 1) & mask;
        final QNm slotName = names[slot];
        if (slotName == null) {
          names[free] = null;
          size--;
          return;
        }
        final int ideal = (int) hashes[slot] & mask;
        // Does `ideal` lie outside the (free, slot] window? Then the entry may move into `free`.
        if (free <= slot
            ? (ideal <= free || ideal > slot)
            : (ideal <= free && ideal > slot)) {
          break;
        }
      }
      hashes[free] = hashes[slot];
      parentKeys[free] = parentKeys[slot];
      names[free] = names[slot];
      kinds[free] = kinds[slot];
      values[free] = values[slot];
      free = slot;
    }
  }

  private void grow() {
    final long[] oldHashes = hashes;
    final long[] oldParentKeys = parentKeys;
    final QNm[] oldNames = names;
    final byte[] oldKinds = kinds;
    final long[] oldValues = values;
    final int oldCapacity = oldNames.length;

    allocate(oldCapacity << 1);
    size = 0;
    for (int i = 0; i < oldCapacity; i++) {
      if (oldNames[i] != null) {
        insertRehashed(oldHashes[i], oldParentKeys[i], oldNames[i], oldKinds[i], oldValues[i]);
      }
    }
  }

  /** Insert into a freshly sized table: the key is known to be absent, so no compare is needed. */
  private void insertRehashed(final long hash, final long parentKey, final QNm name, final byte kind,
      final long value) {
    int slot = (int) hash & mask;
    while (names[slot] != null) {
      slot = (slot + 1) & mask;
    }
    hashes[slot] = hash;
    parentKeys[slot] = parentKey;
    names[slot] = name;
    kinds[slot] = kind;
    values[slot] = value;
    size++;
  }

  private void allocate(final int capacity) {
    hashes = new long[capacity];
    parentKeys = new long[capacity];
    names = new QNm[capacity];
    kinds = new byte[capacity];
    values = new long[capacity];
    mask = capacity - 1;
    threshold = (int) (capacity * LOAD_FACTOR);
  }

  /** Smallest power of two that holds {@code expectedEntries} at {@link #LOAD_FACTOR}. */
  private static int capacityFor(final int expectedEntries) {
    if (expectedEntries <= 0) {
      return MIN_CAPACITY;
    }
    final long wanted = (long) Math.ceil(expectedEntries / (double) LOAD_FACTOR);
    if (wanted >= 1L << 30) {
      return 1 << 30;
    }
    int capacity = MIN_CAPACITY;
    while (capacity < wanted) {
      capacity <<= 1;
    }
    return capacity;
  }
}
