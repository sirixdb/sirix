package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

/**
 * One record of a sealed dictionary generation's PERMUTATION table: for a run of consecutive keys
 * the packed entry of each. A generation persists the permutation in BOTH directions as two runs of
 * these records behind one header key: {@code mint -> rank} at {@code rankTableKey + i}, then
 * {@code rank -> mint} at {@code rankTableKey + recordCountFor(P) + i}.
 *
 * <p>
 * A segment dictionary mints ids the moment a value is first encoded — on a flush thread, into a
 * page that is written long before the segment's value set is closed — so the ids in the pages are
 * in arrival order (MINTS). The seal then stores the values in UTF-16 collation order (RANKS, the
 * storage positions), which is what makes the front-coded blocks pay and a probe a binary search over
 * a separator array. The forward run is the bridge every read takes: {@code entryOf(mint)} is the
 * position the value occupies in the ordered storage. The inverse run serves the probe, which finds
 * a POSITION by binary search and must hand back the id the rows carry: one record read per probe
 * instead of an {@code orderedPrefixCount}-int inverse built per view — at a 100M scale a per-probe
 * allocation of megabytes on the query path, for a table that costs the same bytes again on disk
 * (≈ 2.5 B per distinct value, a rounding error of the resource).
 * </p>
 *
 * <p>
 * Entries are bit-packed at the generation's uniform width ({@link #bitsFor}), so a record covering
 * {@link #ENTRIES_PER_RECORD} keys is at most 64 KiB even at 32 bits and a key resolves in one shift
 * and one mask with no per-record decode state. The word array carries ONE padding word so a value
 * straddling two words reads branch-free ({@code words[w + 1] << ~s << 1} is zero when the shift is
 * zero, the standard trick) — which also means records are NOT concatenable by array copy; each is
 * unpacked through its own {@link #entryAt}. Records are addressed arithmetically for the {@code i}-th
 * run of {@link #ENTRIES_PER_RECORD} keys, so a reader holds record references, never a copy of the
 * table.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryRankTableNode implements DataRecord {

  /** Keys per record; {@code 1 << 14}, so {@code (key - 1) >>> 14} is the record index within a run. */
  public static final int ENTRIES_PER_RECORD = 1 << 14;

  /** {@code log2(ENTRIES_PER_RECORD)}, the shift that turns a key into its record index. */
  public static final int ENTRIES_PER_RECORD_SHIFT = 14;

  /** Widest entry: a rank or a mint is a positive {@code int}. */
  public static final int MAX_BITS_PER_ENTRY = 32;

  private final long nodeKey;

  /** First key this record covers, {@code 1 + i * ENTRIES_PER_RECORD} for record {@code i} of its run. */
  private final int firstKey;

  /** How many keys this record covers, {@code 1..ENTRIES_PER_RECORD}; only the last record of a run is short. */
  private final int count;

  /** Bits per packed entry, {@code 1..32}, uniform across the generation's records (both runs). */
  private final int bitsPerEntry;

  private final long mask;

  /** Packed entries plus one padding word. */
  private final long[] words;

  private ValueDictionaryRankTableNode(final long nodeKey, final int firstKey, final int count,
      final int bitsPerEntry, final long[] words) {
    this.nodeKey = nodeKey;
    this.firstKey = firstKey;
    this.count = count;
    this.bitsPerEntry = bitsPerEntry;
    this.mask = (1L << bitsPerEntry) - 1L;
    this.words = words;
  }

  /**
   * The uniform entry width for a generation of {@code orderedPrefixCount} ranked values: enough
   * bits to hold the largest rank, which is the count itself.
   *
   * @param orderedPrefixCount the number of ranked values, at least 1
   * @return bits per entry, {@code 1..32}
   */
  public static int bitsFor(final int orderedPrefixCount) {
    if (orderedPrefixCount < 1) {
      throw new IllegalArgumentException("a rank table needs at least one ranked value, not " + orderedPrefixCount);
    }
    return Integer.SIZE - Integer.numberOfLeadingZeros(orderedPrefixCount);
  }

  /**
   * How many records a generation of {@code orderedPrefixCount} ranked values needs.
   *
   * @param orderedPrefixCount the number of ranked values, at least 1
   * @return the record count, at least 1
   */
  public static int recordCountFor(final int orderedPrefixCount) {
    if (orderedPrefixCount < 1) {
      throw new IllegalArgumentException("a rank table needs at least one ranked value, not " + orderedPrefixCount);
    }
    return (orderedPrefixCount - 1 >>> ENTRIES_PER_RECORD_SHIFT) + 1;
  }

  /** Number of words that hold {@code count} entries of {@code bitsPerEntry} bits, plus the padding word. */
  static int wordsFor(final int count, final int bitsPerEntry) {
    final long bits = (long) count * bitsPerEntry;
    return (int) ((bits + 63) >>> 6) + 1;
  }

  /**
   * Pack the entries of the keys {@code firstKey .. firstKey + count - 1} into a record.
   *
   * @param nodeKey the record key, the run's first key plus {@code recordIndex}
   * @param firstKey the first key covered, {@code 1 + recordIndex * ENTRIES_PER_RECORD}
   * @param entryByKey entries indexed by key (slot 0 unused), each in {@code 1..orderedPrefixCount}
   * @param count how many keys this record covers
   * @param bitsPerEntry the generation's uniform width, {@link #bitsFor}
   * @return the packed record
   */
  public static ValueDictionaryRankTableNode pack(final long nodeKey, final int firstKey, final int[] entryByKey,
      final int count, final int bitsPerEntry) {
    if (nodeKey <= 0) {
      throw new IllegalArgumentException("invalid value dictionary rank table key " + nodeKey);
    }
    if (entryByKey == null) {
      throw new NullPointerException("entryByKey must not be null");
    }
    checkShape(firstKey, count, bitsPerEntry);
    if ((long) firstKey + count > entryByKey.length) {
      throw new IllegalArgumentException("rank table record " + firstKey + "+" + count + " exceeds the entries given ("
          + entryByKey.length + ")");
    }
    final long[] words = new long[wordsFor(count, bitsPerEntry)];
    final long limit = Math.min(Integer.MAX_VALUE, (1L << bitsPerEntry) - 1L);
    for (int i = 0; i < count; i++) {
      final int entry = entryByKey[firstKey + i];
      if (entry < 1 || entry > limit) {
        throw new IllegalArgumentException("entry " + entry + " of key " + (firstKey + i) + " does not fit "
            + bitsPerEntry + " bits (or is not positive)");
      }
      final long bit = (long) i * bitsPerEntry;
      final int w = (int) (bit >>> 6);
      final int s = (int) bit & 63;
      words[w] |= (long) entry << s;
      // The high part when the entry straddles a word boundary; `>>> ~s >>> 1` is zero at s == 0,
      // so the padding word never sees a stray bit (and neither does the next entry's word).
      words[w + 1] |= (long) entry >>> ~s >>> 1;
    }
    return new ValueDictionaryRankTableNode(nodeKey, firstKey, count, bitsPerEntry, words);
  }

  /**
   * Takes ownership of a deserialized word array; the caller must not touch it again.
   *
   * @param words {@link #wordsFor}{@code (count, bitsPerEntry)} words, the last one padding
   */
  public static ValueDictionaryRankTableNode takeOwnership(final long nodeKey, final int firstKey, final int count,
      final int bitsPerEntry, final long[] words) {
    if (nodeKey <= 0) {
      throw new IllegalArgumentException("invalid value dictionary rank table key " + nodeKey);
    }
    if (words == null) {
      throw new NullPointerException("words must not be null");
    }
    checkShape(firstKey, count, bitsPerEntry);
    if (words.length != wordsFor(count, bitsPerEntry)) {
      throw new IllegalArgumentException("rank table record of " + count + " entries at " + bitsPerEntry
          + " bits needs " + wordsFor(count, bitsPerEntry) + " words, not " + words.length);
    }
    return new ValueDictionaryRankTableNode(nodeKey, firstKey, count, bitsPerEntry, words);
  }

  private static void checkShape(final int firstKey, final int count, final int bitsPerEntry) {
    if (firstKey < 1 || (firstKey - 1 & ENTRIES_PER_RECORD - 1) != 0) {
      throw new IllegalArgumentException("a rank table record must start at 1 + k * " + ENTRIES_PER_RECORD
          + ", not at key " + firstKey);
    }
    if (count < 1 || count > ENTRIES_PER_RECORD) {
      throw new IllegalArgumentException("a rank table record covers 1.." + ENTRIES_PER_RECORD + " keys, not " + count);
    }
    if (bitsPerEntry < 1 || bitsPerEntry > MAX_BITS_PER_ENTRY) {
      throw new IllegalArgumentException("rank table entries are 1.." + MAX_BITS_PER_ENTRY + " bits wide, not "
          + bitsPerEntry);
    }
  }

  /** First key this record covers. */
  public int firstKey() {
    return firstKey;
  }

  /** How many consecutive keys this record covers. */
  public int size() {
    return count;
  }

  /** Bits per packed entry. */
  public int bitsPerEntry() {
    return bitsPerEntry;
  }

  /** Whether {@code key} is one of this record's. */
  public boolean covers(final int key) {
    return key >= firstKey && key - firstKey < count;
  }

  /**
   * The entry ({@code 1..orderedPrefixCount}) of the {@code index}-th key of this record.
   * Allocation- and branch-free after the bounds check: one shift per word, one mask.
   *
   * @param index {@code key - firstKey}, in {@code 0..size() - 1}
   */
  public int entryAt(final int index) {
    if (index < 0 || index >= count) {
      throw new IndexOutOfBoundsException("rank table index " + index + " outside 0.." + (count - 1));
    }
    final long bit = (long) index * bitsPerEntry;
    final int w = (int) (bit >>> 6);
    final int s = (int) bit & 63;
    return (int) ((words[w] >>> s | words[w + 1] << ~s << 1) & mask);
  }

  /**
   * The entry of {@code key}, which must be one this record {@link #covers}: the storage position of
   * a mint in the forward run, the mint stored at a position in the inverse run.
   */
  public int entryOf(final int key) {
    return entryAt(key - firstKey);
  }

  /** RAW packed words including the padding word; for the serializer only. */
  public long[] words() {
    return words;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_RANK_TABLE;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return 0;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return 0;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("firstKey", firstKey)
                         .add("count", count)
                         .add("bitsPerEntry", bitsPerEntry)
                         .toString();
  }
}
