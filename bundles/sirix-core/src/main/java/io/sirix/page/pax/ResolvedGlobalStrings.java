/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.exception.SirixIOException;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * A page's global-tag ids already turned into BYTES — what a {@link GlobalStringDictionaries}
 * produced, never the resolver that produced it.
 *
 * <h2>Why the page caches bytes and not a resolver</h2>
 *
 * The obvious design hands the page its transaction's resolver and lets value re-injection call it
 * during chunk expansion. A call-graph walk killed it. Expansion is reached from four places that
 * have <em>no reader on the stack at all</em> — the writer's copy-on-write {@code deepCopy()} on the
 * flush lane, the two commit-time FSST passes, and the static versioning combine — so there is
 * nothing to parameterize and no reader to find. Worse, {@code LazyChunkedBody} holds
 * {@code synchronized(page)} across the injector, so a resolver call inside expansion would be an
 * unbounded trie descent under a page monitor with uncontrolled lock order, which is precisely what
 * {@link GlobalStringDictionaries}'s own contract forbids for decode.
 *
 * <p>
 * So resolution moves OUT of expansion. A reader-held site resolves the whole page's tags in one
 * ascending walk and leaves this table behind; expansion then reads an array. The table holds
 * nothing transaction-scoped, which is the property that lets a page keep it for its whole life in
 * the buffer cache — the same property that lets an FSST page cache its symbol table.
 * </p>
 *
 * <h2>Resolution is page-determined</h2>
 *
 * The first transaction to touch a page fixes its values for every later one, INCLUDING a
 * transaction whose own anchors would have refused the tag. That is correct rather than lucky. A
 * refusal is conservatism about a dictionary the refusing transaction cannot validate, not evidence
 * that the bytes differ: the page NAMES the dictionary its ids were minted against, a rank-ordered
 * dictionary only ever appends, so ids {@code 1..recordedEntryCount} resolve to the same bytes for
 * every reader that can see that dictionary at all. Two readers therefore cannot disagree about a
 * value — one of them can only decline to answer.
 *
 * <h2>Size</h2>
 *
 * A leaf holds about six distinct values per converted tag, so this is a handful of byte arrays per
 * page — against the ~512 copies the expanded heap materialises for the same values anyway. It adds
 * roughly 1 % to what an expanded page already costs, and only for pages that actually use the lane.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ResolvedGlobalStrings {

  /** Marks a local tag index that is not global; no tag value is ever this. */
  private static final int NOT_GLOBAL = Integer.MIN_VALUE;

  /**
   * The table of a page that has no global tags at all. Distinct from {@code null}, which means
   * "not resolved yet" — the difference between the two is what makes the refusal in
   * {@code PageKind} a statement about wiring rather than about content.
   */
  public static final ResolvedGlobalStrings NONE = new ResolvedGlobalStrings(new int[0], new byte[0][][], new int[0][]);

  /** Per LOCAL tag index: the tag's VALUE (a path node key), or {@link #NOT_GLOBAL}. */
  private final int[] tagValues;

  /** Per local tag index: the resolved bytes indexed by LOCAL dictionary id, or {@code null}. */
  private final byte[][][] values;

  /**
   * Per local tag index: the GLOBAL id each local dictionary slot carried, or {@code null}.
   * Diagnostics only — an error that names the id it could not resolve is the difference between a
   * reproducible report and a re-derivation, and the array is six ints per tag.
   */
  private final int[][] globalIds;

  private ResolvedGlobalStrings(final int[] tagValues, final byte[][][] values, final int[][] globalIds) {
    this.tagValues = tagValues;
    this.values = values;
    this.globalIds = globalIds;
  }

  /**
   * Build a table for a page with {@code tagCount} local tags.
   *
   * @param tagCount the page's {@code parentDictSize}
   * @return a builder whose {@link Builder#build()} is the table
   */
  public static Builder forTags(final int tagCount) {
    if (tagCount < 0) {
      throw new IllegalArgumentException("tag count must not be negative, got " + tagCount);
    }
    return new Builder(tagCount);
  }

  /**
   * Resolve every global tag of a page's string region in ONE pass, ascending by dictionary id.
   *
   * <p>
   * The algorithm lives here, beside the table it produces, rather than in the reader that calls it:
   * the reader owns the SITE (where a resolution is safe and a guard must be held), and this owns
   * WHAT a resolution is. It also means a test can exercise the real walk against a stub dictionary
   * instead of restating it.
   * </p>
   *
   * <p>
   * <b>Ascending is worth 5.6×.</b> A dictionary point read costs 417 ns at a random id and 75 ns at
   * a sequential one, because the cost is block residency rather than lookup depth. A leaf holds
   * about six distinct values per tag, so the ordering is an insertion sort over a handful of ints.
   * It is a performance property with a measurement behind it: resolving on demand, or in slot
   * order, or in parallel returns exactly the same values and loses the 5.6× with no test failing.
   * </p>
   *
   * <p>
   * All or nothing. A tag that cannot be resolved throws rather than contributing a hole — a record
   * with an absent value is not a record with an empty value, and that substitution is the failure
   * the whole anchor design exists to prevent.
   * </p>
   *
   * @param header a header already parsed from {@code stringPayload}
   * @param stringPayload the string region's payload
   * @param dictionaries the resolver; consulted here and never retained by the result
   * @param recordPageKey the page, for the message a refusal has to carry
   * @return the table; {@link #NONE} when the region marks no tag global
   */
  public static ResolvedGlobalStrings resolve(final StringRegion.Header header, final MemorySegment stringPayload,
      final GlobalStringDictionaries dictionaries, final long recordPageKey) {
    Builder builder = null;
    for (int tagIndex = 0; tagIndex < header.parentDictSize; tagIndex++) {
      if (!header.tagGlobal[tagIndex]) {
        continue;
      }
      if (builder == null) {
        builder = forTags(header.parentDictSize);
      }
      final int tagValue = header.parentDict[tagIndex];
      final long dictionaryKey = header.tagDictionaryKey[tagIndex];
      final int recordedEntryCount = header.tagDictionaryEntryCount[tagIndex];
      final int entries = header.tagStringDictSize[tagIndex];
      final int[] ids = new int[entries];
      for (int entry = 0; entry < entries; entry++) {
        ids[entry] = StringRegion.globalIdAt(stringPayload, header, tagIndex, entry);
      }
      final byte[][] values = new byte[entries][];
      final int[] order = ascendingByIdOrder(ids);
      for (int position = 0; position < entries; position++) {
        final int entry = order[position];
        final byte[] value = dictionaries.valueOf(tagValue, dictionaryKey, recordedEntryCount, ids[entry]);
        if (value == null) {
          throw new SirixIOException("record page " + recordPageKey + " stores tag " + tagValue
              + " as ids in dictionary " + dictionaryKey + " (recorded at " + recordedEntryCount
              + " entries), but this transaction cannot resolve id " + ids[entry]
              + " against it. Either the tag resolves against a different dictionary here, or the named one now has "
              + "fewer entries than the page recorded -- a rank-ordered dictionary only appends, so a smaller live "
              + "count means the key was reused and its ids mean something else now. Resolving anyway would return "
              + "a plausible wrong value; refusing is the only answer that is not a lie.");
        }
        values[entry] = value;
      }
      builder.tag(tagIndex, tagValue, ids, values);
    }
    return builder == null
        ? NONE
        : builder.build();
  }

  /**
   * Positions of {@code ids} in ascending id order.
   *
   * <p>
   * Insertion sort, because {@code ids.length} is about six: it allocates nothing beyond the result,
   * boxes nothing, and beats any comparator-driven sort at this size. Equal ids keep their relative
   * order, which costs nothing and makes the walk deterministic.
   * </p>
   */
  static int[] ascendingByIdOrder(final int[] ids) {
    final int[] order = new int[ids.length];
    for (int i = 0; i < ids.length; i++) {
      order[i] = i;
    }
    for (int i = 1; i < order.length; i++) {
      final int candidate = order[i];
      final int key = ids[candidate];
      int j = i - 1;
      while (j >= 0 && ids[order[j]] > key) {
        order[j + 1] = order[j];
        j--;
      }
      order[j + 1] = candidate;
    }
    return order;
  }

  /** Whether any tag on the page was resolved; {@code false} for {@link #NONE}. */
  public boolean isEmpty() {
    return tagValues.length == 0;
  }

  /**
   * The bytes for one value of a global tag.
   *
   * <p>
   * {@code expectedTagValue} is not decoration. The table is indexed by the tag's position in the
   * page's parsed header, and a position is only meaningful for the payload it was parsed from — so
   * the tag's VALUE is carried alongside and compared. Any re-encode that shifts positions turns
   * into a loud failure here instead of a silent read of another tag's dictionary. One int compare
   * per elided slot.
   * </p>
   *
   * @param tagIndex the tag's local index in the page's string-region header
   * @param expectedTagValue the tag value that index must carry ({@code header.parentDict[tagIndex]})
   * @param dictId the value's index in the tag's local dictionary
   * @return the bytes; never {@code null}
   * @throws IllegalStateException if the tag was not resolved or the index does not carry the
   *         expected tag value
   * @throws IndexOutOfBoundsException if {@code dictId} is outside the tag's dictionary
   */
  public byte[] value(final int tagIndex, final int expectedTagValue, final int dictId) {
    if (tagIndex < 0 || tagIndex >= tagValues.length) {
      throw new IllegalStateException("string-region tag index " + tagIndex + " is outside the " + tagValues.length
          + " tags this page's global-string table was resolved for");
    }
    if (tagValues[tagIndex] != expectedTagValue) {
      throw new IllegalStateException("string-region tag index " + tagIndex + " carries tag " + expectedTagValue
          + " but the resolved table holds " + (tagValues[tagIndex] == NOT_GLOBAL
              ? "no global tag"
              : "tag " + tagValues[tagIndex]) + " there — the region was re-encoded after it was resolved");
    }
    final byte[][] forTag = values[tagIndex];
    if (forTag == null) {
      throw new IllegalStateException("tag " + expectedTagValue + " has no resolved values on this page");
    }
    if (dictId < 0 || dictId >= forTag.length) {
      throw new IndexOutOfBoundsException(
          "dict id " + dictId + " outside tag " + expectedTagValue + "'s " + forTag.length + " resolved entries");
    }
    final byte[] value = forTag[dictId];
    if (value == null) {
      // The builder refuses a null entry, so this cannot come from a partial resolve; it would take
      // a table mutated after build, which is impossible for an instance this class hands out.
      throw new IllegalStateException(
          "tag " + expectedTagValue + " entry " + dictId + " resolved to nothing on this page");
    }
    return value;
  }

  /** The global id local slot {@code dictId} of {@code tagIndex} carried; {@code -1} if unknown. */
  public int globalIdAt(final int tagIndex, final int dictId) {
    if (tagIndex < 0 || tagIndex >= globalIds.length) {
      return -1;
    }
    final int[] forTag = globalIds[tagIndex];
    return forTag == null || dictId < 0 || dictId >= forTag.length
        ? -1
        : forTag[dictId];
  }

  @Override
  public String toString() {
    final StringBuilder text = new StringBuilder(64).append("ResolvedGlobalStrings[");
    boolean first = true;
    for (int t = 0; t < tagValues.length; t++) {
      if (tagValues[t] == NOT_GLOBAL) {
        continue;
      }
      if (!first) {
        text.append(", ");
      }
      first = false;
      text.append("tag ").append(tagValues[t]).append('=').append(values[t].length).append(" values");
    }
    return text.append(']').toString();
  }

  /** Accumulates one page's resolved tags; not thread-safe and not reused across pages. */
  public static final class Builder {

    private final int[] tagValues;
    private final byte[][][] values;
    private final int[][] globalIds;
    private int resolvedTags;

    private Builder(final int tagCount) {
      this.tagValues = new int[tagCount];
      this.values = new byte[tagCount][][];
      this.globalIds = new int[tagCount][];
      Arrays.fill(this.tagValues, NOT_GLOBAL);
    }

    /**
     * Record one tag's whole dictionary.
     *
     * @param tagIndex the tag's local index
     * @param tagValue the tag's value (path node key)
     * @param ids the global id per local dictionary slot, taken by reference
     * @param resolved the bytes per local dictionary slot, taken by reference; no entry may be null
     */
    public Builder tag(final int tagIndex, final int tagValue, final int[] ids, final byte[][] resolved) {
      if (tagIndex < 0 || tagIndex >= tagValues.length) {
        throw new IndexOutOfBoundsException("tag index " + tagIndex + " outside " + tagValues.length + " tags");
      }
      if (tagValue == NOT_GLOBAL) {
        throw new IllegalArgumentException("tag value " + tagValue + " collides with the not-global marker");
      }
      if (ids.length != resolved.length) {
        throw new IllegalArgumentException(
            "ids and values must be index-aligned: " + ids.length + " vs " + resolved.length);
      }
      for (int i = 0; i < resolved.length; i++) {
        if (resolved[i] == null) {
          // A half-resolved tag is the failure mode this whole design exists to prevent: expansion
          // would produce a record with an absent value, which is not a record with an empty value.
          // All or nothing, refused here rather than at the slot that meets the hole.
          throw new IllegalArgumentException(
              "tag " + tagValue + " entry " + i + " resolved to nothing; a tag is resolved whole or not at all");
        }
      }
      tagValues[tagIndex] = tagValue;
      values[tagIndex] = resolved;
      globalIds[tagIndex] = ids;
      resolvedTags++;
      return this;
    }

    /** How many tags have been recorded, for a caller that must prove it resolved every one. */
    public int resolvedTagCount() {
      return resolvedTags;
    }

    /** The immutable table. */
    public ResolvedGlobalStrings build() {
      return resolvedTags == 0
          ? NONE
          : new ResolvedGlobalStrings(tagValues, values, globalIds);
    }
  }

  /** Never {@code null}: {@link #NONE} stands in, so a caller can compare against it. */
  public static ResolvedGlobalStrings orNone(final @Nullable ResolvedGlobalStrings table) {
    return table == null
        ? NONE
        : table;
  }
}
