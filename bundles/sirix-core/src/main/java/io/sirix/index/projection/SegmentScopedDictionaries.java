/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * ENCODE-direction dictionaries scoped to a SEGMENT of record pages, minted during the load.
 *
 * <p>
 * This is the write half of {@code docs/SEGMENT_SCOPED_DICTIONARIES.md}: the answer to the pre-pass.
 * {@link TrieLaneWriteDictionaries} resolves against a dictionary a PRE-PASS already committed, which
 * is why the corpus must be read twice, why the value set must be closed before the shred, and why an
 * unknown value fails the build. Here a segment's dictionary is built AS its pages are encoded — a
 * value is minted the first time it is seen — so there is no pre-pass, no second read, no closed
 * corpus and no absent value.
 * </p>
 *
 * <h2>Why the anchor is a SEGMENT id, not a storage key</h2>
 *
 * A page records {@link #dictionaryKey} while it is encoded, and a segment's dictionary cannot be
 * written until every page of that segment has been encoded — so the storage key does not exist yet at
 * the moment the page needs an anchor. The anchor is therefore the segment's own id, and the resource
 * maps segment to header key once the segment is persisted. The page format is unchanged: the anchor
 * is a {@code long} the page stores and hands back to the reader, and only the two resolvers interpret
 * it.
 *
 * <h2>Why views are per PAGE, and why that is the correctness argument</h2>
 *
 * {@link #idOf} carries no page, and record pages are encoded on the async flush pool
 * ({@code sirix.asyncFlush.parallelism}) — so a page of segment N can be encoded AFTER the writer has
 * moved on to segment N+1. A resolver that answered "the segment I am currently filling" would mint
 * that page's ids in N+1 and stamp N+1's anchor onto a page whose neighbours point at N: a coherent
 * wrong answer, and one the reader's entry-count validity check cannot catch, because both
 * dictionaries are live and both are large enough.
 *
 * <p>
 * So the segment is bound to the PAGE, not to a moment: {@link #viewFor} is called on the
 * single-threaded writer side where the page's record-page key is known, and the view the page carries
 * answers for that page's segment forever after, whatever the flush pool does and whenever it does it.
 * This is the same class of defect as the thread-local scratch shared between a serializer and a
 * decoder re-entered from inside it — anything an encoder reads as "the current X" is suspect when the
 * encoder runs on a pool.
 * </p>
 *
 * <h2>Ids are ARRIVAL-ordered, and that costs nothing here</h2>
 *
 * Ids cannot be collation-ranked: they are minted at page encode, before the segment's value set is
 * known. That is not a compromise. Nothing probes value to id after a segment freezes — the read side
 * resolves id to value, which is an indexed lookup on any id order — so a segment dictionary needs no
 * persisted forward index, which is the entire cost that made a corpus-wide streaming dictionary
 * unaffordable (0.81 radix nodes per entry, each with a 256-slot child array, retained by
 * copy-on-write across appends: 64.7 B/entry at D = 275 K rising to 1,650 at D = 18 M). Rank order
 * still matters for the PROJECTION's dictionary, where id-order zone pruning reads it; it does not
 * matter here.
 *
 * <h2>What it deliberately cannot do</h2>
 *
 * The DECODE direction. {@link #valueOf} and {@link #accepts} always refuse, exactly as
 * {@link TrieLaneWriteDictionaries} does: an encoder never turns an id back into bytes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentScopedDictionaries {

  /**
   * Record pages per segment. The page trie groups {@code Constants.INP_REFERENCE_COUNT} leaves under
   * one indirect page, so a power of two at or above that aligns segments with the trie and lets a
   * page's segment be derived rather than stored.
   */
  private final long leavesPerSegment;

  /**
   * Tag (path node key) to column, republished as a whole immutable map when the load resolves new
   * path classes — never mutated in place, because the flush lane reads it while the extraction side
   * grows it. A tag absent from the snapshot simply keeps its bytes on that page: storage, never
   * correctness.
   */
  private volatile Int2IntMap columnByTag;

  /** {@code (segment, column)} to its minting state, created on first sight. */
  private final ConcurrentHashMap<Long, ColumnDictionary> dictionaries = new ConcurrentHashMap<>();

  /**
   * @param leavesPerSegment record pages per segment, a positive power of two
   * @param columnByTag initial tag-to-column mapping; may be replaced later by {@link #publishTags}
   */
  public SegmentScopedDictionaries(final long leavesPerSegment, final Int2IntMap columnByTag) {
    if (leavesPerSegment <= 0 || Long.bitCount(leavesPerSegment) != 1) {
      throw new IllegalArgumentException("leavesPerSegment must be a positive power of two: " + leavesPerSegment);
    }
    this.leavesPerSegment = leavesPerSegment;
    this.columnByTag = requireNonNull(columnByTag, "columnByTag must not be null");
  }

  /**
   * Replace the tag-to-column mapping wholesale. The caller owns the map afterwards only if it never
   * mutates it: readers hold the reference, so a published map must be immutable in practice.
   */
  public void publishTags(final Int2IntMap tags) {
    this.columnByTag = requireNonNull(tags, "tags must not be null");
  }

  /** The segment {@code recordPageKey} belongs to. */
  public long segmentOf(final long recordPageKey) {
    if (recordPageKey < 0) {
      throw new IllegalArgumentException("recordPageKey must not be negative: " + recordPageKey);
    }
    return recordPageKey / leavesPerSegment;
  }

  /**
   * The resolver a page must carry: bound to THAT page's segment for the rest of its life. Install it
   * on the single-threaded writer side, where the page's key is known and before the page reaches the
   * flush lane.
   */
  public GlobalStringDictionaries viewFor(final long recordPageKey) {
    return new SegmentView(segmentOf(recordPageKey));
  }

  /** Distinct values minted for {@code column} in {@code segment}; {@code 0} when it has none. */
  public int entryCount(final long segment, final int column) {
    final ColumnDictionary dictionary = dictionaries.get(key(segment, column));
    return dictionary == null
        ? 0
        : dictionary.size();
  }

  /**
   * The segment's values for {@code column} in ID ORDER, which is arrival order — the stream a
   * persisting caller writes, where the value at index {@code i} is id {@code i + 1}.
   *
   * <p>
   * Call once the segment's pages have all been encoded; a value minted afterwards would not appear.
   * Nothing enforces that here because the condition is a flush-completion property of the caller's
   * pipeline, not something this class can observe.
   * </p>
   */
  public Iterator<byte[]> valuesOf(final long segment, final int column) {
    final ColumnDictionary dictionary = dictionaries.get(key(segment, column));
    return dictionary == null
        ? new Iterator<>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public byte[] next() {
            throw new NoSuchElementException();
          }
        }
        : dictionary.valuesById();
  }

  /** Segments that have minted at least one value (test observability; unordered). */
  public int liveDictionaryCount() {
    return dictionaries.size();
  }

  private static Long key(final long segment, final int column) {
    return (segment << 20) | (column & 0xfffffL);
  }

  /**
   * One {@code (segment, column)} dictionary: value bytes to a 1-based id, minted once per distinct
   * value. Concurrent because the flush pool encodes several pages of one segment at a time; minting
   * happens once per distinct value per segment, not per row, so a concurrent map's cost is paid on a
   * path that is already rare.
   */
  private static final class ColumnDictionary {
    private final ConcurrentHashMap<ByteKey, Integer> ids = new ConcurrentHashMap<>();

    private final AtomicInteger next = new AtomicInteger(1); // 0 is ID_ABSENT

    int idOf(final byte[] value, final int offset, final int length) {
      final ByteKey probe = new ByteKey(Arrays.copyOfRange(value, offset, offset + length));
      final Integer existing = ids.get(probe);
      if (existing != null) {
        return existing;
      }
      return ids.computeIfAbsent(probe, ignored -> next.getAndIncrement());
    }

    int size() {
      return ids.size();
    }

    Iterator<byte[]> valuesById() {
      final byte[][] byId = new byte[ids.size()][];
      for (final var entry : ids.entrySet()) {
        final int id = entry.getValue();
        if (id >= 1 && id <= byId.length) {
          byId[id - 1] = entry.getKey().bytes;
        }
      }
      return Arrays.asList(byId).iterator();
    }
  }

  /** Value bytes with a cached hash: the map key, never handed out. */
  private static final class ByteKey {
    private final byte[] bytes;

    private final int hash;

    ByteKey(final byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(final @Nullable Object other) {
      return other instanceof final ByteKey key && hash == key.hash && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  /** The per-page resolver: every answer is its segment's, whatever the writer is doing now. */
  private final class SegmentView implements GlobalStringDictionaries {
    private final long segment;

    SegmentView(final long segment) {
      this.segment = segment;
    }

    /** The segment this view answers for (test observability). */
    long segment() {
      return segment;
    }

    @Override
    public boolean hasDictionary(final int tag) {
      return columnByTag.containsKey(tag);
    }

    @Override
    public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
      return false; // encode-direction only; reading is the read resolver's job
    }

    @Override
    public int idOf(final int tag, final byte[] value, final int offset, final int length) {
      requireNonNull(value, "value must not be null");
      if (offset < 0 || length < 0 || offset > value.length - length) {
        throw new IndexOutOfBoundsException("offset " + offset + " length " + length + " over " + value.length);
      }
      final Int2IntMap tags = columnByTag;
      if (!tags.containsKey(tag)) {
        return ID_ABSENT; // not a projected column: the page keeps its bytes
      }
      final int column = tags.get(tag);
      return dictionaries.computeIfAbsent(key(segment, column), ignored -> new ColumnDictionary())
                         .idOf(value, offset, length);
    }

    @Override
    public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
        final int id) {
      return null; // encode-direction only
    }

    @Override
    public long dictionaryKey(final int tag) {
      return segment;
    }

    @Override
    public int dictionaryEntryCount(final int tag) {
      final Int2IntMap tags = columnByTag;
      return tags.containsKey(tag)
          ? entryCount(segment, tags.get(tag))
          : 0;
    }
  }

  /** An empty tag map, for a load that has resolved no path class yet. */
  public static Int2IntMap noTags() {
    return Int2IntMaps.EMPTY_MAP;
  }
}
