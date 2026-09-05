/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

/**
 * ENCODE-direction dictionaries scoped to a SEGMENT of record pages, minted during the load.
 *
 * <p>
 * This is the write half of {@code docs/SEGMENT_DICTIONARY_DESIGN.md}: the answer to the pre-pass.
 * {@link TrieLaneWriteDictionaries} resolves against a dictionary a PRE-PASS already committed, which
 * is why the corpus must be read twice, why the value set must be closed before the shred, and why an
 * unknown value fails the build. Here a segment's dictionary is built AS its pages are encoded — a
 * value is minted the first time it is seen — so there is no pre-pass, no second read, no closed
 * corpus and no absent value.
 * </p>
 *
 * <h2>Where a segment begins and ends</h2>
 *
 * {@link SegmentBoundaries} decides, at {@link #adopt}, on the writer side: the open segment closes
 * once the distinct-value bytes minted into it reach its budget, and the page being adopted opens
 * the next one. The bytes are counted here, on the mint path, per segment, which is why adoption
 * and minting live in one class.
 *
 * <h2>Why the anchor is a SEGMENT id, not a storage key</h2>
 *
 * A page records {@link SegmentView#dictionaryKey} while it is encoded, and a segment's dictionary
 * cannot be written until every page of that segment has been encoded — so the storage key does not
 * exist yet at the moment the page needs an anchor. The anchor is therefore the segment's own id,
 * and the resource maps segment to header key once the segment is persisted. The page format is
 * unchanged: the anchor is a {@code long} the page stores and hands back to the reader, and only
 * the two resolvers interpret it.
 *
 * <h2>Why views are per PAGE, and why that is the correctness argument</h2>
 *
 * {@link GlobalStringDictionaries#idOf} carries no page, and record pages are encoded on the async
 * flush pool ({@code sirix.asyncFlush.parallelism}) — so a page of segment N can be encoded AFTER
 * the writer has moved on to segment N+1. A resolver that answered "the segment I am currently
 * filling" would mint that page's ids in N+1 and stamp N+1's anchor onto a page whose neighbours
 * point at N: a coherent wrong answer, and one the reader's entry-count validity check cannot catch,
 * because both dictionaries are live and both are large enough.
 *
 * <p>
 * So the segment is bound to the PAGE, not to a moment: {@link #adopt} is called on the
 * single-threaded writer side where the page's record-page key is known, and the view the page
 * carries answers for that page's segment forever after, whatever the flush pool does and whenever
 * it does it. This is the same class of defect as the thread-local scratch shared between a
 * serializer and a decoder re-entered from inside it — anything an encoder reads as "the current X"
 * is suspect when the encoder runs on a pool.
 * </p>
 *
 * <h2>Ids are MINTS: arrival-ordered, dense from 1, permanent</h2>
 *
 * Ids cannot be collation-ranked here: they are minted at page encode, before the segment's value
 * set is known. The seal sorts the values afterwards and persists a rank table beside the ordered
 * storage ({@code GlobalValueDictionary.attachRankTable}), so a page's id stays what this class
 * minted while the storage is served in collation order. Nothing here needs a forward index and
 * nothing here is ever appended to after the seal.
 *
 * <h2>The mint map, on the hot path</h2>
 *
 * Every string value of every page goes through {@link GlobalStringDictionaries#idOf}, and almost
 * all of them HIT (a segment holds a few hundred thousand distinct values over millions of rows). So
 * the hit path allocates nothing: an open-addressing table of {@code (hash << 32 | id)} words,
 * probed lock-free with acquire loads, and the value bytes compared in place against the slice the
 * encoder hands in. A MISS takes the dictionary's lock, re-probes (another encoder may have minted
 * the value meanwhile), copies the bytes once — they have to be kept anyway — and publishes the
 * slot with a release store after the count and the value it points at, so a reader that sees the
 * slot sees both. The count therefore never lags an issued id: the encoder derives the id lane's
 * bit width from it, and a count below an id would write that id TRUNCATED, silently.
 *
 * <h2>What it deliberately cannot do</h2>
 *
 * The DECODE direction. {@link SegmentView#valueOf} and {@link SegmentView#accepts} always refuse,
 * exactly as {@link TrieLaneWriteDictionaries} does: an encoder never turns an id back into bytes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentScopedDictionaries {

  private static final VarHandle STATES = MethodHandles.arrayElementVarHandle(SegmentState[].class);
  private static final VarHandle ADOPTING;

  static {
    try {
      ADOPTING = MethodHandles.lookup().findVarHandle(SegmentScopedDictionaries.class, "adopting", int.class);
    } catch (final ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static final SegmentState[] NO_STATES = new SegmentState[0];
  /** The answer for a segment or column that minted nothing; shared because it is immutable. */
  private static final byte[][] NO_VALUES = new byte[0][];

  private final SegmentBoundaries boundaries;

  /**
   * Tag (path node key) to column, republished as a whole immutable map when the load resolves new
   * path classes — never mutated in place, because the flush lane reads it while the extraction side
   * grows it. A tag absent from the snapshot simply keeps its bytes on that page: storage, never
   * correctness. Built by {@link TagColumnMap}, so a tag two columns claim is absent here exactly as
   * it is absent from the reader's map.
   */
  private volatile Int2IntMap columnByTag;

  /**
   * Segment to its minting state, at index {@code segment}; {@code null} until the segment adopts its
   * first page. Segments are dense from 0, so an array indexed by segment is the natural map and
   * boxes nothing. Grown by the adopting thread only, read from any thread through acquire loads.
   */
  private volatile SegmentState[] states = NO_STATES;

  /**
   * Nonzero while a thread is inside {@link #adopt}. Adoption is sequential by contract — one
   * adopter at a time, with an ordered hand-over between the bulk importer's coordinator and the
   * transaction's own thread — and {@link #adopt} updates the boundary cursors and the state array
   * without atomics on that basis. This is the positive witness for that contract: one compare-and-set
   * per PAGE, which is nothing beside the page itself, and a caller that breaks the contract is told
   * so instead of silently losing a segment state to a lost update.
   */
  @SuppressWarnings("unused") // read and written through ADOPTING
  private volatile int adopting;

  /**
   * @param boundaries where segments begin and end; owned by this instance from now on
   * @param columnByTag initial tag-to-column mapping; may be replaced later by {@link #publishTags}
   */
  public SegmentScopedDictionaries(final SegmentBoundaries boundaries, final Int2IntMap columnByTag) {
    this.boundaries = requireNonNull(boundaries, "boundaries must not be null");
    this.columnByTag = requireNonNull(columnByTag, "columnByTag must not be null");
  }

  /**
   * Replace the tag-to-column mapping wholesale. The caller owns the map afterwards only if it never
   * mutates it: readers hold the reference, so a published map must be immutable in practice. A view
   * picks the new map up at its next {@link SegmentView#hasDictionary} — the opener of a tag's encode
   * run — never in the middle of one.
   */
  public void publishTags(final Int2IntMap tags) {
    this.columnByTag = requireNonNull(tags, "tags must not be null");
  }

  /** The tag-to-column map as published now; the seal files each dictionary under its own tags. */
  public Int2IntMap tags() {
    return columnByTag;
  }

  /** The boundaries this instance decides at adoption and reports at seal. */
  public SegmentBoundaries boundaries() {
    return boundaries;
  }

  /** The segment {@code recordPageKey} belongs to; fixed for every adopted page. Any thread. */
  public int segmentOf(final long recordPageKey) {
    return boundaries.segmentOf(recordPageKey);
  }

  /**
   * Adopt a page: close the open segment if it is full, place the page, and hand back the resolver
   * the page must carry — bound to THAT page's segment for the rest of its life. Call on the
   * single-threaded writer side, where the page's key is known and before the page reaches the flush
   * lane; {@link SegmentBoundaries#adopt} holds the adopting thread to that.
   */
  public SegmentView adopt(final long recordPageKey) {
    if (!ADOPTING.compareAndSet(this, 0, 1)) {
      throw new IllegalStateException("two threads adopted a page at the same time; adoption is sequential — the"
          + " boundary cursors and the segment states are updated without atomics on that contract");
    }
    try {
      final int segment = boundaries.adopt(recordPageKey, mintedBytes(boundaries.openSegment()));
      SegmentState state = stateOf(segment);
      if (state == null) {
        state = new SegmentState();
        publishState(segment, state);
      } else if (state.released) {
        // Loud, because in a load that seals once at the end this can only be a bookkeeping failure.
        // The general write path — a later revision touching a page of a long-sealed segment — needs
        // the other answer (keep the bytes, or probe the persisted dictionary); that arrives with
        // incremental sealing, which is also what first makes this reachable by design.
        throw new IllegalStateException("segment " + segment + " was sealed and released; page " + recordPageKey
            + " cannot be adopted into it");
      }
      return new SegmentView(segment, state);
    } finally {
      ADOPTING.setRelease(this, 0);
    }
  }

  /** Distinct values minted for {@code column} in {@code segment}; {@code 0} when it has none. */
  public int entryCount(final int segment, final int column) {
    requireNonNegativeSegment(segment);
    requireNonNegativeColumn(column);
    final SegmentState state = stateOf(segment);
    return state == null
        ? 0
        : state.entryCount(column);
  }

  /** Distinct-value bytes minted into {@code segment} so far, summed over its columns. */
  public long mintedBytes(final int segment) {
    requireNonNegativeSegment(segment);
    final SegmentState state = stateOf(segment);
    return state == null
        ? 0L
        : state.mintedBytes.sum();
  }

  /**
   * The segment's values for {@code column} in ID ORDER — index {@code i} holds id {@code i + 1} — as
   * a fresh array the caller may sort or hand on. The ELEMENTS are the dictionary's own byte arrays,
   * not copies: a caller that writes into one corrupts the dictionary it came from.
   *
   * <p>
   * Call once the segment's pages have all been encoded; a value minted afterwards would not appear.
   * Nothing enforces that here because the condition is a flush-completion property of the caller's
   * pipeline, not something this class can observe.
   * </p>
   */
  public byte[][] valuesById(final int segment, final int column) {
    requireNonNegativeSegment(segment);
    requireNonNegativeColumn(column);
    final SegmentState state = stateOf(segment);
    if (state == null) {
      return NO_VALUES;
    }
    final ColumnDictionary dictionary = state.dictionaryAt(column);
    return dictionary == null
        ? NO_VALUES
        : dictionary.valuesById();
  }

  /** {@link #valuesById} as the stream a persisting caller reads. */
  public Iterator<byte[]> valuesOf(final int segment, final int column) {
    return Arrays.asList(valuesById(segment, column)).iterator();
  }

  /**
   * Drop {@code segment}'s mint maps once it is sealed: the dictionary is persisted, so the values
   * are no longer needed in memory. A later mint into the segment, and a later adoption into it, are
   * refused loudly rather than starting a second dictionary nobody would seal.
   *
   * @throws IllegalStateException if the segment has adopted no page — there is nothing to release,
   *         and a silent no-op here would let a later adoption start exactly the unsealed dictionary
   *         this method exists to prevent
   */
  public void release(final int segment) {
    requireNonNegativeSegment(segment);
    final SegmentState state = stateOf(segment);
    if (state == null) {
      throw new IllegalStateException("segment " + segment + " has adopted no page; there is nothing to release");
    }
    state.release();
  }

  /**
   * {@code (segment, column)} dictionaries {@code segment} has minted into, released ones included.
   * The seal compares this with the number of dictionaries it wrote: a dictionary at a column the
   * seal does not iterate would be released unwritten, and every page that stamped ids against it
   * would be unreadable.
   */
  public int dictionaryCount(final int segment) {
    requireNonNegativeSegment(segment);
    final SegmentState state = stateOf(segment);
    return state == null
        ? 0
        : state.dictionaryCount();
  }

  /** Segments that have adopted at least one page, released ones included (test observability). */
  int liveSegmentCount() {
    int count = 0;
    for (final SegmentState state : states) {
      if (state != null) {
        count++;
      }
    }
    return count;
  }

  /**
   * {@code (segment, column)} dictionaries that can still mint: created by a value and not released
   * (test observability).
   */
  int liveDictionaryCount() {
    int count = 0;
    for (final SegmentState state : states) {
      if (state != null && !state.released) {
        count += state.dictionaryCount();
      }
    }
    return count;
  }

  /** Slots in the mint table of {@code (segment, column)}, {@code 0} without one (test observability). */
  int slotCount(final int segment, final int column) {
    final ColumnDictionary dictionary = dictionaryAt(segment, column);
    return dictionary == null
        ? 0
        : dictionary.slotCount();
  }

  /** Value slots {@code (segment, column)} keeps resident, {@code 0} without one (test observability). */
  int retainedValueSlots(final int segment, final int column) {
    final ColumnDictionary dictionary = dictionaryAt(segment, column);
    return dictionary == null
        ? 0
        : dictionary.retainedValueSlots();
  }

  private @Nullable ColumnDictionary dictionaryAt(final int segment, final int column) {
    requireNonNegativeSegment(segment);
    requireNonNegativeColumn(column);
    final SegmentState state = stateOf(segment);
    return state == null
        ? null
        : state.dictionaryAt(column);
  }

  private @Nullable SegmentState stateOf(final int segment) {
    final SegmentState[] snapshot = states;
    return segment < snapshot.length
        ? (SegmentState) STATES.getAcquire(snapshot, segment)
        : null;
  }

  /** Adopting thread only. Publishes the state so a reader that sees it sees a constructed one. */
  private void publishState(final int segment, final SegmentState state) {
    final SegmentState[] snapshot = states;
    if (segment < snapshot.length) {
      STATES.setRelease(snapshot, segment, state);
      return;
    }
    final SegmentState[] grown = Arrays.copyOf(snapshot, Math.max(segment + 1, Math.max(4, snapshot.length << 1)));
    grown[segment] = state;
    states = grown; // the volatile write publishes the element with the array
  }

  private static void requireNonNegativeSegment(final int segment) {
    if (segment < 0) {
      throw new IllegalArgumentException("segment must not be negative: " + segment);
    }
  }

  private static void requireNonNegativeColumn(final int column) {
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
  }

  /** One segment's minting state: a dictionary per column and the bytes they hold between them. */
  private static final class SegmentState {
    private static final VarHandle COLUMNS = MethodHandles.arrayElementVarHandle(ColumnDictionary[].class);
    private static final ColumnDictionary[] NO_COLUMNS = new ColumnDictionary[0];

    /**
     * Dictionaries at index {@code column}; {@code null} until the column mints its first value.
     * Grown under the lock, read from any thread through acquire loads.
     */
    private volatile ColumnDictionary[] columns = NO_COLUMNS;
    private final LongAdder mintedBytes = new LongAdder();
    private volatile boolean released;

    /**
     * The dictionary for {@code column}, created on first use. Synchronised against {@link #release}
     * so a column cannot be created in the gap between the release flag and the release loop — that
     * dictionary would be live, unreleased and never sealed. Taken once per tag run per page (the
     * view caches it), never per value.
     */
    synchronized ColumnDictionary column(final int column) {
      requireNonNegativeColumn(column);
      if (released) {
        throw new IllegalStateException(
            "the segment was sealed and released; no value may be minted into it any more");
      }
      ColumnDictionary[] snapshot = columns;
      if (column >= snapshot.length) {
        snapshot = Arrays.copyOf(snapshot, Math.max(column + 1, Math.max(2, snapshot.length << 1)));
        columns = snapshot;
      }
      ColumnDictionary dictionary = snapshot[column]; // the lock makes a plain read current
      if (dictionary == null) {
        dictionary = new ColumnDictionary(mintedBytes);
        COLUMNS.setRelease(snapshot, column, dictionary); // a reader that sees it sees a constructed one
      }
      return dictionary;
    }

    @Nullable ColumnDictionary dictionaryAt(final int column) {
      final ColumnDictionary[] snapshot = columns;
      return column < snapshot.length
          ? (ColumnDictionary) COLUMNS.getAcquire(snapshot, column)
          : null;
    }

    int entryCount(final int column) {
      final ColumnDictionary dictionary = dictionaryAt(column);
      return dictionary == null
          ? 0
          : dictionary.size();
    }

    int dictionaryCount() {
      int count = 0;
      final ColumnDictionary[] snapshot = columns;
      for (int column = 0; column < snapshot.length; column++) {
        if (COLUMNS.getAcquire(snapshot, column) != null) {
          count++;
        }
      }
      return count;
    }

    /**
     * Release every column's tables. The dictionaries stay in place so {@link #entryCount} keeps
     * answering the sealed count; what they held is gone, and a mint into one of them is refused.
     */
    synchronized void release() {
      released = true;
      final ColumnDictionary[] snapshot = columns;
      for (final ColumnDictionary dictionary : snapshot) {
        if (dictionary != null) {
          dictionary.release();
        }
      }
    }
  }

  /**
   * One {@code (segment, column)} dictionary: value bytes to a 1-based id, minted once per distinct
   * value. Concurrent because the flush pool encodes several pages of one segment at a time; the hit
   * path is lock-free and allocation-free, the miss path is serialised and rare (once per distinct
   * value per segment, not per row). A table word is {@code (hash << 32) | id}: the hash's low bits
   * place it, its high bits discriminate a probe chain, and {@code id} is never 0, so a word is never
   * 0 and 0 can mean empty.
   *
   * <h2>Memory ordering</h2>
   *
   * A slot is published with a release store AFTER {@code size} and the value it points at, so a
   * reader whose acquire load returns the slot sees a count covering the id and the bytes behind it.
   * Growing either array publishes the grown array through its volatile field BEFORE the slot that
   * needs it, and a grown array holds a copy of everything published before it, so a reader may see
   * a newer array than the writer used, never an older one. A reader probing an OLD table after a
   * rehash can miss a value inserted only into the new one; it then takes the miss path, which
   * re-probes the current table under the lock — a wasted lock, never a duplicate id.
   *
   * <h2>Release</h2>
   *
   * {@link #release} drops both arrays. A probe racing it may read any mix of the old and the new
   * arrays — the two volatile stores are not ordered against a reader's two loads — so the lock-free
   * path makes NO decision about release: an id whose value slot is gone is treated as a miss, and
   * every miss reaches {@link #mint}, which reads {@code released} under the lock and refuses. The
   * refusal is therefore certain, whichever interleaving the reader saw.
   */
  static final class ColumnDictionary {
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final LongHashFunction HASH = LongHashFunction.xx3();
    private static final int INITIAL_SLOTS = 1 << 10;
    private static final int INITIAL_VALUES = 1 << 9;
    /**
     * Ids are ints and the lane packs them by width; a segment's byte budget bounds it far below this
     * (64 MiB of distinct bytes is at most 2^26 values of one byte), and the table at load factor one
     * half still fits an array at this size.
     */
    static final int MAX_ENTRIES = 1 << 28;
    /**
     * The table of a released dictionary: one empty slot, so every probe misses and reaches
     * {@link #mint}, which refuses under the lock. The hit path pays nothing for the release check
     * because a released dictionary has no hits. Never written: {@code mint} throws first.
     */
    private static final long[] RELEASED_SLOTS = new long[1];
    /** The released dictionary's value array: empty, so every probe misses. */
    private static final byte[][] RELEASED_VALUES = new byte[0][];

    static {
      // The probe masks with (length - 1) and the rehash doubles: both need powers of two.
      if (Integer.bitCount(INITIAL_SLOTS) != 1 || Integer.bitCount(INITIAL_VALUES) != 1
          || Integer.bitCount(MAX_ENTRIES) != 1) {
        throw new IllegalStateException("the mint table's sizes must be powers of two");
      }
    }

    /** Open-addressing table: {@code (hash << 32) | id}, {@code 0} for empty. Replaced on rehash. */
    private volatile long[] slots = new long[INITIAL_SLOTS];
    /** Value bytes at {@code id - 1}. Replaced on growth, never shrunk. */
    private volatile byte[][] valueById = new byte[INITIAL_VALUES][];
    /** Ids issued so far; written before the slot that makes the id visible. */
    private volatile int size;
    private final LongAdder mintedBytes;
    /** Set by {@link #release} and read by {@link #mint} and {@link #valuesById}, all under the lock. */
    private boolean released;

    ColumnDictionary(final LongAdder mintedBytes) {
      this.mintedBytes = mintedBytes;
    }

    int idOf(final byte[] value, final int offset, final int length) {
      final int hash = hash(value, offset, length);
      final long[] table = slots;
      final int mask = table.length - 1;
      int slot = hash & mask;
      while (true) {
        final long word = (long) SLOTS.getAcquire(table, slot);
        if (word == 0L) {
          break; // not in this snapshot: mint, which re-probes the current table under the lock
        }
        if ((int) (word >>> 32) == hash && matches((int) word, value, offset, length)) {
          return (int) word;
        }
        slot = (slot + 1) & mask;
      }
      return mint(value, offset, length, hash);
    }

    /**
     * The 32-bit hash a word carries: both halves of xxh3 folded, so the word alone yields the slot in
     * any table size (a rehash re-derives every slot from the word, recomputing nothing) and the bits
     * above the mask still discriminate colliding chains.
     */
    static int hash(final byte[] value, final int offset, final int length) {
      final long hash = HASH.hashBytes(value, offset, length);
      return (int) (hash ^ (hash >>> 32));
    }

    /**
     * Whether id {@code id}'s stored bytes equal the slice. The slot was published after the bytes,
     * so a value slot that is GONE means the dictionary was released between the reader's two loads:
     * that is a miss, not an error — the miss path refuses under the lock, where release is decided.
     */
    boolean matches(final int id, final byte[] value, final int offset, final int length) {
      final byte[][] values = valueById;
      if (id > values.length) {
        return false;
      }
      final byte[] stored = values[id - 1];
      if (stored == null) {
        // Published slots point at published bytes, by the ordering above; a null here is a broken
        // invariant, and probing on would mint the value a SECOND id.
        throw new IllegalStateException("id " + id + " is published without its value");
      }
      return stored.length == length && Arrays.equals(stored, 0, length, value, offset, offset + length);
    }

    private synchronized int mint(final byte[] value, final int offset, final int length, final int hash) {
      long[] table = slots;
      int mask = table.length - 1;
      int slot = hash & mask;
      while (true) {
        final long word = table[slot]; // writers are serialised by this lock; a plain read is current
        if (word == 0L) {
          break;
        }
        if ((int) (word >>> 32) == hash && matches((int) word, value, offset, length)) {
          return (int) word; // minted by another encoder between our probe and our lock
        }
        slot = (slot + 1) & mask;
      }
      if (released) {
        throw new IllegalStateException("the segment dictionary was sealed and released; no value may be minted into it"
            + " any more — a page of a sealed segment was encoded after its seal");
      }
      final int issued = size;
      if (issued >= MAX_ENTRIES) {
        throw new IllegalStateException("a segment dictionary holds at most " + MAX_ENTRIES + " distinct values");
      }
      final int id = issued + 1;
      byte[][] values = valueById;
      if (id > values.length) {
        values = Arrays.copyOf(values, values.length << 1);
        valueById = values; // published before the slot that needs it
      }
      values[id - 1] = Arrays.copyOfRange(value, offset, offset + length);
      mintedBytes.add(length);
      size = id; // before the slot: a reader that sees the id sees a count covering it
      if ((id << 1) > table.length) {
        // Load factor one half, so linear probing always finds an empty slot. The rehash inserts every
        // existing word into a fresh array with plain stores and publishes the array once, and the
        // new id goes into the fresh array before it is published.
        table = rehash(table, table.length << 1);
        mask = table.length - 1;
        slot = hash & mask;
        while (table[slot] != 0L) {
          slot = (slot + 1) & mask;
        }
        table[slot] = word(hash, id);
        slots = table;
      } else {
        SLOTS.setRelease(table, slot, word(hash, id));
      }
      return id;
    }

    private static long word(final int hash, final int id) {
      return ((long) hash << 32) | id;
    }

    /** Every word of {@code table} in a fresh table of {@code capacity} slots, placed by its own hash. */
    private static long[] rehash(final long[] table, final int capacity) {
      final long[] grown = new long[capacity];
      final int mask = capacity - 1;
      for (final long word : table) {
        if (word == 0L) {
          continue;
        }
        int slot = (int) (word >>> 32) & mask;
        while (grown[slot] != 0L) {
          slot = (slot + 1) & mask;
        }
        grown[slot] = word;
      }
      return grown;
    }

    /**
     * Ids issued so far — from the counter, never from the table. The counter can only ever
     * over-report, by an id whose slot is still being installed, and a width one bit too wide costs
     * nothing; a count BELOW an issued id would write that id truncated.
     */
    int size() {
      return size;
    }

    /** Slots in the current table (test observability). */
    int slotCount() {
      return slots.length;
    }

    /** Value slots currently resident (test observability); {@code 0} once released. */
    int retainedValueSlots() {
      return valueById.length;
    }

    /**
     * The values in id order, a fresh array whose ELEMENTS are the dictionary's own byte arrays; call
     * once no encoder can still mint into it.
     */
    synchronized byte[][] valuesById() {
      if (released) {
        throw new IllegalStateException("the segment dictionary was released; its values must be read before that");
      }
      return Arrays.copyOf(valueById, size);
    }

    /**
     * Forget the values and the table: the dictionary is persisted. {@link #size} stays, so the
     * sealed count is still answered; every probe from now on misses — the table is empty and the
     * value slots are gone — and reaches {@link #mint}, which refuses under this lock. The two stores
     * are not ordered against a concurrent probe's loads and need not be: whichever it sees, a miss
     * is the worst it can conclude.
     */
    synchronized void release() {
      released = true;
      valueById = RELEASED_VALUES;
      slots = RELEASED_SLOTS;
    }
  }

  /**
   * The per-page resolver: every answer is its segment's, whatever the writer is doing now.
   *
   * <h2>Why the per-tag cache below can be plain fields</h2>
   *
   * One page is encoded by one thread at a time, and a view belongs to exactly one page. Each
   * {@link #adopt} mints a fresh view, so two pages never share one; the flush pool serializes an
   * adopted leaf IN PLACE (the same instance, on one pool thread) and deep-copies anything else,
   * and a deep copy carries no resolver at all. A page written in two flush epochs is encoded twice,
   * by possibly different pool threads — but the epochs are separated by the window join and the
   * writer's own sequential pass, which orders the first encode's writes before the second's reads.
   *
   * <h2>One snapshot per tag run</h2>
   *
   * The encoder resolves a tag's values in a run — {@link #hasDictionary}, then
   * {@link #dictionaryKey}, then {@link #idOf} per value, then {@link #dictionaryEntryCount} — and
   * derives the page's id width from the count at the end. Every answer of a run must come from ONE
   * dictionary: a count from a different column than the ids would either fail the encode or record
   * a bound that does not cover the ids. So {@link #hasDictionary}, the run's opener (the interface
   * asks it once per tag per page), resolves the tag against the tag map as published NOW and the
   * three later answers come from that resolution, however often the map is republished meanwhile.
   * A republished map is picked up at the next run, which is also what lets a page re-encoded in a
   * later flush epoch see a tag that has since become contested and keep its bytes.
   *
   * <p>
   * The COLUMN is what a run pins; the column's dictionary is taken at the run's first value and
   * cached for the rest of it. A tag whose page holds no value therefore creates nothing, and the
   * one synchronised {@code column} call is paid once per tag run rather than once per value.
   * </p>
   */
  public final class SegmentView implements GlobalStringDictionaries {
    private static final int NO_TAG = -1;

    private final int segment;
    private final SegmentState state;
    /** The tag last resolved, {@link #NO_TAG} when the last resolution found no column. */
    private int cachedTag = NO_TAG;
    /** The column {@link #cachedTag} resolved to; the run's single snapshot. */
    private int cachedColumn = TagColumnMap.NO_COLUMN;
    /** {@link #cachedColumn}'s dictionary once the run has minted; {@code null} until then. */
    private @Nullable ColumnDictionary cachedDictionary;

    SegmentView(final int segment, final SegmentState state) {
      this.segment = segment;
      this.state = state;
    }

    /** The segment this view answers for. */
    public int segment() {
      return segment;
    }

    @Override
    public boolean hasDictionary(final int tag) {
      return resolve(tag) >= 0;
    }

    @Override
    public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
      return false; // encode-direction only; reading is the read resolver's job
    }

    @Override
    public int maxValueBytes() {
      // What the seal can persist. The encoder asks once per tag run and leaves a tag holding a
      // longer value as bytes without probing, so nothing is minted for a tag that keeps its bytes.
      return GlobalValueDictionaryWriter.MAX_VALUE_BYTES;
    }

    @Override
    public int idOf(final int tag, final byte[] value, final int offset, final int length) {
      requireNonNull(value, "value must not be null");
      if (offset < 0 || length < 0 || offset > value.length - length) {
        throw new IndexOutOfBoundsException("offset " + offset + " length " + length + " over " + value.length);
      }
      if (length > GlobalValueDictionaryWriter.MAX_VALUE_BYTES) {
        // A caller that did not pre-check {@link #maxValueBytes}. The seal could not persist it, and
        // the encoder treats an absent id as "this tag keeps its bytes on this page" — so the page
        // stays readable and the load does not die at the seal.
        return ID_ABSENT;
      }
      final int column = columnFor(tag);
      if (column < 0) {
        return ID_ABSENT; // not a projected column: the page keeps its bytes
      }
      ColumnDictionary dictionary = cachedDictionary;
      if (dictionary == null) {
        dictionary = state.column(column); // the run's first value: one synchronised call per tag run
        cachedDictionary = dictionary;
      }
      return dictionary.idOf(value, offset, length);
    }

    @Override
    public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
        final int id) {
      return null; // encode-direction only
    }

    @Override
    public long dictionaryKey(final int tag) {
      // segment + 1, because 0 is the encoder's "this tag has no dictionary" sentinel and segment 0
      // is a real segment. The read side subtracts it back; nothing else interprets the value.
      return columnFor(tag) < 0
          ? 0L
          : segment + 1L;
    }

    @Override
    public int dictionaryEntryCount(final int tag) {
      final int column = columnFor(tag);
      if (column < 0) {
        return 0;
      }
      // The run's own dictionary when it minted. The other arm is defensive: the encoder always
      // calls idOf before the count (and returns early for a tag with no values), so a run that
      // reaches here has a dictionary. It answers with the column's own count rather than 0 for a
      // caller that does not — never a dictionary of a different column, which is the bound that
      // matters.
      final ColumnDictionary dictionary = cachedDictionary == null
          ? state.dictionaryAt(column)
          : cachedDictionary;
      return dictionary == null
          ? 0
          : dictionary.size();
    }

    /** The run's column for {@code tag}: the cached resolution, or a fresh one for a new tag. */
    private int columnFor(final int tag) {
      return cachedTag == tag
          ? cachedColumn
          : resolve(tag);
    }

    /**
     * Resolve {@code tag} against the tag map as published now and make that the run's snapshot. A
     * tag without a column leaves no snapshot, so a later call for it resolves again — the map may
     * have gained the tag by then, and there is no run to keep consistent when nothing was answered.
     */
    private int resolve(final int tag) {
      final Int2IntMap tags = columnByTag;
      final int column = tags.containsKey(tag)
          ? tags.get(tag)
          : TagColumnMap.NO_COLUMN;
      cachedDictionary = null; // a new run: the previous run's dictionary must not answer for it
      if (column < 0) {
        cachedTag = NO_TAG;
        cachedColumn = TagColumnMap.NO_COLUMN;
        return TagColumnMap.NO_COLUMN;
      }
      cachedTag = tag;
      cachedColumn = column;
      return column;
    }
  }

  /** An empty tag map, for a load that has resolved no path class yet. */
  public static Int2IntMap noTags() {
    return Int2IntMaps.EMPTY_MAP;
  }
}
