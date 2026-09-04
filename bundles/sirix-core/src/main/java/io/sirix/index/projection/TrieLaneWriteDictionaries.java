/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * The ENCODE-direction resolver for the trie lane: value bytes to a prebuilt dictionary's ids.
 *
 * <h2>Why this is not {@link TrieLaneDictionaries}</h2>
 *
 * That one answers the DECODE direction and holds one transaction's reader. This runs somewhere
 * very different: {@code PageKind.buildRegionTable} builds a record page's string region inside
 * {@code serializeSnapshotWindowAsync}'s {@code runAsync} plus parallel {@code forEach}, so
 * {@link #idOf} is called concurrently from many ForkJoinPool threads. A resolver that walked the
 * trie through the writer from there would be the same hazard that got the writer-side resolution
 * front deleted: concurrent mutation of a reader declared single-threaded.
 *
 * <h2>Thread confinement is the safety argument, not a detail</h2>
 *
 * Every piece of mutable state lives behind a {@link ThreadLocal}: the probe memo and the snapshot
 * reader it probes through. Nothing is shared between flush threads except immutable maps and the
 * committed dictionary itself.
 *
 * <p>
 * <b>This is what makes the memo safe, and it is the property a "share the cache, it will hit more"
 * change would destroy.</b> {@link GlobalValueDictionaryHotCache} stores a slot's hash, its length,
 * its bytes and its id in four separate arrays with no synchronisation and no volatiles. Shared
 * across threads, a reader can match one thread's HASH against another thread's ID and get back a
 * valid-looking id for a different value — not a crash and not a torn field, but a coherent wrong
 * answer that flows straight into the id lane. One memo per thread makes that unreachable rather
 * than unlikely.
 * </p>
 *
 * <h2>Why a probe and not a value table</h2>
 *
 * The alternative is retaining the pre-pass's sorted values and binary-searching them, which is
 * thread-safe by immutability and costs roughly 18.3M x 184 B ≈ 4-5 GB of heap for URL alone, times
 * every converted column, held for the whole load — on a load that has historically run its arenas
 * to the edge. A probe holds no value set at all. {@code PrebuiltGlobalDictionary}'s javadoc prices
 * this exact shape: paid once per per-leaf DICTIONARY ENTRY and memoised across leaves, ~600k
 * probes for URL and Title together at 1M. That matches the measured leaf shape — a leaf is only
 * ~9.7 ClickBench rows, so ~6 distinct values per tag per leaf.
 *
 * <h2>What it deliberately cannot do</h2>
 *
 * The DECODE direction. {@link #valueOf} and {@link #accepts} always refuse: an encoder never turns
 * an id back into bytes, and answering would invite a caller to read pages through a resolver built
 * for writing. Reading is {@link TrieLaneDictionaries}' job, from a reader positioned at the
 * reading transaction's revision.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TrieLaneWriteDictionaries implements GlobalStringDictionaries, AutoCloseable {

  /** One-off write-side diagnostic for the slot-0 truncation hunt. */
  private static final boolean TRIE_LANE_DIAG = Boolean.getBoolean("sirix.trieLaneDiag");

  private static final Logger LOGGER = LoggerFactory.getLogger(TrieLaneWriteDictionaries.class);

  /** Generous: a stuck flush thread is what this is meant to surface, not a slow one. */
  private static final long AWAIT_IN_FLIGHT_NANOS = 60L * 1_000_000_000L;

  private final ResourceSession<?, ?> resourceSession;

  /** The revision the pre-pass committed its dictionaries in; every probe reads exactly this one. */
  private final int dictionaryRevision;

  /** Column to the dictionary header key the pre-pass published. Immutable. */
  private final Int2LongMap headerKeyByColumn;

  /** Column to its dictionary's entry count, read once from each header. Immutable. */
  private final Int2IntMap entryCountByColumn;

  /**
   * Tag (path node key) to column, republished as a whole immutable map whenever the load resolves
   * new path classes.
   *
   * <p>
   * A field path only gets a path class when its first occurrence is shredded, so this map GROWS
   * during a load — and the side that can see the path summary is the single-threaded extraction
   * side, not the flush lane. So the flush lane never reads the path summary: it reads this
   * reference, and the extraction side publishes a NEW map rather than mutating the old one. A reader
   * therefore sees some consistent snapshot, never a map mid-update, and a tag that is not in the
   * snapshot yet simply keeps its bytes on that page — which costs storage, never correctness.
   * </p>
   */
  private volatile Int2IntMap columnByTag;

  /** Every per-thread probe handed out, so {@link #close()} can reach them all. */
  private final Queue<ThreadProbes> issued = new ConcurrentLinkedQueue<>();

  /**
   * One probe state per flush thread. <b>This ThreadLocal IS the correctness argument, not a
   * performance tactic.</b>
   *
   * <p>
   * {@link GlobalValueDictionaryHotCache} keeps a slot's hash, length, bytes and id in four separate
   * unsynchronised arrays. Shared across the flush lane's ForkJoinPool threads, a reader could match
   * one thread's HASH against another thread's ID and get back a valid-looking id for a different
   * value — a coherent wrong answer written straight into the id lane, which no exception and no
   * value-comparing test would catch. Confinement makes that unreachable rather than unlikely.
   * </p>
   *
   * <p>
   * So an optimisation of the form "share one cache, it will hit more often" reintroduces a silent
   * data-corruption bug. If the hit rate ever needs improving, make the per-thread memo bigger.
   * </p>
   */
  private final ThreadLocal<ThreadProbes> probes = ThreadLocal.withInitial(() -> {
    final ThreadProbes created = new ThreadProbes();
    issued.add(created);
    return created;
  });

  private final LongAdder probeCount = new LongAdder();

  private final LongAdder memoHits = new LongAdder();

  private final LongAdder absentValues = new LongAdder();

  /**
   * Probes refused because {@link #close()} had already run — the DRAIN-ORDERING counter.
   *
   * <p>
   * Separate from {@link #absentValues} on purpose, and the separation is the whole point. Both make
   * a page keep its bytes, so one shared counter would let two completely different faults look
   * alike: "the pre-pass and the load disagree about the value set" and "a flush thread was still
   * encoding after the load released the lane". The second is a lifecycle bug, and it would silently
   * under-convert exactly the LATE-flushed pages — the census would show less shrink than expected
   * and nothing would say why.
   * </p>
   *
   * <p>
   * Counted apart, and the converted arm's gate asserts BOTH are zero. That turns "is the flush
   * executor really drained before close?" from a question answered by reading executor code into one
   * answered by the run.
   * </p>
   */
  private final LongAdder closedProbes = new LongAdder();

  private volatile boolean closed;

  /**
   * Probes currently INSIDE {@link #idOf}, so {@link #close()} can wait for them.
   *
   * <p>
   * The {@code closed} flag alone is a TOCTOU, and the path that makes it bite is documented rather
   * than hypothetical: {@code ProjectionIndexChangeListener} calls {@code load.abort()} on a
   * global-dictionary budget breach DURING the load — its own operator message says "the load
   * completes; the projection is STALE" — so the import keeps running and the writer keeps flushing
   * record pages, which is exactly where {@code idOf} runs. A thread already past the flag when
   * {@code close()} fires would probe through a reader being closed underneath it: a use-after-close,
   * not a wrong value, and so invisible to every correctness witness.
   * </p>
   *
   * <p>
   * The order is the Dekker one and it matters: {@code idOf} increments BEFORE reading
   * {@code closed}, {@code close()} writes {@code closed} BEFORE reading this. Both fields carry the
   * necessary happens-before, so no interleaving lets a probe start after close and no probe is
   * missed.
   * </p>
   */
  private final AtomicInteger inFlight = new AtomicInteger();

  /**
   * @param resourceSession the session the load runs against
   * @param dictionaryRevision the revision the pre-pass committed the dictionaries in
   * @param headerKeyByColumn column to header key, from {@code sirix.projection.globalDict.prebuilt}
   * @param entryCountByColumn column to the dictionary's entry count
   */
  public TrieLaneWriteDictionaries(final ResourceSession<?, ?> resourceSession, final int dictionaryRevision,
      final Int2LongMap headerKeyByColumn, final Int2IntMap entryCountByColumn) {
    this.resourceSession = Objects.requireNonNull(resourceSession, "resourceSession");
    if (dictionaryRevision < 0) {
      throw new IllegalArgumentException("dictionary revision must not be negative: " + dictionaryRevision);
    }
    this.dictionaryRevision = dictionaryRevision;
    this.headerKeyByColumn = Objects.requireNonNull(headerKeyByColumn, "headerKeyByColumn");
    this.entryCountByColumn = Objects.requireNonNull(entryCountByColumn, "entryCountByColumn");
    this.columnByTag = new Int2IntOpenHashMap();
  }

  /**
   * Bind the dictionaries {@code -Dsirix.projection.globalDict.prebuilt} names, or {@code null}.
   *
   * <p>
   * Runs once, single-threaded, at load start. It reads each named dictionary's header to learn the
   * entry count the pages will record, which is also the check that the dictionary is actually there:
   * a header that cannot be read, or one whose directory is incomplete, refuses the whole binding
   * rather than letting pages name a dictionary no reader can resolve.
   * </p>
   *
   * <p>
   * Completeness is required; collation order deliberately is NOT. The lane needs dense, append-only,
   * stable ids and an anchor naming the dictionary they came from — rank order is a SERVING property
   * (the ordered arms, the prefix probe), not a correctness property of storing ids in a record page.
   * Requiring it here would refuse a dictionary that works perfectly.
   * </p>
   *
   * @param resourceSession the session the load runs against
   * @param dictionaryRevision the revision the pre-pass committed into
   * @param columnCount the projection's column count, for the anchor parser's bounds check
   * @return the resolver, or {@code null} when no prebuilt anchors are configured
   */
  public static @Nullable TrieLaneWriteDictionaries bindConfigured(final ResourceSession<?, ?> resourceSession,
      final int dictionaryRevision, final int columnCount) {
    final long[] anchors = ProjectionIndexBuilder.configuredPrebuiltAnchors(columnCount);
    if (anchors == null) {
      return null;
    }
    final Int2LongMap headerKeys = new Int2LongOpenHashMap();
    final Int2IntMap entryCounts = new Int2IntOpenHashMap();
    try (final NodeReadOnlyTrx rtx = resourceSession.beginNodeReadOnlyTrx(dictionaryRevision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      for (int column = 0; column < anchors.length; column++) {
        if (anchors[column] <= 0L) {
          continue;
        }
        final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(anchors[column], reader);
        if (header == null || !header.isDirectoryComplete()) {
          throw new IllegalStateException("trie lane: prebuilt dictionary " + anchors[column] + " for column " + column
              + " is unreadable or incomplete at revision " + dictionaryRevision
              + "; record pages must not name a dictionary no reader can resolve");
        }
        headerKeys.put(column, anchors[column]);
        entryCounts.put(column, header.getEntryCount());
      }
    }
    if (headerKeys.isEmpty()) {
      return null;
    }
    return new TrieLaneWriteDictionaries(resourceSession, dictionaryRevision, headerKeys, entryCounts);
  }

  /**
   * Publish a new tag-to-column snapshot, from the single-threaded extraction side.
   *
   * <p>
   * A whole new map, never an edit of the live one: the flush lane reads this reference without any
   * lock, and a map being rehashed underneath it would be a data race on fastutil internals. Building
   * a fresh map costs one allocation per refresh against a load's worth of lock-free reads.
   * </p>
   *
   * @param pathClassToColumn tag to column for every field path that has a path class so far
   */
  public void publishTags(final Int2IntMap pathClassToColumn) {
    this.columnByTag = new Int2IntOpenHashMap(Objects.requireNonNull(pathClassToColumn, "pathClassToColumn"));
  }

  @Override
  public boolean hasDictionary(final int tag) {
    return columnOf(tag) >= 0;
  }

  @Override
  public int idOf(final int tag, final byte[] value, final int offset, final int length) {
    Objects.checkFromIndexSize(offset, length, value.length);
    final int column = columnOf(tag);
    if (column < 0) {
      return ID_ABSENT;
    }
    // Announce BEFORE testing the flag. Reversing these two lines reintroduces the use-after-close:
    // close() would see zero in flight and free this thread's reader while it is about to probe.
    inFlight.incrementAndGet();
    try {
      return probeWhileOpen(column, value, offset, length);
    } finally {
      inFlight.decrementAndGet();
    }
  }

  private int probeWhileOpen(final int column, final byte[] value, final int offset, final int length) {
    if (closed) {
      // A probe that arrived after the lane was released. Degrading keeps the page readable -- it
      // stores bytes -- but this is NOT the same event as a value the dictionary does not hold, so it
      // does not share that counter. See closedProbes.
      closedProbes.increment();
      return ID_ABSENT;
    }
    final long headerKey = headerKeyByColumn.get(column);
    final ThreadProbes threadProbes = probes.get();
    final int memoised = threadProbes.memo(column).find(value, offset, length);
    if (memoised > 0) {
      memoHits.increment();
      return memoised;
    }
    probeCount.increment();
    if (TRIE_LANE_DIAG && probeCount.sum() <= 3) {
      // [DIAG] the first probes of the load: which thread, which length, and whether the reader
      // is being opened on this probe -- the slot-0 truncation hunt.
      System.err.println("[trie-lane-diag] probe#" + probeCount.sum() + " thread=" + Thread.currentThread().getName()
          + " column=" + column + " len=" + length + " readerOpen=" + (threadProbes.trx != null));
    }
    final int id = GlobalValueDictionary.probe(headerKey, value, offset, length, threadProbes.reader());
    if (id <= 0) {
      // ABSENT and UNKNOWN both mean "this tag cannot be written as ids on this page" — the caller
      // keeps its bytes. Deliberately NOT an exception, unlike PrebuiltGlobalDictionary's build-side
      // contract: there a miss means the pre-pass and the build disagree about the value set and the
      // whole index would be wrong, whereas here the fallback is a page that stores its strings the
      // way every page did before this lever existed. Counted, so a systematic disagreement shows up
      // as a number rather than as a lever that quietly did nothing.
      absentValues.increment();
      return ID_ABSENT;
    }
    // Bounded memo: 64 slots of at most 512 bytes each, per thread. A miss or an eviction costs
    // another probe and never a wrong id — the memo is only ever consulted for an exact byte match,
    // so the worst an eviction can do is make the next lookup slower. Values longer than the slot
    // width are never memoised and are re-probed every time.
    threadProbes.memo(column).put(value, offset, length, id);
    return id;
  }

  @Override
  public long dictionaryKey(final int tag) {
    final int column = columnOf(tag);
    return column < 0
        ? 0L
        : headerKeyByColumn.get(column);
  }

  @Override
  public int dictionaryEntryCount(final int tag) {
    final int column = columnOf(tag);
    return column < 0
        ? 0
        : entryCountByColumn.get(column);
  }

  @Override
  public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    // The DECODE direction, and this resolver has no business answering it. A page is validated
    // against the dictionaries the READING transaction can see, at its own revision; answering here
    // would let a page be resolved against the writer's view of the world.
    return false;
  }

  @Override
  public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
      final int id) {
    return null;
  }

  /**
   * Probes taken, memo hits served, and values the dictionary did not hold.
   *
   * <p>
   * <b>{@code absent} must be ZERO on a converted arm, and the gate asserts it.</b> A miss keeps the
   * tag's bytes, which is the correct runtime contract — it is what every page did before this lever
   * existed, and a load must not abort over it. But the pre-pass built its dictionary from THIS
   * input, so a miss is evidence of a real disagreement: encoding drift, a normalisation difference
   * between the value extraction and the shredder, or a value the extraction never saw. The counter
   * exists to make that loud instead of letting the lane quietly convert less than it claims.
   * </p>
   *
   * <p>
   * The corollary matters for measurement: an arm with a non-zero absent count has under-converted
   * its pages, so its size is not this lever's size. Never report storage from such an arm.
   * </p>
   */
  public String describeCounters() {
    return "trie lane encode: probes=" + probeCount.sum() + " memoHits=" + memoHits.sum() + " absent="
        + absentValues.sum() + " afterClose=" + closedProbes.sum();
  }

  /** Probes refused after close -- non-zero means a flush thread outlived the lane's release. */
  public long closedProbeCount() {
    return closedProbes.sum();
  }

  public long probeCount() {
    return probeCount.sum();
  }

  public long memoHitCount() {
    return memoHits.sum();
  }

  public long absentValueCount() {
    return absentValues.sum();
  }

  /**
   * Close every reader this handed out.
   *
   * <p>
   * Called when the flush executor has drained, which is the only moment at which no flush thread can
   * still be inside {@link #idOf}. {@code closed} is set first so a late caller degrades to
   * {@link #ID_ABSENT} — keeping its bytes — rather than probing through a reader being closed.
   * </p>
   */
  @Override
  public void close() {
    closed = true;
    awaitInFlightProbes();
    ThreadProbes threadProbes;
    while ((threadProbes = issued.poll()) != null) {
      threadProbes.close();
    }
  }

  /**
   * Wait for every probe already inside {@link #idOf} to leave, then let the readers be closed.
   *
   * <p>
   * THROWS on expiry rather than proceeding, and the difference is the whole point: proceeding would
   * re-open the use-after-close this method exists to prevent, quietly, at exactly the moment
   * something is already wrong. A timeout here says a flush thread is stuck — which is a fact worth
   * failing a load for — while a silent proceed says nothing and corrupts.
   * </p>
   *
   * <p>
   * Cannot deadlock against the caller. {@code abort()} holds the {@code ProjectionBulkLoad} monitor
   * while calling this, and {@code idOf} touches only this class's own fields and
   * {@code GlobalValueDictionary.probe} — never that monitor — so a probe in flight always finishes
   * on its own. Checked rather than assumed.
   * </p>
   */
  private void awaitInFlightProbes() {
    final long deadline = System.nanoTime() + AWAIT_IN_FLIGHT_NANOS;
    while (inFlight.get() > 0) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("trie lane: " + inFlight.get() + " encode probes still in flight "
            + AWAIT_IN_FLIGHT_NANOS / 1_000_000_000L + "s after the lane was closed. A flush thread is stuck inside "
            + "idOf; closing its snapshot reader now would be a use-after-close, so this fails loudly instead.");
      }
      Thread.onSpinWait();
      try {
        Thread.sleep(1L);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("trie lane: interrupted while waiting for encode probes to drain", interrupted);
      }
    }
  }

  private int columnOf(final int tag) {
    final Int2IntMap tags = columnByTag;
    return tags.containsKey(tag)
        ? tags.get(tag)
        : -1;
  }

  /**
   * One flush thread's probe state: a snapshot reader and a memo per column.
   *
   * <p>
   * The reader is opened LAZILY, on the thread's first probe, because the flush pool's size is not
   * known here and a pool that never touches a converted page should open nothing. It is never handed
   * out, so it cannot escape the thread that made it.
   * </p>
   */
  private final class ThreadProbes implements AutoCloseable {

    private NodeReadOnlyTrx trx;

    private final Int2ObjectLikeCache memos = new Int2ObjectLikeCache();

    StorageEngineReader reader() {
      if (trx == null) {
        // A snapshot at the pre-pass's revision. The dictionaries were committed before the load
        // began and an append-only store never rewrites them, so this reader sees a fixed, complete
        // structure for the whole load and needs no coordination with the writer.
        //
        // beginNodeReadOnlyTrx takes NO session lock -- AbstractResourceSession documents removing
        // the per-session monitor precisely so N concurrent reader-opens run in parallel. Said
        // explicitly because the opposite belief invites reasoning about a lock ordering against the
        // committing writer that does not exist.
        //
        // The residual cost is real though: each reader holds an epoch ticket and a
        // storageEngineReaderMap entry for the load's life, so a converted load pins revisions x
        // flush threads. Bounded, but it is why close() must actually close these rather than
        // deferring them to writer teardown -- epoch-ticket pinning is the shape that exhausted the
        // arena on every 100M load once before.
        trx = resourceSession.beginNodeReadOnlyTrx(dictionaryRevision);
      }
      return trx.getStorageEngineReader();
    }

    GlobalValueDictionaryHotCache memo(final int column) {
      return memos.get(column);
    }

    @Override
    public void close() {
      final NodeReadOnlyTrx open = trx;
      trx = null;
      if (open != null && !open.isClosed()) {
        try {
          open.close();
        } catch (final RuntimeException failure) {
          LOGGER.debug("trie lane: closing an encode-side snapshot reader failed", failure);
        }
      }
    }
  }

  /** A tiny column-keyed holder; there are a handful of converted columns, never enough for a map. */
  private static final class Int2ObjectLikeCache {

    private int[] columns = new int[4];

    private GlobalValueDictionaryHotCache[] caches = new GlobalValueDictionaryHotCache[4];

    private int size;

    GlobalValueDictionaryHotCache get(final int column) {
      for (int i = 0; i < size; i++) {
        if (columns[i] == column) {
          return caches[i];
        }
      }
      if (size == columns.length) {
        final int[] grownColumns = new int[size << 1];
        System.arraycopy(columns, 0, grownColumns, 0, size);
        columns = grownColumns;
        final GlobalValueDictionaryHotCache[] grownCaches = new GlobalValueDictionaryHotCache[size << 1];
        System.arraycopy(caches, 0, grownCaches, 0, size);
        caches = grownCaches;
      }
      columns[size] = column;
      final GlobalValueDictionaryHotCache created = new GlobalValueDictionaryHotCache();
      caches[size] = created;
      size++;
      return created;
    }
  }
}
