package io.sirix.query.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.ShardedPageCache;
import io.sirix.settings.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Overlaps record-page materialization with consumption during a sequential array scan.
 *
 * <p><b>Why this exists.</b> A record-cache miss in SirixDB does not cost an I/O — it costs a
 * decode. {@code docs/COMPARISON_POSTGRES_BULK.md} §4.1 measures {@code deserializeSlottedPage} at
 * ~54 % of cold-scan CPU against ~3–5 % in {@code pread}, because the on-disk page is
 * column-oriented and elided while the in-memory page is a row-oriented heap: every miss transposes
 * the columns back into whole records. The OS page cache cannot help with that — it saves the read,
 * not the rebuild. Consequently a cold scan is ~97 % decode-bound (measured: 14.4 s cold against
 * 352 ms warm for the same {@code countAll}) and pins a single core while every other one idles.
 *
 * <p><b>What it does.</b> Decoding page <i>N+1</i> does not depend on page <i>N</i>, so the work is
 * embarrassingly parallel. As the cursor crosses a record-page boundary this prefetcher submits the
 * pages just beyond it to a shared worker pool; workers materialize them into the <em>global</em>
 * record-page cache, so the cursor finds them already resident.
 *
 * <p><b>Why it cannot change an answer.</b> A worker only performs
 * {@link JsonNodeReadOnlyTrx#moveTo(long)} on its own private read-only transaction, pinned to the
 * same revision as the scanning cursor, and publishes nothing but cache residency. A page that is
 * prefetched and never used costs work and no correctness; a page that is never prefetched is
 * loaded on demand exactly as before. Set {@code -Dsirix.scan.prefetch.threads=0} to disable.
 *
 * <p><b>Bounding.</b> Speculative work must not outrun the cache it fills: in a buffer-pressured
 * regime an unbounded read-ahead would evict pages before the cursor reached them, converting a
 * prefetch into a self-inflicted miss. In-flight loads are capped by a semaphore, and submission is
 * strictly best-effort ({@link Semaphore#tryAcquire()}, never {@code acquire()}) so a saturated
 * prefetcher skips ahead instead of blocking the consumer it exists to serve.
 */
final class RecordPagePrefetcher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordPagePrefetcher.class);

  /** {@code recordKey >> 10} is the DOCUMENT-index page key (NodeStorageEngineReader#pageKey). */
  private static final int PAGE_KEY_SHIFT = Constants.INP_REFERENCE_COUNT_EXPONENT;

  /**
   * Worker threads. Default is half the available cores: the scanning thread needs one, and going
   * wider mostly buys queueing against a decode that is already memory-bandwidth heavy. {@code 0}
   * disables prefetching and restores the plain on-demand load path.
   */
  private static final int THREADS = Math.max(0, Integer.getInteger("sirix.scan.prefetch.threads",
      Math.max(1, Runtime.getRuntime().availableProcessors() / 2)));

  /**
   * Pages to keep in flight ahead of the cursor. Enough per worker to cover the jitter of
   * individual decodes, without running so far ahead that a small cache evicts the result first.
   */
  private static final int WINDOW = Math.max(1, Integer.getInteger("sirix.scan.prefetch.window",
      Math.max(16, THREADS * 4)));

  /**
   * Record pages a WALK must span before read-ahead is worth starting for it.
   *
   * <p>Measured against the walk, never against the resource. The resource's page count says only
   * how much data exists, and gating on it admitted a prefetcher for any sequential step in a large
   * store — including the three-element {@code $movie.genres[]} nested arrays a scan of that store
   * visits by the million. Each one submitted a full window of speculative decodes for a walk that
   * ended two elements later, and then had to dispose of them.
   */
  private static final long MIN_PAGES = Long.getLong("sirix.scan.prefetch.minPages", 8L);

  /** {@code -Dsirix.scan.prefetch.debug=true} reports submit/complete counts when a scan ends. */
  private static final boolean DEBUG = Boolean.getBoolean("sirix.scan.prefetch.debug");

  /** Page boundaries between hit-rate probes. Small enough to react, large enough to be free. */
  private static final int PROBE_PAGES = Math.max(8, Integer.getInteger("sirix.scan.prefetch.probePages", 64));

  /**
   * One pool for the whole process, not one per scan: a scan is a transient object and paying
   * thread start-up per array would swamp the decode it is meant to hide. Threads are daemons and
   * time out when idle, so an application that never scans carries nothing.
   */
  private static final ThreadPoolExecutor POOL = newPool();

  private static ThreadPoolExecutor newPool() {
    if (THREADS == 0) {
      return null;
    }
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(THREADS, THREADS, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(Math.max(WINDOW, THREADS) * 4),
        runnable -> {
          final Thread thread = new Thread(runnable, "sirix-scan-prefetch");
          thread.setDaemon(true);
          return thread;
        },
        // Abort, not discard: the task itself releases the in-flight permit in its finally block,
        // so a silently dropped task would leak a permit and the read-ahead would throttle itself
        // to a standstill. Aborting surfaces the rejection to submit(), which releases the permit
        // and reports back-pressure. (The queue is sized well above the window, so this is a
        // safety net rather than an expected path.)
        new ThreadPoolExecutor.AbortPolicy());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  private final JsonResourceSession session;
  private final int revision;
  private final long maxPageKey;
  private final Semaphore inFlight = new Semaphore(WINDOW);

  /**
   * One read-only transaction per worker thread, for this scan. A transaction is not thread-safe,
   * so it must never be shared; keying by thread gives each worker its own cursor while keeping
   * them all reachable for {@link #close()}.
   */
  private final Map<Thread, JsonNodeReadOnlyTrx> workerTrxs = new ConcurrentHashMap<>();

  /** Highest page key already submitted. Only touched by the scanning thread. */
  private long submittedUpTo = -1L;

  /** Page boundaries crossed since the last hit-rate probe. Scanning thread only. */
  private int pagesSinceProbe;

  /** Global miss count at the last probe, or {@code -1} before the first one. */
  private long missesAtLastProbe = -1L;

  /**
   * Set when the last probe showed the scan being served from cache. A warm scan needs no
   * read-ahead, and running one anyway is not free: the workers still descend the trie and take
   * guards per page, which measurably slowed the fully-cached regime (countAll 352 ms → 414 ms)
   * before this back-off existed. Re-probed continuously, so a scan that runs off the end of the
   * cache resumes prefetching within one probe interval.
   */
  private boolean suspended;

  private final AtomicLong submitted = new AtomicLong();
  private final AtomicLong completed = new AtomicLong();

  /**
   * Tasks submitted and not yet finished, queued ones included. Whoever drives this to zero after
   * {@link #closed} is set owns the worker-transaction release — see {@link #close()}.
   */
  private final AtomicInteger outstanding = new AtomicInteger();

  /** Guards the release so the closing thread and the last worker cannot both perform it. */
  private final AtomicBoolean workerTrxsReleased = new AtomicBoolean();

  private volatile boolean closed;

  private RecordPagePrefetcher(final JsonNodeReadOnlyTrx rtx) {
    this.session = rtx.getResourceSession();
    this.revision = rtx.getRevisionNumber();
    this.maxPageKey = rtx.getMaxNodeKey() >> PAGE_KEY_SHIFT;
  }

  /** Whether scan prefetching is switched on at all. */
  static boolean isEnabled() {
    return THREADS > 0 && POOL != null;
  }

  /**
   * Creates a prefetcher for the walk about to run, or {@code null} when prefetching is disabled,
   * the walk is too short to amortize it, or the transaction cannot be interrogated.
   *
   * <p><b>The admission test measures the walk.</b> {@code nodeStride} is the observed node-key
   * distance between the two elements already visited and {@code remainingElements} is how many are
   * left, so their product is what the rest of this walk will actually traverse — the only quantity
   * that says whether read-ahead can pay for itself. A walk that will not cross {@link #MIN_PAGES}
   * record-page boundaries is refused outright, so it never opens a worker transaction, never
   * submits a speculative decode, and has nothing to dispose of when it ends three elements later.
   *
   * @param rtx               the scanning cursor, positioned anywhere; only its revision, resource
   *                          and node-key bound are read
   * @param fromNodeKey       node key the walk continues from
   * @param nodeStride        measured node-key distance between consecutive elements; clamped to at
   *                          least one by the caller
   * @param remainingElements elements still to visit after {@code fromNodeKey}; {@code <= 0}
   *                          declines, which is also how an unknown element count declines
   */
  static RecordPagePrefetcher createOrNull(final JsonNodeReadOnlyTrx rtx, final long fromNodeKey,
      final long nodeStride, final long remainingElements) {
    if (!isEnabled() || rtx == null || remainingElements <= 0L || nodeStride <= 0L) {
      return null;
    }
    try {
      final long maxNodeKey = rtx.getMaxNodeKey();
      final long startPageKey = fromNodeKey >> PAGE_KEY_SHIFT;
      // The walk cannot run past the resource, so the resource bound caps the estimate rather than
      // gating it — a long walk over a small resource is still short in pages.
      final long walkEndNodeKey = Math.min(maxNodeKey, saturatingAdd(fromNodeKey,
                                                                    saturatingMultiply(nodeStride,
                                                                                       remainingElements)));
      if (((walkEndNodeKey >> PAGE_KEY_SHIFT) - startPageKey) < MIN_PAGES) {
        return null;
      }
      return new RecordPagePrefetcher(rtx);
    } catch (final RuntimeException e) {
      // Prefetching is an optimization; never let it break a scan that would otherwise succeed.
      LOGGER.debug("Scan prefetching unavailable, falling back to on-demand loads", e);
      return null;
    }
  }

  /** {@code a * b}, saturating at {@link Long#MAX_VALUE}; both operands are positive here. */
  private static long saturatingMultiply(final long a, final long b) {
    final long product = a * b;
    // Long.MAX_VALUE / b is one division on a path taken once per walk, not per element.
    return a > Long.MAX_VALUE / b ? Long.MAX_VALUE : product;
  }

  /** {@code a + b}, saturating at {@link Long#MAX_VALUE}; both operands are non-negative here. */
  private static long saturatingAdd(final long a, final long b) {
    final long sum = a + b;
    return sum < 0L ? Long.MAX_VALUE : sum;
  }

  /**
   * Tells the prefetcher the cursor has reached {@code nodeKey} and submits the pages ahead of it.
   * Allocation-free, and a single compare on the common path (still inside the previous page).
   */
  void advanceTo(final long nodeKey) {
    if (closed) {
      return;
    }
    final long pageKey = nodeKey >> PAGE_KEY_SHIFT;
    final long target = Math.min(pageKey + WINDOW, maxPageKey);
    if (target <= submittedUpTo) {
      return;
    }
    // One probe per PROBE_PAGES boundaries crossed; `target > submittedUpTo` means this call
    // crossed into new pages, so this counts page boundaries rather than elements.
    if (++pagesSinceProbe >= PROBE_PAGES) {
      probeHitRate();
    }
    if (suspended) {
      // Still advance the cursor: on resume we want to prefetch ahead of where the scan is now,
      // not replay the stretch that was already served from cache.
      submittedUpTo = target;
      return;
    }
    // Advance the cursor one page at a time and only past pages that were actually submitted. An
    // earlier version jumped straight to `target` before submitting, so every page the window
    // refused was skipped permanently rather than retried — which silently capped the read-ahead
    // hit rate at ~25 % of pages.
    for (long next = Math.max(submittedUpTo + 1L, pageKey + 1L); next <= target; next++) {
      if (!submit(next)) {
        return;
      }
      submittedUpTo = next;
    }
  }

  /**
   * Decides whether the scan is missing often enough to be worth prefetching for.
   *
   * <p>The signal is the engine's own record-cache miss counter: a miss <em>is</em> a decode, so
   * misses per page crossed is exactly the quantity read-ahead can hide. Below the threshold the
   * pages are already resident and prefetching would only add trie descents and guard traffic to a
   * scan that is already fast.
   *
   * <p>The feedback loop is stable in both directions. While prefetching, the workers' own loads
   * keep the count rising, so a genuinely cold scan stays on; while suspended, only the consumer
   * can miss, so a scan that outruns the cache pushes the count back up and resumes within one
   * interval.
   */
  private void probeHitRate() {
    pagesSinceProbe = 0;
    final long misses = ShardedPageCache.getCacheMisses();
    if (missesAtLastProbe >= 0L) {
      suspended = (misses - missesAtLastProbe) < (PROBE_PAGES / 4);
    }
    missesAtLastProbe = misses;
  }

  /** @return {@code false} when the in-flight window is full, so the caller should retry later. */
  private boolean submit(final long pageKey) {
    // Best-effort: a full window means the workers are already behind, and blocking here would
    // charge the consumer for the very work the prefetch exists to overlap.
    if (!inFlight.tryAcquire()) {
      return false;
    }
    final long recordKey = pageKey << PAGE_KEY_SHIFT;
    outstanding.incrementAndGet();
    try {
      POOL.execute(() -> {
        try {
          if (!closed) {
            // Materializes the page into the shared record-page cache as a side effect. The
            // return value is irrelevant: a page whose first slot is empty still had to be
            // decoded to discover that.
            workerTrx().moveTo(recordKey);
          }
        } catch (final RuntimeException e) {
          LOGGER.debug("Prefetch of page {} failed", pageKey, e);
        } finally {
          completed.incrementAndGet();
          inFlight.release();
          // Decremented LAST, after every touch of this worker's transaction: reaching zero is
          // what licenses another thread to close it.
          if (outstanding.decrementAndGet() == 0 && closed) {
            releaseWorkerTrxsIfQuiesced();
          }
        }
      });
      submitted.incrementAndGet();
      return true;
    } catch (final RejectedExecutionException e) {
      inFlight.release();
      // The SAME handoff the worker's finally performs, and for the same reason: the task never
      // ran, so this thread is the one driving `outstanding` to zero. close() runs on a foreign
      // thread — the Cleaner action registered by AbstractJsonDBArray — and when it observes a
      // non-zero count it deliberately delegates the release to "whoever decrements last". A bare
      // decrement here accepted that delegation and then dropped it, stranding every worker
      // transaction until session close: exactly the leak this protocol exists to prevent, and
      // reachable whenever the pool's bounded queue is full at the moment close() lands.
      if (outstanding.decrementAndGet() == 0 && closed) {
        releaseWorkerTrxsIfQuiesced();
      }
      return false;
    }
  }

  private JsonNodeReadOnlyTrx workerTrx() {
    return workerTrxs.computeIfAbsent(Thread.currentThread(),
                                      thread -> session.beginNodeReadOnlyTrx(revision));
  }

  /**
   * Ends the read-ahead. Never blocks.
   *
   * <p>This runs on the QUERY thread — at the last element of a completed walk, on the probe that
   * overruns it, and on a structural mutation. Waiting here for outstanding decodes to land would
   * charge the consumer for speculative work whose results nothing will ever read, which is the
   * exact inversion of what a prefetcher is for. So the outstanding window is cancelled rather than
   * drained: {@link #closed} is published first, and every task that has not started yet sees it
   * and skips its decode.
   *
   * <p>The teardown guarantee survives that. A worker transaction may not be closed while a worker
   * might still touch it, so ownership of the release is handed to whoever observes the last task
   * finish: this thread when the window is already empty (the common case, since the pages of a
   * completed walk have been consumed), otherwise the last worker itself. Either way a finished
   * walk retains no open read-only transaction, and a wedged worker delays only the release, not
   * the query.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (DEBUG) {
      System.err.printf("# prefetch: submitted=%d completed=%d maxPageKey=%d threads=%d window=%d%n",
                        submitted.get(), completed.get(), maxPageKey, THREADS, WINDOW);
    }
    releaseWorkerTrxsIfQuiesced();
  }

  /**
   * Closes the worker transactions iff no task can still be using one. Called by the closing thread
   * and by the last worker to finish; the guard makes exactly one of them do the work, and neither
   * can run it while a task is outstanding.
   */
  private void releaseWorkerTrxsIfQuiesced() {
    if (outstanding.get() != 0) {
      // A task is queued or running. It decrements last, sees `closed`, and releases from there.
      return;
    }
    if (!workerTrxsReleased.compareAndSet(false, true)) {
      return;
    }
    for (final JsonNodeReadOnlyTrx trx : workerTrxs.values()) {
      try {
        trx.close();
      } catch (final RuntimeException e) {
        LOGGER.debug("Failed to close a prefetch transaction", e);
      }
    }
    workerTrxs.clear();
  }
}
