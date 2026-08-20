package io.sirix.access.trx.node;

import io.sirix.utils.ToStringHelper;
import io.sirix.access.User;
import io.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.PostCommitHook;
import io.sirix.api.PreCommitHook;
import io.sirix.api.StorageEngineWriter;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.PostOrderAxis;
import io.sirix.diff.DiffTuple;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixThreadedException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.page.UberPage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.nio.file.Files.deleteIfExists;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Abstract implementation for {@link InternalNodeTrx}.
 *
 * @author Joao Sousa
 */
public abstract class AbstractNodeTrxImpl<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor, NF extends NodeFactory, N extends ImmutableNode, IN extends InternalNodeReadOnlyTrx<N>>
    implements InternalNodeTrx<W>, NodeCursor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeTrxImpl.class);

  /**
   * Maximum size of the first {@link AfterCommitState#KEEP_OPEN_ASYNC_FLUSH} epoch.
   *
   * <p>The first snapshot append pays one-time serializer, codec, and worker warm-up. Starting that
   * work when the 1,048,576-modification threshold is crossed gives it a full configured epoch to
   * finish before the foreground needs the sole append permit again. Smaller configured epochs are
   * left exactly as requested.</p>
   */
  static final int MAX_ASYNC_FLUSH_PRIMING_NODE_COUNT = 1 << 20;

  /**
   * Maximum number of node modifications before auto commit.
   */
  private final int maxNodeCount;

  /**
   * Active count threshold. Only the first async-flush epoch can differ from
   * {@link #maxNodeCount}; a successful first rotation restores the configured value permanently.
   */
  private int autoCommitNodeCountThreshold;

  /**
   * {@code true} if transaction is auto-committing (by count or by delay), {@code false} otherwise.
   * Auto-committing enables async fsync for better throughput.
   */
  private final boolean isAutoCommitting;

  /**
   * Scheduled executor service.
   */
  private final ScheduledExecutorService commitScheduler;

  /**
   * Hash kind of Structure.
   */
  private final HashType hashType;

  /**
   * {@link InternalJsonNodeReadOnlyTrx} reference.
   */
  protected final IN nodeReadOnlyTrx;
  private final R typeSpecificTrx;

  /**
   * The resource session.
   */
  protected final InternalResourceSession<R, W> resourceSession;

  /**
   * {@link PathSummaryWriter} instance.
   */
  protected PathSummaryWriter<R> pathSummaryWriter;

  /**
   * {@link NodeFactory} to be able to create nodes.
   */
  protected NF nodeFactory;

  /**
   * Determines if a path summary should be built and kept up-to-date or not.
   */
  protected final boolean buildPathSummary;

  /**
   * Collects update operations in pre-order, thus it must be an order-preserving sorted map.
   */
  protected final SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered;

  /**
   * Collects update operations in no particular order (if DeweyIDs used for sorting are not stored).
   */
  protected final Map<Long, DiffTuple> updateOperationsUnordered;

  /**
   * An optional lock for all methods, if an automatic commit is issued.
   */
  @Nullable
  protected final Lock lock;

  /**
   * Transaction state.
   */
  private volatile State state;

  /**
   * After commit state: keep open or close.
   */
  private final AfterCommitState afterCommitState;

  /**
   * Modification counter.
   */
  private long modificationCount;

  /**
   * Monotonic mutation-attempt sequence used to validate transaction-local cursor fast paths.
   * Unlike {@link #modificationCount}, this value is never reset at an intermediate flush.
   */
  private long mutationSequence;

  /**
   * One-shot marker for a cursor that was just rebound to a newly inserted document node.
   *
   * <p>Streaming shredders commonly call {@code moveTo(insertedNodeKey)} immediately after an
   * insert even though the insert contract already leaves the cursor on that node. A write cursor
   * normally must resolve every move through the transaction-intent log: an async flush can replace
   * the active modified page while the frozen predecessor remains open. The marker makes only the
   * provably safe immediate self-move skippable. A subsequent mutation changes
   * {@link #mutationSequence}; a commit/revert replaces {@link #storageEngineWriter}; and the
   * type-specific transaction additionally verifies that the physical cursor still has this key.
   * The first explicit {@code moveTo} consumes the marker regardless of whether it qualifies.
   * Consuming the marker only approves reuse of the physical binding; the caller must still invoke
   * {@link InternalNodeReadOnlyTrx#prepareForApprovedSelfMove()} before reporting success.</p>
   */
  private long freshlyInsertedCursorNodeKey = Long.MIN_VALUE;
  private long freshlyInsertedCursorMutationSequence;
  @Nullable
  private StorageEngineWriter freshlyInsertedCursorWriter;

  /**
   * The storage engine writer.
   */
  protected StorageEngineWriter storageEngineWriter;

  /**
   * The {@link IndexController} used within the resource session this {@link NodeTrx} is bound to.
   */
  protected IndexController<R, W> indexController;

  /**
   * The node to revisions index (when a node has changed)
   */
  protected final RecordToRevisionsIndex nodeToRevisionsIndex;

  /**
   * Collection holding pre-commit hooks.
   */
  private final List<PreCommitHook> preCommitHooks = new ArrayList<>();

  /**
   * Collection holding post-commit hooks.
   */
  private final List<PostCommitHook> postCommitHooks = new ArrayList<>();

  /**
   * Hashes nodes.
   */
  protected AbstractNodeHashing<N, R> nodeHashing;

  /**
   * The transaction states.
   */
  private enum State {
    RUNNING,

    COMMITTING,

    COMMITTED,

    CLOSED
  }

  protected AbstractNodeTrxImpl(final ThreadFactory threadFactory, final HashType hashType, final IN nodeReadOnlyTrx,
      final R typeSpecificTrx, final InternalResourceSession<R, W> resourceSession,
      final AfterCommitState afterCommitState, final AbstractNodeHashing<N, R> nodeHashing,
      final PathSummaryWriter<R> pathSummaryWriter, final NF nodeFactory,
      final RecordToRevisionsIndex nodeToRevisionsIndex, @Nullable final Lock transactionLock,
      final Duration afterCommitDelay, final int maxNodeCount) {
    // Do not accept negative values.
    checkArgument(maxNodeCount >= 0, "Negative argument for maxNodeCount is not accepted.");
    checkArgument(!afterCommitDelay.isNegative(), "After commit delay cannot be negative");
    this.commitScheduler = newScheduledThreadPool(1, threadFactory);
    this.hashType = hashType;
    this.nodeReadOnlyTrx = requireNonNull(nodeReadOnlyTrx);
    this.typeSpecificTrx = typeSpecificTrx;
    this.resourceSession = requireNonNull(resourceSession);
    this.lock = transactionLock;
    this.afterCommitState = requireNonNull(afterCommitState);
    this.nodeHashing = requireNonNull(nodeHashing);
    this.buildPathSummary = resourceSession.getResourceConfig().withPathSummary;
    this.nodeFactory = requireNonNull(nodeFactory);
    this.pathSummaryWriter = pathSummaryWriter;
    this.indexController = resourceSession.getWtxIndexController(nodeReadOnlyTrx.getStorageEngineReader().getRevisionNumber());
    this.nodeToRevisionsIndex = requireNonNull(nodeToRevisionsIndex);

    this.updateOperationsOrdered = new TreeMap<>();
    this.updateOperationsUnordered = new HashMap<>();

    this.storageEngineWriter = (StorageEngineWriter) nodeReadOnlyTrx.getStorageEngineReader();

    // Only auto commit by node modifications if it is more then 0.
    this.maxNodeCount = maxNodeCount;
    this.autoCommitNodeCountThreshold = initialAutoCommitNodeCountThreshold(maxNodeCount, afterCommitState);
    this.isAutoCommitting = maxNodeCount > 0 || !afterCommitDelay.isZero();
    this.modificationCount = 0L;

    this.state = State.RUNNING;

    if (!afterCommitDelay.isZero()) {
      commitScheduler.scheduleWithFixedDelay(() -> commit("autoCommit", null), afterCommitDelay.toMillis(),
          afterCommitDelay.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  /** Resolve the one-time async-flush priming threshold without changing any other commit mode. */
  static int initialAutoCommitNodeCountThreshold(final int maxNodeCount,
      final AfterCommitState afterCommitState) {
    checkArgument(maxNodeCount >= 0, "Negative argument for maxNodeCount is not accepted.");
    requireNonNull(afterCommitState);
    return afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH
        ? Math.min(maxNodeCount, MAX_ASYNC_FLUSH_PRIMING_NODE_COUNT)
        : maxNodeCount;
  }

  protected abstract W self();

  protected void assertRunning() {
    if (state != State.RUNNING) {
      throw new IllegalStateException("Transaction state is not running: " + state);
    }
  }

  @Override
  public Optional<User> getUser() {
    return getResourceSession().getUser();
  }

  @Override
  public W setBulkInsertion(final boolean bulkInsertion) {
    nodeHashing.setBulkInsert(bulkInsertion);
    // When enabling bulk insertion with auto-committing transactions, we need to also
    // enable autoCommit on nodeHashing so that hashes are computed during insertion.
    // This is critical for the KotlinJsonStreamingShredder which calls setBulkInsertion(true)
    // externally rather than going through insertSubtreeAsFirstChild().
    if (bulkInsertion && isAutoCommitting) {
      nodeHashing.setAutoCommit(true);
    } else if (!bulkInsertion) {
      // When disabling bulk insertion, also disable autoCommit on nodeHashing
      nodeHashing.setAutoCommit(false);
    }
    return self();
  }

  /**
   * Get the current node.
   *
   * @return {@link Node} implementation
   */
  protected N getCurrentNode() {
    return nodeReadOnlyTrx.getCurrentNode();
  }

  /**
   * Modifying hashes in a postorder-traversal.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  protected void postOrderTraversalHashes() {
    final var axis = new PostOrderAxis(this, IncludeSelf.YES);

    while (axis.hasNext()) {
      axis.nextLong();
      nodeHashing.addHashAndDescendantCount();
    }
  }

  @Override
  public void adaptHashesInPostorderTraversal() {
    if (hashType != HashType.NONE) {
      final long nodeKey = nodeReadOnlyTrx.getNodeKey();
      postOrderTraversalHashes();
      final ImmutableNode startNode = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
      // Pre-capture all values from startNode before traversing ancestors.
      // Each ancestor's prepareRecordForModification may overwrite the singleton if same kind.
      final long hashToAdd = startNode.computeHash(nodeHashing.getBytes());
      final boolean startIsStruct = startNode instanceof StructNode;
      final long startDescendantCount = startIsStruct
          ? ((StructNode) startNode).getDescendantCount()
          : 0;
      moveToParent();
      while (nodeReadOnlyTrx.hasParent()) {
        moveToParent();
        nodeHashing.addParentHash(hashToAdd, startIsStruct, startDescendantCount);
      }
      moveTo(nodeKey);
    }
  }

  @Override
  public void runLocked(final Runnable runnable) {
    if (lock != null) {
      lock.lock();
    }
    try {
      runnable.run();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public W commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp) {
    nodeReadOnlyTrx.assertNotClosed();
    if (commitTimestamp != null && !resourceSession.getResourceConfig().customCommitTimestamps()) {
      throw new IllegalStateException("Custom commit timestamps are not enabled for the resource.");
    }

    // Public commit() is always a final (non-intermediate) commit.
    return commitInternal(commitMessage, commitTimestamp, false);
  }

  /**
   * Internal commit implementation shared by explicit commit() and intermediate auto-commit.
   *
   * @param commitMessage optional commit message
   * @param commitTimestamp optional commit timestamp
   * @param isIntermediateCommit if true, this is an intermediate auto-commit during bulk insert;
   *        redundant I/O (e.g. unchanged index definitions) may be skipped
   * @return this transaction for chaining
   */
  // ==================== ASYNC COMMIT PIPELINE (KEEP_OPEN_ASYNC_COMMIT) ====================
  // Depth-1 pipeline state. Lives on the NODE transaction (not the page writer) because every
  // async-commit epoch creates a NEW page writer; the pipeline outlives each of them.

  /** One background hardening in flight at most; acquired by phase 1, released after phase 2. */
  private final Semaphore asyncCommitPermit = new Semaphore(1);

  /** First background hardening failure; latches the transaction terminally. */
  private volatile Throwable asyncCommitFailure;

  /** Permanent failure latch — a lost hardening invalidates every successor epoch. */
  private volatile boolean asyncCommitTerminalFailure;

  /**
   * Drain the async-commit pipeline: wait for a pending background hardening and surface its
   * failure. Called before any synchronous commit, rollback, or close.
   */
  private void awaitPendingAsyncCommit() {
    asyncCommitPermit.acquireUninterruptibly();
    asyncCommitPermit.release();
    final Throwable failure = asyncCommitFailure;
    if (failure != null) {
      asyncCommitFailure = null;
      asyncCommitTerminalFailure = true;
      throw new SirixIOException("Async commit hardening failed", failure);
    }
    if (asyncCommitTerminalFailure) {
      throw new SirixIOException("Transaction in terminal failure state from prior async commit error");
    }
  }

  /**
   * Pipelined intermediate commit: phase 1 (page writes) inline, phase 2 (durability barriers)
   * in the background; the transaction immediately continues on a successor epoch based on the
   * pending uber page. Readers see the revision when the background hardening publishes it.
   */
  private void asyncCommitInternal(final String commitMessage) {
    runLocked(() -> {
      final var preCommitRevision = getRevisionNumber();

      state = State.COMMITTING;

      try {
        if (pathSummaryWriter != null) {
          pathSummaryWriter.flushPendingStats();
        }
        for (final PreCommitHook hook : preCommitHooks) {
          hook.preCommit(this);
        }

        // Apply commit-time index maintenance (incremental projection
        // updates) while index writes can still ride this commit.
        indexController.applyPendingIndexMaintenance();

        // Depth-1: wait for the previous epoch's hardening (and surface its failure), and drain
        // any pending async flush of THIS writer before serializing.
        awaitPendingAsyncCommit();
        storageEngineWriter.awaitPendingAsyncFlush();
      } catch (final RuntimeException | Error e) {
        state = State.RUNNING;
        throw e;
      }

      asyncCommitPermit.acquireUninterruptibly();

      final StorageEngineWriter committingWriter = storageEngineWriter;
      final UberPage pendingUberPage;
      try {
        pendingUberPage = committingWriter.commitWritePages(commitMessage, null, true);
      } catch (final RuntimeException | Error e) {
        asyncCommitPermit.release();
        // Nothing is durable yet — keep the transaction usable, mirroring commitInternal.
        state = State.RUNNING;
        throw e;
      }

      // Phase 1 succeeded: the epoch will become durable (or the pipeline poisons the trx).
      // Expose the pending revision root so the successor epoch (the only reader that can reach
      // the unpublished revision) resolves it from memory.
      resourceSession.putPendingRevisionRoot(pendingUberPage.getRevisionNumber(),
          committingWriter.getActualRevisionRootPage());
      modificationCount = 0L;

      if (resourceSession.getResourceConfig().storeDiffs()) {
        try {
          serializeUpdateDiffs(preCommitRevision);
        } catch (final RuntimeException | Error e) {
          LOGGER.error("Update-diff serialization failed for pipelined revision {} — the diff file "
              + "for this revision is missing.", preCommitRevision, e);
        }
      }

      try {
        reInstantiate(getId(), pendingUberPage.getRevisionNumber(), pendingUberPage);
        state = State.RUNNING;
      } catch (final RuntimeException | Error e) {
        // Successor epoch could not be created — harden inline so the committed data survives,
        // then surface the failure with a truthful terminal state (mirrors commitInternal).
        hardenAndPublish(committingWriter, pendingUberPage);
        asyncCommitPermit.release();
        state = State.COMMITTED;
        throw e;
      }

      try {
        CompletableFuture.runAsync(() -> {
          try {
            hardenAndPublish(committingWriter, pendingUberPage);
          } catch (final Throwable t) {
            asyncCommitFailure = t;
            asyncCommitTerminalFailure = true;
          } finally {
            asyncCommitPermit.release();
          }
        });
      } catch (final Throwable t) {
        // Submission failed (e.g. RejectedExecutionException): harden inline — correctness over
        // pipelining.
        try {
          hardenAndPublish(committingWriter, pendingUberPage);
        } finally {
          asyncCommitPermit.release();
        }
      }
    });

    // Execute post-commit hooks (parity with sync intermediate commits).
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }
  }

  /**
   * Phase 2 + publication: harden the pending revision, make it visible to readers, and close the
   * superseded page transaction (releasing its writer channels).
   */
  private void hardenAndPublish(final StorageEngineWriter committingWriter, final UberPage pendingUberPage) {
    committingWriter.hardenCommit(pendingUberPage, true);
    resourceSession.setLastCommittedUberPage(pendingUberPage);
    // Publish-then-clear: there is no window in which the revision resolves through neither path.
    resourceSession.clearPendingRevisionRoot(pendingUberPage.getRevisionNumber());
    committingWriter.close();
  }

  private W commitInternal(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
      final boolean isIntermediateCommit) {
    runLocked(() -> {
      final var preCommitRevision = getRevisionNumber();

      // Drain the async-commit pipeline first: the synchronous commit below must build on a
      // hardened predecessor (and a poisoned pipeline must fail loudly, not commit on top of a
      // lost revision).
      awaitPendingAsyncCommit();

      state = State.COMMITTING;

      final UberPage uberPage;
      try {
        // Flush deferred PathSummary statistics into real PathNodes before
        // commit. Each recordValue() call during the transaction buffered its
        // delta in PathSummaryWriter.pendingStats rather than paying a
        // prepareRecordForModification COW per insert; applying them all here
        // reduces per-shred PathSummary COW traffic by several orders of
        // magnitude on analytical workloads.
        if (pathSummaryWriter != null) {
          pathSummaryWriter.flushPendingStats();
        }

        // Execute pre-commit hooks.
        for (final PreCommitHook hook : preCommitHooks) {
          hook.preCommit(this);
        }

        // Apply commit-time index maintenance (incremental projection
        // updates) while index writes can still ride this commit. A final
        // commit additionally closes any load-time index build: it is the
        // only signal the transaction gives that no more records follow.
        indexController.applyPendingIndexMaintenance(!isIntermediateCommit);

        // Await any pending async background flush before sync commit
        storageEngineWriter.awaitPendingAsyncFlush();

        uberPage = storageEngineWriter.commit(commitMessage, commitTimestamp, isAutoCommitting, isIntermediateCommit);
      } catch (final RuntimeException | Error e) {
        // Nothing is durable yet: restore the state machine so the transaction stays usable
        // (retry, further work, or an explicit rollback all remain possible) instead of being
        // stranded in COMMITTING where assertRunning() rejects everything. The modification
        // counter is untouched here — it is only cleared after durability — so the dirty-close
        // guard keeps refusing to silently drop the uncommitted work (#1061).
        state = State.RUNNING;
        throw e;
      }

      // The revision is durable from this point on. Clear the dirty counter only now: resetting
      // it before the storage commit let a failed commit followed by close() silently discard
      // uncommitted work (close() only refuses when modificationCount > 0).
      modificationCount = 0L;

      // Remember successfully committed uber page in resource session.
      resourceSession.setLastCommittedUberPage(uberPage);

      if (resourceSession.getResourceConfig().storeDiffs()) {
        try {
          serializeUpdateDiffs(preCommitRevision);
        } catch (final RuntimeException | Error e) {
          // Post-durability failure: the revision IS committed; a failed diff-sidecar write must
          // neither strand the transaction in COMMITTING nor pretend the commit failed. Surface
          // it loudly and keep the transaction alive.
          LOGGER.error("Update-diff serialization failed after revision {} was durably committed — "
              + "the diff file for this revision is missing.", preCommitRevision, e);
        }
      }

      try {
        // Reinstantiate everything.
        if (afterCommitState == AfterCommitState.KEEP_OPEN
            || afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH
            || afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT) {
          // Use the newly committed revision number, not the pre-commit revision.
          // After commit, uberPage represents the new revision, and we need to
          // create a page transaction that will prepare the NEXT revision.
          final int newlyCommittedRevision = uberPage.getRevisionNumber();
          reInstantiate(getId(), newlyCommittedRevision);
          state = State.RUNNING;
        } else {
          state = State.COMMITTED;
        }
      } catch (final RuntimeException | Error e) {
        // The data is durable but the transaction could not be re-bound to the new revision.
        // Terminal COMMITTED (never stuck COMMITTING): further mutations are rejected with a
        // truthful state, and close() is clean because nothing uncommitted remains.
        state = State.COMMITTED;
        throw e;
      }
    });

    // Execute post-commit hooks.
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }

    return self();
  }

  /**
   * Depth of nested compound structural operations (move, replace, …). While > 0, count-based
   * intermediate auto-commits are suppressed: compound operations internally call public mutators
   * (each running {@link #checkAccessAndCommit()}), and an auto-commit firing between the internal
   * steps would durably persist a structurally inconsistent tree — e.g. a moved subtree already
   * detached from its old position but not yet re-attached (#1062). The counter keeps growing, so
   * the deferred auto-commit fires on the next top-level mutation once the tree is consistent
   * again. Guarded by the transaction lock like all mutations; no extra synchronization needed.
   */
  private int compoundOperationDepth;

  /**
   * Mark the start of a compound structural operation. Must be balanced with
   * {@link #endCompoundOperation()} in a {@code finally} block so an exception mid-operation
   * cannot leave auto-commits suppressed forever.
   */
  protected final void beginCompoundOperation() {
    compoundOperationDepth++;
  }

  /** Mark the end of a compound structural operation (see {@link #beginCompoundOperation()}). */
  protected final void endCompoundOperation() {
    assert compoundOperationDepth > 0 : "unbalanced endCompoundOperation()";
    compoundOperationDepth--;
  }

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  protected void checkAccessAndCommit() {
    nodeReadOnlyTrx.assertNotClosed();
    assertRunning();
    mutationSequence++;
    modificationCount++;
    intermediateCommitIfRequired();
  }

  /**
   * Fast-path for bulk insert: skips assertNotClosed/assertRunning (always true mid-shredder),
   * keeps mod count + auto-commit logic. Uses intermediate commit to skip redundant I/O.
   */
  protected final void checkAccessAndCommitBulk() {
    mutationSequence++;
    modificationCount++;
    if (maxNodeCount > 0 && modificationCount > autoCommitNodeCountThreshold && compoundOperationDepth == 0) {
      if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT) {
        asyncCommitInternal("autoCommit");
      } else if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH) {
        flushIntermediateAsyncEpoch();
      } else {
        commitInternal("autoCommit", null, true);
      }
    }
  }

  /**
   * Shared insertion preflight. A subtree shredder already established that the transaction is
   * open and running before enabling bulk mode, so its per-node inserts retain count-based
   * rotation while avoiding two redundant state checks. Standalone JSON/XML insertions keep the
   * full public-mutator validation path.
   */
  protected final void checkAccessAndCommitForInsert() {
    if (nodeHashing.isBulkInsert()) {
      checkAccessAndCommitBulk();
    } else {
      checkAccessAndCommit();
    }
  }

  /**
   * Position the cursor on the document node created by the current insertion.
   *
   * <p>The node factory's allocation is still the writer's most recent document allocation at
   * this point: insertion preflight ran before creation, while linkage and hash maintenance do not
   * allocate another document node. The internal cursor can therefore bind its own singleton from
   * that exact page slot and avoid a redundant TIL page resolution. Any violated guard takes the
   * ordinary TIL-aware move path.</p>
   */
  protected final void moveToJustInsertedNode(final long nodeKey) {
    if (!nodeReadOnlyTrx.tryMoveToLastAllocatedDocumentNode(storageEngineWriter, nodeKey)) {
      nodeReadOnlyTrx.moveTo(nodeKey);
    }
  }

  /**
   * Mark that a successful insert has left the physical cursor on {@code nodeKey}.
   *
   * <p>The marker is deliberately transaction-local and allocation-free. It does not relax the
   * general rule that write-cursor moves must consult the transaction-intent log.</p>
   */
  protected final void markFreshlyInsertedCursor(final long nodeKey) {
    assert nodeKey >= 0 : "inserted document node keys must be non-negative";
    assert nodeReadOnlyTrx.getNodeKey() == nodeKey : "insert must leave the cursor on the new node";
    freshlyInsertedCursorNodeKey = nodeKey;
    freshlyInsertedCursorMutationSequence = mutationSequence;
    freshlyInsertedCursorWriter = storageEngineWriter;
  }

  /**
   * Consume and validate the one-shot freshly-inserted cursor marker.
   *
   * <p>A {@code true} result approves skipping only the physical node rebind. The caller remains
   * responsible for applying the normal logical cursor-move side effects through
   * {@link InternalNodeReadOnlyTrx#prepareForApprovedSelfMove()} before returning to its caller.</p>
   *
   * @return {@code true} only when {@code nodeKey} is an immediate self-move in the same mutation
   *         epoch and on the same storage writer
   */
  protected final boolean consumeFreshlyInsertedSelfMove(final long nodeKey) {
    final long markedNodeKey = freshlyInsertedCursorNodeKey;
    final long markedMutationSequence = freshlyInsertedCursorMutationSequence;
    final StorageEngineWriter markedWriter = freshlyInsertedCursorWriter;

    // Every explicit move consumes the marker. A later move must take the normal TIL-aware path.
    freshlyInsertedCursorNodeKey = Long.MIN_VALUE;
    freshlyInsertedCursorWriter = null;

    return nodeKey == markedNodeKey
        && mutationSequence == markedMutationSequence
        && storageEngineWriter == markedWriter
        && nodeReadOnlyTrx.getNodeKey() == nodeKey;
  }

  protected final void persistUpdatedRecord(final DataRecord record) {
    if (record instanceof FlyweightNode fn && fn.isWriteSingleton() && fn.getOwnerPage() != null) {
      return; // Bound write singleton — mutations already on heap via inlined setters
    }
    // Ensure the mutated record is stored in the TIL's modified page.
    // For records obtained via prepareRecordForModification(), the page is already
    // in the TIL and the record is in records[] — this is a safe (redundant) setRecord.
    // For records obtained via nodeReadOnlyTrx.getCurrentNode() (e.g., setName/setValue),
    // this ensures the page enters the TIL and the record gets into records[].
    storageEngineWriter.persistRecord(record, IndexType.DOCUMENT, -1);
  }

  /**
   * Making an intermediate commit based on set attributes.
   *
   * @throws SirixException if commit fails
   */
  private void intermediateCommitIfRequired() {
    nodeReadOnlyTrx.assertNotClosed();
    if (maxNodeCount > 0 && modificationCount > autoCommitNodeCountThreshold && compoundOperationDepth == 0) {
      if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT) {
        asyncCommitInternal("autoCommit");
      } else if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH) {
        flushIntermediateAsyncEpoch();
      } else {
        LOGGER.debug("AUTO-COMMIT triggered: modificationCount=" + modificationCount + ", maxNodeCount=" + maxNodeCount);
        commitInternal("autoCommit", null, true);
        LOGGER.debug("AUTO-COMMIT completed");
      }
    }
  }

  /**
   * Rotate one intermediate async-flush epoch and permanently retire the first-epoch threshold only
   * after the rotation succeeds.
   */
  private void flushIntermediateAsyncEpoch() {
    // Index maintenance that has to READ the records it is maintaining must run while those records
    // are still reachable through this transaction. The flush writes their pages out and lets go of
    // them, and the revision they belong to is not committed yet, so nothing can read them back
    // afterwards.
    indexController.notifyBeforePageFlush();
    storageEngineWriter.asyncFlush();

    // Keep failure semantics unchanged: neither the dirty counter nor the priming threshold is reset
    // if index maintenance or the async rotation throws.
    modificationCount = 0;
    autoCommitNodeCountThreshold = maxNodeCount;

    // Match sync reInstantiate() behavior: new nodeHashing has autoCommit=false. Without this,
    // rollingAdd() walks the full ancestor chain on every insert; bulk insertion skips hashing during
    // intermediate epochs in the same way as the synchronous path.
    nodeHashing.setAutoCommit(false);
  }

  protected abstract void serializeUpdateDiffs(int revisionNumber);

  /**
   * Create new instances.
   *
   * @param trxID transaction ID
   * @param revNumber revision number
   */
  private void reInstantiate(final int trxID, final int revNumber) {
    reInstantiate(trxID, revNumber, null);
  }

  /**
   * @param pendingBaseUberPage non-null for pipelined async commits: the phase-1-complete uber
   *        page of the still-hardening revision. The superseded page transaction is DETACHED
   *        (closed later by the background hardening thread), and the successor is based on the
   *        pending uber page instead of {@code lastCommittedUberPage}.
   */
  private void reInstantiate(final int trxID, final int revNumber, final UberPage pendingBaseUberPage) {
    final boolean timing = LOGGER.isDebugEnabled();
    final long r0 = timing ? System.nanoTime() : 0;

    // Save the current cursor position. getNodeKey() reads from a Java field, always valid.
    final long currentNodeKey = nodeReadOnlyTrx.getNodeKey();

    // Reset page transaction to new uber page.
    if (pendingBaseUberPage == null) {
      resourceSession.closeNodePageWriteTransaction(getId());
    } else {
      resourceSession.detachNodePageWriteTransaction(getId());
    }

    final long r1 = timing ? System.nanoTime() : 0;

    storageEngineWriter =
        resourceSession.createPageTransaction(trxID, revNumber, revNumber, InternalResourceSession.Abort.NO, true,
            pendingBaseUberPage);
    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(storageEngineWriter);
    resourceSession.setNodePageWriteTransaction(getId(), storageEngineWriter);

    final long r2 = timing ? System.nanoTime() : 0;

    nodeFactory = reInstantiateNodeFactory(storageEngineWriter);

    final boolean isBulkInsert = nodeHashing.isBulkInsert();
    nodeHashing = reInstantiateNodeHashing(storageEngineWriter);
    nodeHashing.setBulkInsert(isBulkInsert);

    updateOperationsUnordered.clear();
    updateOperationsOrdered.clear();

    reInstantiateIndexes();

    // Re-read the current node from the new page transaction.
    // FlyweightNode getters read from the page MemorySegment; after closing the old transaction,
    // that MemorySegment is stale. Re-reading creates a fresh node from the new transaction.
    nodeReadOnlyTrx.moveTo(currentNodeKey);

    final long r3 = timing ? System.nanoTime() : 0;

    if (timing) {
      LOGGER.debug("reInstantiate: close={}ms createPageTrx={}ms rest={}ms total={}ms",
          ms(r1 - r0), ms(r2 - r1), ms(r3 - r2), ms(r3 - r0));
    }
  }

  private static long ms(final long nanos) {
    return nanos / 1_000_000;
  }

  protected abstract AbstractNodeHashing<N, R> reInstantiateNodeHashing(StorageEngineWriter storageEngineWriter);

  protected abstract NF reInstantiateNodeFactory(StorageEngineWriter storageEngineWriter);

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (buildPathSummary) {
      pathSummaryWriter = new PathSummaryWriter<>(storageEngineWriter, resourceSession, nodeFactory, typeSpecificTrx);
    }

    // Recreate index listeners. Clear first: after rollback/revertTo the
    // revision-keyed controller cache can return a controller that still
    // holds listeners bound to the PREVIOUS (now closed) storage-engine
    // writer / path summary — re-adding without clearing would either
    // duplicate listeners or (for listener types that dedup per definition)
    // keep a stale, possibly spent listener in place of a fresh one.
    final var indexDefs = indexController.getIndexes().getIndexDefs();
    indexController = resourceSession.getWtxIndexController(nodeReadOnlyTrx.getStorageEngineReader().getRevisionNumber());
    indexController.clearChangeListeners();
    indexController.createIndexListeners(indexDefs, self());

    nodeToRevisionsIndex.setStorageEngineWriter(storageEngineWriter);
  }

  @Override
  public synchronized W rollback() {
    if (lock != null) {
      lock.lock();
    }
    try {
      nodeReadOnlyTrx.assertNotClosed();

      // Drain the async-commit pipeline: rolling back on top of an un-hardened (or failed)
      // predecessor epoch is unsound.
      awaitPendingAsyncCommit();

      // Save the current cursor position before closing the old page transaction.
      final long rollbackNodeKey = nodeReadOnlyTrx.getNodeKey();

      // Reset modification counter.
      modificationCount = 0L;

      // Close current page transaction.
      final int trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = storageEngineWriter.getUberPage().isBootstrap()
          ? 0
          : revision - 1;

      final UberPage uberPage = storageEngineWriter.rollback();

      // Remember successfully committed uber page in resource session.
      resourceSession.setLastCommittedUberPage(uberPage);

      resourceSession.closeNodePageWriteTransaction(getId());
      nodeReadOnlyTrx.setPageReadTransaction(null);
      removeCommitFile();

      storageEngineWriter =
          resourceSession.createPageTransaction(trxID, revNumber, revNumber, InternalResourceSession.Abort.YES, true);
      nodeReadOnlyTrx.setPageReadTransaction(storageEngineWriter);
      resourceSession.setNodePageWriteTransaction(getId(), storageEngineWriter);

      nodeFactory = reInstantiateNodeFactory(storageEngineWriter);

      // Re-bind nodeHashing to the NEW page transaction: AbstractNodeHashing holds a final
      // reference to its StorageEngineWriter, and the old one was just closed by the abort. Without
      // this, the first post-rollback mutation on a hash-enabled resource would call
      // prepareRecordForModification on the closed writer and throw.
      final boolean isBulkInsert = nodeHashing.isBulkInsert();
      nodeHashing = reInstantiateNodeHashing(storageEngineWriter);
      nodeHashing.setBulkInsert(isBulkInsert);

      reInstantiateIndexes();

      // Discard update-operation tuples recorded before the rollback: their node keys belong to the
      // aborted revision and must not leak into the next commit's diff (a later commit would
      // otherwise serialize phantom operations that were never committed).
      updateOperationsUnordered.clear();
      updateOperationsOrdered.clear();

      // Re-read the current node from the new page transaction (FlyweightNode binding is stale).
      nodeReadOnlyTrx.moveTo(rollbackNodeKey);

      return self();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void removeCommitFile() {
    try {
      deleteIfExists(resourceSession.getCommitFile());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public W revertTo(final int revision) {
    if (lock != null) {
      lock.lock();
    }

    try {
      nodeReadOnlyTrx.assertNotClosed();
      resourceSession.assertAccess(revision);

      // Save the current cursor position before closing the old page transaction.
      final long revertNodeKey = nodeReadOnlyTrx.getNodeKey();

      // Close current page transaction.
      final int trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      resourceSession.closeNodePageWriteTransaction(getId());
      storageEngineWriter =
          resourceSession.createPageTransaction(trxID, revision, revNumber - 1, InternalResourceSession.Abort.NO, true);
      nodeReadOnlyTrx.setPageReadTransaction(null);
      nodeReadOnlyTrx.setPageReadTransaction(storageEngineWriter);
      resourceSession.setNodePageWriteTransaction(getId(), storageEngineWriter);

      nodeHashing = reInstantiateNodeHashing(storageEngineWriter);

      // Reset node factory.
      nodeFactory = reInstantiateNodeFactory(storageEngineWriter);

      // New index instances.
      reInstantiateIndexes();

      // Discard update-operation tuples recorded against the reverted-from revision.
      updateOperationsUnordered.clear();
      updateOperationsOrdered.clear();

      // Reset modification counter.
      modificationCount = 0L;

      // Move to document root.
      moveToDocumentRoot();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }

    return self();
  }

  @Override
  public W addPreCommitHook(final PreCommitHook hook) {
    if (lock != null) {
      lock.lock();
    }

    try {
      preCommitHooks.add(requireNonNull(hook));
      return self();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public W addPostCommitHook(final PostCommitHook hook) {
    if (lock != null) {
      lock.lock();
    }

    try {
      postCommitHooks.add(requireNonNull(hook));
      return self();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Rolls the resource back to {@code revision}: truncates the data + revisions files, rewrites
   * both uber beacons, resets the session's last-committed uber page, and drops the database's
   * cached pages. Was a no-op TODO — {@code TransactionImpl}'s atomicity undo silently did
   * nothing. The transaction's cursor state still refers to the truncated revision afterwards;
   * callers must close (or roll back) the transaction without committing through it.
   */
  @Override
  public W truncateTo(final int revision) {
    nodeReadOnlyTrx.assertNotClosed();
    final int currentRevision = getRevisionNumber();
    checkArgument(revision >= 0 && revision < currentRevision,
                  "revision %s must be in [0, current revision %s).", revision, currentRevision);

    storageEngineWriter.truncateTo(revision);

    return self();
  }

  @Override
  public PathSummaryReader getPathSummary() {
    if (lock != null) {
      lock.lock();
    }

    try {
      return pathSummaryWriter.getPathSummary();
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public StorageEngineWriter getStorageEngineWriter() {
    nodeReadOnlyTrx.assertNotClosed();
    return (StorageEngineWriter) nodeReadOnlyTrx.getStorageEngineReader();
  }

  @Override
  public Optional<User> getUserOfRevisionToRepresent() {
    return nodeReadOnlyTrx.getUser();
  }

  @Override
  public void close() {
    runLocked(() -> {
      if (!isClosed()) {
        // Drain the async-commit pipeline so every hardening finished (or its failure is known)
        // before resources are torn down. A failure is logged, not thrown: close() must still
        // release resources — the data loss already happened and was latched.
        try {
          awaitPendingAsyncCommit();
        } catch (final RuntimeException e) {
          LOGGER.error("Async commit pipeline failed before close — the last pipelined revision(s) "
              + "did not become durable.", e);
        }

        // Make sure to commit all dirty data.
        if (modificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Release all state immediately.
        final int trxId = getId();
        nodeReadOnlyTrx.close();

        // CRITICAL FIX: Close StorageEngineWriter to trigger TIL.close() and clean up uncommitted pages
        // Without this, TIL instances with uncommitted pages leak
        if (storageEngineWriter != null && !storageEngineWriter.isClosed()) {
          storageEngineWriter.close();
        }

        resourceSession.closeWriteTransaction(trxId);
        removeCommitFile();

        pathSummaryWriter = null;
        nodeFactory = null;

        // Shutdown pool.
        commitScheduler.shutdown();
        try {
          commitScheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          throw new SirixThreadedException(e);
        }
      }
    });
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    nodeReadOnlyTrx.assertNotClosed();

    return nodeReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    nodeReadOnlyTrx.assertNotClosed();
    return nodeReadOnlyTrx.getDeweyID();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AbstractNodeTrxImpl<?, ?, ?, ?, ?> that = (AbstractNodeTrxImpl<?, ?, ?, ?, ?>) o;
    return nodeReadOnlyTrx.equals(that.nodeReadOnlyTrx);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeReadOnlyTrx);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                      .add("super", super.toString())
                      .add("hashType", this.hashType)
                      .add("nodeReadOnlyTrx", this.nodeReadOnlyTrx)
                      .toString();
  }
}
