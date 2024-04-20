package io.sirix.access.trx.node;

import com.google.common.base.MoreObjects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import io.sirix.api.*;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.PostOrderAxis;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.diff.DiffTuple;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.exception.*;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.page.UberPage;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.deleteIfExists;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Abstract implementation for {@link InternalNodeTrx}.
 *
 * @author Joao Sousa
 */
public abstract class AbstractNodeTrxImpl<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor, NF extends NodeFactory, N extends ImmutableNode, IN extends InternalNodeReadOnlyTrx<N>>
    implements NodeReadOnlyTrx, InternalNodeTrx<W>, NodeCursor {

  /**
   * Single threaded executor service to commit asynchronally.
   */
  private final ExecutorService commitThreadPool = Executors.newSingleThreadExecutor(new JsonNodeTrxThreadFactory());

  /**
   * Maximum number of node modifications before auto commit.
   */
  private final int maxNodeCount;
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
   * The resource manager.
   */
  protected final InternalResourceSession<R, W> resourceSession;

  private final String databaseName;

  private final boolean doAsyncCommit;

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
   * The page write trx.
   */
  protected PageTrx pageTrx;

  /**
   * The {@link IndexController} used within the resource manager this {@link NodeTrx} is bound to.
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

  private final Semaphore commitLock = new Semaphore(1);

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

  /**
   * The revision number before bulk-inserting nodes.
   */
  protected int beforeBulkInsertionRevisionNumber;

  /**
   * {@code true}, if transaction is auto-committing, {@code false} if not.
   */
  private final boolean isAutoCommitting;

  protected AbstractNodeTrxImpl(final String databaseName, final ThreadFactory threadFactory, final HashType hashType,
      final IN nodeReadOnlyTrx, final R typeSpecificTrx, final InternalResourceSession<R, W> resourceManager,
      final AfterCommitState afterCommitState, final AbstractNodeHashing<N, R> nodeHashing,
      final PathSummaryWriter<R> pathSummaryWriter, final NF nodeFactory,
      final RecordToRevisionsIndex nodeToRevisionsIndex, @Nullable final Lock transactionLock,
      final Duration afterCommitDelay, @NonNegative final int maxNodeCount, final boolean doAsyncCommit) {
    // Do not accept negative values.
    checkArgument(maxNodeCount >= 0, "Negative argument for maxNodeCount is not accepted.");
    checkArgument(!afterCommitDelay.isNegative(), "After commit delay cannot be negative");
    this.databaseName = databaseName;
    this.commitScheduler = newScheduledThreadPool(1, threadFactory);
    this.hashType = hashType;
    this.nodeReadOnlyTrx = requireNonNull(nodeReadOnlyTrx);
    this.typeSpecificTrx = typeSpecificTrx;
    this.resourceSession = requireNonNull(resourceManager);
    this.lock = transactionLock;
    this.afterCommitState = requireNonNull(afterCommitState);
    this.nodeHashing = requireNonNull(nodeHashing);
    this.buildPathSummary = resourceManager.getResourceConfig().withPathSummary;
    this.nodeFactory = requireNonNull(nodeFactory);
    this.pathSummaryWriter = pathSummaryWriter;
    this.indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    this.nodeToRevisionsIndex = requireNonNull(nodeToRevisionsIndex);
    this.doAsyncCommit = doAsyncCommit;

    this.updateOperationsOrdered = new TreeMap<>();
    this.updateOperationsUnordered = new HashMap<>();

    this.pageTrx = (PageTrx) nodeReadOnlyTrx.getPageTrx();

    // Only auto commit by node modifications if it is more then 0.
    this.maxNodeCount = maxNodeCount;
    this.modificationCount = 0L;

    this.state = State.RUNNING;

    isAutoCommitting = maxNodeCount > 0 || !afterCommitDelay.isZero();

    if (!afterCommitDelay.isZero()) {
      commitScheduler.scheduleWithFixedDelay(() -> commit("autoCommit", null),
                                             afterCommitDelay.toMillis(),
                                             afterCommitDelay.toMillis(),
                                             TimeUnit.MILLISECONDS);
    }
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
      final long nodeKey = getCurrentNode().getNodeKey();
      postOrderTraversalHashes();
      final ImmutableNode startNode = getCurrentNode();
      moveToParent();
      while (getCurrentNode().hasParent()) {
        moveToParent();
        nodeHashing.addParentHash(startNode);
      }
      moveTo(nodeKey);
    }
  }

  protected void runLocked(final Runnable runnable) {
    if (lock != null) {
      lock.lock();
    }
    runnable.run();
    if (lock != null) {
      lock.unlock();
    }
  }

  public W asyncCommit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp) {
    nodeReadOnlyTrx.assertNotClosed();
    if (commitTimestamp != null && !resourceSession.getResourceConfig().customCommitTimestamps()) {
      throw new IllegalStateException("Custom commit timestamps are not enabled for the resource.");
    }

    try {
      commitLock.acquire();
    } catch (final InterruptedException e) {
      throw new SirixRuntimeException(e.getMessage(), e);
    }

    // Execute pre-commit hooks.
    for (final PreCommitHook hook : preCommitHooks) {
      hook.preCommit(this);
    }

    if (lock != null) {
      lock.lock();
    }

    try {
      if (pageTrx.getFormerLog() != null) {
        pageTrx.getFormerLog().close();
      }

      // Reinstantiate everything.
      final var uberPage = pageTrx.getUberPage();
      final var log = new TransactionIntentLog(pageTrx.getLog());
      pageTrx.setLog(new TransactionIntentLog(log));

      // Reset modification counter.
      modificationCount = 0L;

      final int revisionNumber = getRevisionNumber();

      final var commitTask = new CommitTask<>(databaseName,
                                              commitMessage,
                                              pageTrx,
                                              commitLock,
                                              lock,
                                              resourceSession,
                                              nodeHashing,
                                              beforeBulkInsertionRevisionNumber,
                                              isAutoCommitting,
                                              new TreeMap<>(updateOperationsOrdered),
                                              new TreeMap<>(updateOperationsUnordered),
                                              storeDeweyIDs());

      commitThreadPool.submit(commitTask);

      reInstantiate(getId(), revisionNumber, uberPage, log);
    } catch (Throwable e) {
      throw new SirixRuntimeException(e.getMessage(), e);
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }

    return self();
  }

  private final static class CommitTask<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
      implements Callable<UberPage> {
    private final String commitMessage;

    private final PageTrx pageTrx;

    private final Semaphore commitLock;

    private final Lock lock;

    private final InternalResourceSession<R, W> resourceSession;

    private final int revisionNumber;

    private final AbstractNodeHashing<?, ?> nodeHashing;

    private final int beforeBulkInsertionRevisionNumber;

    private final boolean isAutoCommitting;

    /**
     * Collects update operations in pre-order, thus it must be an order-preserving sorted map.
     */
    private final SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered;

    /**
     * Collects update operations in no particular order (if DeweyIDs used for sorting are not stored).
     */
    private final Map<Long, DiffTuple> updateOperationsUnordered;
    private final boolean storeDeweyIds;
    private final String databaseName;

    // TODO: Extract common stuff to reduce param count.
    public CommitTask(final String databaseName, final String commitMessage, final PageTrx pageTrx,
        final Semaphore commitLock, final Lock lock, final InternalResourceSession<R, W> resourceSession,
        final AbstractNodeHashing<?, ?> nodeHashing, final int beforeBulkInsertionRevisionNumber,
        final boolean isAutoCommitting, SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered,
        final Map<Long, DiffTuple> updateOperationsUnordered, final boolean storeDeweyIds) {
      this.databaseName = databaseName;
      this.commitMessage = commitMessage;
      this.pageTrx = pageTrx;
      this.commitLock = commitLock;
      this.lock = lock;
      this.resourceSession = resourceSession;
      this.revisionNumber = pageTrx.getRevisionNumber();
      this.nodeHashing = nodeHashing;
      this.beforeBulkInsertionRevisionNumber = beforeBulkInsertionRevisionNumber;
      this.isAutoCommitting = isAutoCommitting;
      this.updateOperationsOrdered = new TreeMap<>(updateOperationsOrdered);
      this.updateOperationsUnordered = new HashMap<>(updateOperationsUnordered);
      this.storeDeweyIds = storeDeweyIds;
    }

    public UberPage call() {
      UberPage uberPage = null;
      try {
        uberPage = commitMessage == null ? pageTrx.commit() : pageTrx.commit(commitMessage);

        if (lock != null) {
          lock.lock();
        }
        try {
          // Remember succesfully committed uber page in resource manager.
          resourceSession.setLastCommittedUberPage(uberPage);
        } finally {
          if (lock != null) {
            lock.unlock();
          }
        }

        if (resourceSession.getResourceConfig().storeDiffs()) {
          serializeUpdateDiffs();
        }

        pageTrx.close();
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        if (commitLock != null) {
          commitLock.release();
        }
      }

      return uberPage;
    }

    public void serializeUpdateDiffs() {
      if (resourceSession instanceof JsonResourceSession jsonResourceSession && !nodeHashing.isBulkInsert()
          && revisionNumber - 1 > 0) {
        final var diffSerializer = new JsonDiffSerializer(databaseName,
                                                          jsonResourceSession,
                                                          beforeBulkInsertionRevisionNumber != 0 && isAutoCommitting
                                                              ? beforeBulkInsertionRevisionNumber
                                                              : revisionNumber - 1,
                                                          revisionNumber,
                                                          storeDeweyIds
                                                              ? updateOperationsOrdered.values()
                                                              : updateOperationsUnordered.values());
        final var jsonDiff = diffSerializer.serialize(false);

        // Deserialize index definitions.
        final Path diff = resourceSession.getResourceConfig()
                                         .getResource()
                                         .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                         .resolve(
                                             "diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber + ".json");
        try {
          Files.createFile(diff);
          Files.writeString(diff, jsonDiff);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  @Override
  public W commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp) {
    nodeReadOnlyTrx.assertNotClosed();
    if (commitTimestamp != null && !resourceSession.getResourceConfig().customCommitTimestamps()) {
      throw new IllegalStateException("Custom commit timestamps are not enabled for the resource.");
    }

    try {
      commitLock.acquire();
    } catch (InterruptedException e) {
      throw new SirixRuntimeException(e.getMessage(), e);
    }

    try {
      runLocked(() -> {
        state = State.COMMITTING;

        // Execute pre-commit hooks.
        for (final PreCommitHook hook : preCommitHooks) {
          hook.preCommit(this);
        }

        // Reset modification counter.
        modificationCount = 0L;

        final var preCommitRevision = getRevisionNumber();

        final UberPage uberPage = pageTrx.commit(commitMessage, commitTimestamp);

        // Remember successfully committed uber page in resource manager.
        resourceSession.setLastCommittedUberPage(uberPage);

        if (resourceSession.getResourceConfig().storeDiffs()) {
          serializeUpdateDiffs(preCommitRevision);
        }

        // Reinstantiate everything.
        if (afterCommitState == AfterCommitState.KEEP_OPEN) {
          reInstantiate(getId(), preCommitRevision);
          //pageTrx.getLog().clear();
          state = State.RUNNING;
        } else {
          state = State.COMMITTED;
        }
      });
    } finally {
      commitLock.release();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }

    return self();
  }

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  protected void checkAccessAndCommit() {
    nodeReadOnlyTrx.assertNotClosed();
    assertRunning();
    modificationCount++;
    intermediateCommitIfRequired();
  }

  /**
   * Making an intermediate commit based on set attributes.
   *
   * @throws SirixException if commit fails
   */
  private void intermediateCommitIfRequired() {
    nodeReadOnlyTrx.assertNotClosed();
    if (maxNodeCount > 0 && modificationCount > maxNodeCount) {
      if (doAsyncCommit) {
        asyncCommit("autoCommit", null);
      } else {
        commit("autoCommit", null);
      }
    }
  }

  protected abstract void serializeUpdateDiffs(int revisionNumber);

  /**
   * Create new instances.
   *
   * @param trxID     transaction ID
   * @param revNumber revision number
   */
  private void reInstantiate(final @NonNegative long trxID, final @NonNegative int revNumber) {
    // Reset page transaction to new uber page.
    resourceSession.closeNodePageWriteTransaction(trxID);

    pageTrx = resourceSession.createPageTransaction(trxID,
                                                    revNumber,
                                                    revNumber,
                                                    InternalResourceSession.Abort.NO,
                                                    true,
                                                    null,
                                                    true);

    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
    resourceSession.setNodePageWriteTransaction(getId(), pageTrx);

    nodeFactory = reInstantiateNodeFactory(pageTrx);

    final boolean isBulkInsert = nodeHashing.isBulkInsert();
    nodeHashing = reInstantiateNodeHashing(pageTrx);
    nodeHashing.setBulkInsert(isBulkInsert);

    updateOperationsUnordered.clear();
    updateOperationsOrdered.clear();

    reInstantiateIndexes();
  }

  void reInstantiate(final @NonNegative long trxID, final @NonNegative int revNumber, final UberPage uberPage,
      final @NonNull TransactionIntentLog log) {
    if (uberPage == null) {
      pageTrx = resourceSession.createPageTransaction(trxID,
                                                      revNumber,
                                                      revNumber,
                                                      InternalResourceSession.Abort.NO,
                                                      true,
                                                      log,
                                                      doAsyncCommit);
    } else {
      pageTrx = resourceSession.createPageTransaction(trxID, revNumber, uberPage, true, log, doAsyncCommit);
    }

    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
    resourceSession.setNodePageWriteTransaction(getId(), pageTrx);

    nodeFactory = reInstantiateNodeFactory(pageTrx);
    final boolean isBulkInsert = nodeHashing.isBulkInsert();
    nodeHashing = reInstantiateNodeHashing(pageTrx);
    nodeHashing.setBulkInsert(isBulkInsert);

    updateOperationsUnordered.clear();
    updateOperationsOrdered.clear();

    reInstantiateIndexes();
  }

  protected abstract AbstractNodeHashing<N, R> reInstantiateNodeHashing(PageTrx pageTrx);

  protected abstract NF reInstantiateNodeFactory(PageTrx pageTrx);

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (buildPathSummary) {
      pathSummaryWriter = new PathSummaryWriter<>(pageTrx, resourceSession, nodeFactory, typeSpecificTrx);
    }

    // Recreate index listeners.
    final var indexDefs = indexController.getIndexes().getIndexDefs();
    indexController = resourceSession.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    indexController.createIndexListeners(indexDefs, self());

    nodeToRevisionsIndex.setPageTrx(pageTrx);
  }

  @Override
  public synchronized W rollback() {
    if (lock != null) {
      lock.lock();
    }

    nodeReadOnlyTrx.assertNotClosed();

    // Reset modification counter.
    modificationCount = 0L;

    // Close current page transaction.
    final long trxID = getId();
    final int revision = getRevisionNumber();
    final int revNumber = pageTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

    final UberPage uberPage = pageTrx.rollback();

    // Remember successfully committed uber page in resource manager.
    resourceSession.setLastCommittedUberPage(uberPage);

    resourceSession.closeNodePageWriteTransaction(getId());
    nodeReadOnlyTrx.setPageReadTransaction(null);
    removeCommitFile();

    pageTrx = resourceSession.createPageTransaction(trxID,
                                                    revNumber,
                                                    revNumber,
                                                    InternalResourceSession.Abort.YES,
                                                    true,
                                                    null,
                                                    false);
    nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
    resourceSession.setNodePageWriteTransaction(getId(), pageTrx);

    nodeFactory = reInstantiateNodeFactory(pageTrx);

    reInstantiateIndexes();

    if (lock != null) {
      lock.unlock();
    }

    return self();
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

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      resourceSession.closeNodePageWriteTransaction(getId());
      pageTrx = resourceSession.createPageTransaction(trxID,
                                                      revision,
                                                      revNumber - 1,
                                                      InternalResourceSession.Abort.NO,
                                                      true,
                                                      null,
                                                      false);
      nodeReadOnlyTrx.setPageReadTransaction(null);
      nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
      resourceSession.setNodePageWriteTransaction(getId(), pageTrx);

      nodeHashing = reInstantiateNodeHashing(pageTrx);

      // Reset node factory.
      nodeFactory = reInstantiateNodeFactory(pageTrx);

      // New index instances.
      reInstantiateIndexes();

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

  @Override
  public W truncateTo(final int revision) {
    nodeReadOnlyTrx.assertNotClosed();

    // TODO

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
  public PageTrx getPageWtx() {
    nodeReadOnlyTrx.assertNotClosed();
    return (PageTrx) nodeReadOnlyTrx.getPageTrx();
  }

  @Override
  public Optional<User> getUserOfRevisionToRepresent() {
    return nodeReadOnlyTrx.getUser();
  }

  @Override
  public void close() {
    runLocked(() -> {
      if (!isClosed()) {
        // Make sure to commit all dirty data.
        if (modificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Shutdown pool.
        commitScheduler.shutdown();
        try {
          boolean successful = commitScheduler.awaitTermination(5, TimeUnit.SECONDS);
          if (!successful) {
            throw new SirixThreadedException("Commit scheduler did not terminate in time.");
          }
        } catch (final InterruptedException e) {
          throw new SirixThreadedException(e);
        }

        // Release all state immediately.
        final long trxId = getId();
        nodeReadOnlyTrx.close();
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
    return getCurrentNode().getDeweyID();
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
    return MoreObjects.toStringHelper(this)
                      .add("super", super.toString())
                      .add("hashType", this.hashType)
                      .add("nodeReadOnlyTrx", this.nodeReadOnlyTrx)
                      .toString();
  }

  private static final class JsonNodeTrxThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@NonNull final Runnable runnable) {
      final var thread = new Thread(runnable, "JsonNodeTrxCommitThread");

      thread.setPriority(Thread.NORM_PRIORITY);
      thread.setDaemon(false);

      return thread;
    }
  }
}
