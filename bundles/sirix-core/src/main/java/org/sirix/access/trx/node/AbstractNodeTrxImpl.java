package org.sirix.access.trx.node;

import com.google.common.base.MoreObjects;
import org.sirix.access.User;
import org.sirix.access.trx.node.InternalResourceManager.Abort;
import org.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import org.sirix.api.*;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.diff.DiffTuple;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UberPage;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.Files.deleteIfExists;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Abstract implementation for {@link InternalNodeTrx}.
 *
 * @author Joao Sousa
 */
public abstract class AbstractNodeTrxImpl<R extends NodeReadOnlyTrx & NodeCursor,
        W extends NodeTrx & NodeCursor,
        NF extends NodeFactory,
        N extends ImmutableNode,
        IN extends InternalNodeReadOnlyTrx<N>>
        implements NodeReadOnlyTrx, InternalNodeTrx<W>, NodeCursor {

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
    protected final InternalResourceManager<R, W> resourceManager;

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
    private final Lock lock;

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

    /**
     * Hashes nodes.
     */
    protected AbstractNodeHashing<N> nodeHashing;

    /**
     * The transaction states.
     */
    private enum State {
        Running,

        Committing,

        Committed,

        Closed

    }

    protected AbstractNodeTrxImpl(final ThreadFactory threadFactory,
                                  final HashType hashType,
                                  final IN nodeReadOnlyTrx,
                                  final R typeSpecificTrx,
                                  final InternalResourceManager<R, W> resourceManager,
                                  final AfterCommitState afterCommitState,
                                  final AbstractNodeHashing<N> nodeHashing,
                                  final PathSummaryWriter<R> pathSummaryWriter,
                                  final NF nodeFactory,
                                  final RecordToRevisionsIndex nodeToRevisionsIndex,
                                  @Nullable final Lock transactionLock,
                                  final Duration afterCommitDelay,
                                  @Nonnegative final int maxNodeCount) {

        // Do not accept negative values.
        checkArgument(maxNodeCount >= 0, "Negative argument for maxNodeCount is not accepted.");
        checkArgument(!afterCommitDelay.isNegative(), "After commit delay cannot be negative");
        this.commitScheduler = newScheduledThreadPool(1, threadFactory);
        this.hashType = hashType;
        this.nodeReadOnlyTrx = checkNotNull(nodeReadOnlyTrx);
        this.typeSpecificTrx = typeSpecificTrx;
        this.resourceManager = checkNotNull(resourceManager);
        this.lock = transactionLock;
        this.afterCommitState = checkNotNull(afterCommitState);
        this.nodeHashing = checkNotNull(nodeHashing);
        this.buildPathSummary = resourceManager.getResourceConfig().withPathSummary;
        this.nodeFactory = checkNotNull(nodeFactory);
        this.pathSummaryWriter = pathSummaryWriter;
        this.indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
        this.nodeToRevisionsIndex = checkNotNull(nodeToRevisionsIndex);

        this.updateOperationsOrdered = new TreeMap<>();
        this.updateOperationsUnordered = new HashMap<>();

        this.pageTrx = (PageTrx) nodeReadOnlyTrx.getPageTrx();

        // Only auto commit by node modifications if it is more then 0.
        this.maxNodeCount = maxNodeCount;
        this.modificationCount = 0L;

        this.state = State.Running;

        if (!afterCommitDelay.isZero()) {
            commitScheduler.scheduleWithFixedDelay(() -> commit("autoCommit", null),
                    afterCommitDelay.toMillis(),
                    afterCommitDelay.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        // // Redo last transaction if the system crashed.
        // if (!pPageWriteTrx.isCreated()) {
        // try {
        // commit();
        // } catch (final SirixException e) {
        // throw new IllegalStateException(e);
        // }
        // }
    }

    protected abstract W self();

    protected void assertRunning() {
        if (state != State.Running) {
            throw new IllegalStateException("Transaction state is not running: " + state);
        }
    }

    @Override
    public Optional<User> getUser() {
        return getResourceManager().getUser();
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
        new PostOrderAxis(this, IncludeSelf.YES)
                .forEach((unused) -> nodeHashing.addHashAndDescendantCount());
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
        try {
            runnable.run();
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    protected <T> T supplyLocked(final Supplier<T> supplier) {
        if (lock != null) {
            lock.lock();
        }
        try {
            return supplier.get();
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    @Override
    public W commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp) {
        nodeReadOnlyTrx.assertNotClosed();

        runLocked(() -> {
            state = State.Committing;

            // Execute pre-commit hooks.
            for (final PreCommitHook hook : preCommitHooks) {
                hook.preCommit(this);
            }

            // Reset modification counter.
            modificationCount = 0L;

            final var preCommitRevision = getRevisionNumber();

            final UberPage uberPage = pageTrx.commit(commitMessage, commitTimestamp);

            // Remember successfully committed uber page in resource manager.
            resourceManager.setLastCommittedUberPage(uberPage);

            if (resourceManager.getResourceConfig().storeDiffs()) {
                serializeUpdateDiffs(preCommitRevision);
            }

            // Reinstantiate everything.
            if (afterCommitState == AfterCommitState.KeepOpen) {
                reInstantiate(getId(), preCommitRevision);
                state = State.Running;
            } else {
                state = State.Committed;
            }
        });

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
        commit("autoCommit");
      }
    }

    protected abstract void serializeUpdateDiffs(int revisionNumber);

    /**
     * Create new instances.
     *
     * @param trxID     transaction ID
     * @param revNumber revision number
     */
    private void reInstantiate(final @Nonnegative long trxID, final @Nonnegative int revNumber) {
        // Reset page transaction to new uber page.
        resourceManager.closeNodePageWriteTransaction(getId());
        pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.NO, true);
        nodeReadOnlyTrx.setPageReadTransaction(null);
        nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
        resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

        nodeFactory = reInstantiateNodeFactory(pageTrx);

        final boolean isBulkInsert = nodeHashing.isBulkInsert();
        nodeHashing = reInstantiateNodeHashing(hashType, pageTrx);
        nodeHashing.setBulkInsert(isBulkInsert);

        updateOperationsUnordered.clear();
        updateOperationsOrdered.clear();

        reInstantiateIndexes();
    }

    protected abstract AbstractNodeHashing<N> reInstantiateNodeHashing(HashType hashType, PageTrx pageTrx);

    protected abstract NF reInstantiateNodeFactory(PageTrx pageTrx);

    private void reInstantiateIndexes() {
        // Get a new path summary instance.
        if (buildPathSummary) {
            pathSummaryWriter = new PathSummaryWriter<>(
                    pageTrx,
                    resourceManager,
                    nodeFactory,
                    typeSpecificTrx
            );
        }

        // Recreate index listeners.
        final var indexDefs = indexController.getIndexes().getIndexDefs();
        indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
        indexController.createIndexListeners(indexDefs, self());

        nodeToRevisionsIndex.setPageTrx(pageTrx);
    }

    @Override
    public synchronized W rollback() {
        return supplyLocked(() -> {
            nodeReadOnlyTrx.assertNotClosed();

            // Reset modification counter.
            modificationCount = 0L;

            // Close current page transaction.
            final long trxID = getId();
            final int revision = getRevisionNumber();
            final int revNumber = pageTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

            final UberPage uberPage = pageTrx.rollback();

            // Remember successfully committed uber page in resource manager.
            resourceManager.setLastCommittedUberPage(uberPage);

            resourceManager.closeNodePageWriteTransaction(getId());
            nodeReadOnlyTrx.setPageReadTransaction(null);
            removeCommitFile();

            pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.YES, true);
            nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
            resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

            nodeFactory = reInstantiateNodeFactory(pageTrx);

            reInstantiateIndexes();

            return self();
        });
    }

    private void removeCommitFile() {
        try {
            deleteIfExists(resourceManager.getCommitFile());
        } catch (final IOException e) {
            throw new SirixIOException(e);
        }
    }

    @Override
    public W revertTo(final int revision) {
        return supplyLocked(() -> {
            nodeReadOnlyTrx.assertNotClosed();
            resourceManager.assertAccess(revision);

            // Close current page transaction.
            final long trxID = getId();
            final int revNumber = getRevisionNumber();

            // Reset internal transaction state to new uber page.
            resourceManager.closeNodePageWriteTransaction(getId());
            pageTrx = resourceManager.createPageTransaction(
                    trxID,
                    revision,
                    revNumber - 1,
                    Abort.NO,
                    true
            );
            nodeReadOnlyTrx.setPageReadTransaction(null);
            nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
            resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

            nodeHashing = reInstantiateNodeHashing(hashType, pageTrx);

            // Reset node factory.
            nodeFactory = reInstantiateNodeFactory(pageTrx);

            // New index instances.
            reInstantiateIndexes();

            // Reset modification counter.
            modificationCount = 0L;

            // Move to document root.
            moveToDocumentRoot();

            return self();
        });
    }

    @Override
    public W addPreCommitHook(final PreCommitHook hook) {
        return supplyLocked(() -> {
            preCommitHooks.add(checkNotNull(hook));
            return self();
        });
    }

    @Override
    public W addPostCommitHook(final PostCommitHook hook) {
        return supplyLocked(() -> {
            postCommitHooks.add(checkNotNull(hook));
            return self();
        });
    }

    @Override
    public W truncateTo(final int revision) {
        nodeReadOnlyTrx.assertNotClosed();

        // TODO

        return self();
    }

    @Override
    public PathSummaryReader getPathSummary() {
        return supplyLocked(pathSummaryWriter::getPathSummary);
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
    public synchronized void close() {
        runLocked(() -> {
            if (!isClosed()) {
                // Make sure to commit all dirty data.
                if (modificationCount > 0) {
                    throw new SirixUsageException("Must commit/rollback transaction first!");
                }

                // Release all state immediately.
                final long trxId = getId();
                nodeReadOnlyTrx.close();
                resourceManager.closeWriteTransaction(trxId);
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
}
