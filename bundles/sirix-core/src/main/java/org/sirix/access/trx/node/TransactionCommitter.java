package org.sirix.access.trx.node;

import org.sirix.access.ResourceConfiguration;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.diff.DiffTuple;
import org.sirix.diff.JsonDiffSerializer;
import org.sirix.exception.SirixThreadedException;
import org.sirix.node.SirixDeweyID;
import org.sirix.page.UberPage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * TODO: Class TransactionCommitter's description.
 *
 * @author Joao Sousa
 */
public class TransactionCommitter implements AutoCloseable {

    private final ScheduledExecutorService commitScheduler;
    private final InternalResourceManager<?, ?> resourceManager;
    private final String databaseName;
    private final boolean isAutoCommitting; // TODO might require moving 'bind' into the constructor

    public TransactionCommitter(final ThreadFactory threadFactory,
                                final InternalResourceManager<?, ?> resourceManager,
                                final String databaseName,
                                final boolean isAutoCommitting) {
        this.commitScheduler = newSingleThreadScheduledExecutor(threadFactory);
        this.resourceManager = resourceManager;
        this.databaseName = databaseName;
        this.isAutoCommitting = isAutoCommitting;
    }

    public Optional<ScheduledFuture<?>> bind(final NodeTrx transaction, final Duration autoCommitDelay) {
        checkArgument(!autoCommitDelay.isNegative(), "Auto commit delay cannot be negative");

        if (!autoCommitDelay.isZero()) {
            return Optional.of(this.commitScheduler.scheduleWithFixedDelay(() -> transaction.commit("autoCommit"),
                    autoCommitDelay.getSeconds(),
                    autoCommitDelay.getSeconds(),
                    TimeUnit.SECONDS
            ));
        } else {
            return Optional.empty();
        }
    }

    Future<?> commitAsync(final String commitMessage,
                          final int preCommitRevision,
                          final PageTrx pageTrx,
                          final AbstractNodeHashing<?> nodeHashing,
                          final int beforeBulkInsertionRevisionNumber,
                          final SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered,
                          final Map<Long, DiffTuple> updateOperationsUnordered,
                          final boolean storeDeweyIDs) {

        return this.commitScheduler.submit(() -> {

            final UberPage uberPage = pageTrx.commit(commitMessage);

            // Remember successfully committed uber page in resource manager.
            resourceManager.setLastCommittedUberPage(uberPage);

            if (resourceManager.getResourceConfig().storeDiffs()) {
                serializeUpdateDiffs(
                        preCommitRevision,
                        nodeHashing,
                        beforeBulkInsertionRevisionNumber,
                        updateOperationsOrdered,
                        updateOperationsUnordered,
                        storeDeweyIDs
                );
            }
        });
    }

    private void serializeUpdateDiffs(final int revisionNumber,
                                      final AbstractNodeHashing<?> nodeHashing,
                                      final int beforeBulkInsertionRevisionNumber,
                                      final SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered,
                                      final Map<Long, DiffTuple> updateOperationsUnordered,
                                      final boolean storeDeweyIDs) {
        if (!nodeHashing.isBulkInsert() && revisionNumber - 1 > 0) {
            final var diffSerializer = new JsonDiffSerializer(this.databaseName, (JsonResourceManager) resourceManager,
                    beforeBulkInsertionRevisionNumber != 0 && isAutoCommitting
                            ? beforeBulkInsertionRevisionNumber
                            : revisionNumber - 1,
                    revisionNumber,
                    storeDeweyIDs
                            ? updateOperationsOrdered.values()
                            : updateOperationsUnordered.values());
            final var jsonDiff = diffSerializer.serialize(false);

            // Deserialize index definitions.
            final Path diff = resourceManager.getResourceConfig()
                    .getResource()
                    .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                    .resolve(
                            "diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber + ".json");
            try {
                Files.writeString(diff, jsonDiff, CREATE);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {
        this.commitScheduler.shutdown();
        try {
            this.commitScheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new SirixThreadedException(e);
        }
    }
}
