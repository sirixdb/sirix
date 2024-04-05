/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.node.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.User;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.trx.node.RecordToRevisionsIndex;
import io.sirix.access.trx.page.PageTrxFactory;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.dagger.DatabaseName;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.UberPage;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.io.IOStorage;

import javax.inject.Inject;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides node transactions on different revisions of JSON resources.
 */
public final class JsonResourceSessionImpl extends AbstractResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx>
    implements JsonResourceSession, InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /**
   * {@link JsonIndexController}s used for this session.
   */
  private final ConcurrentMap<Integer, JsonIndexController> rtxIndexControllers;

  /**
   * {@link JsonIndexController}s used for this session.
   */
  private final ConcurrentMap<Integer, JsonIndexController> wtxIndexControllers;

  /**
   * The name of the database on which this instance operates.
   */
  private final String databaseName;

  /**
   * Constructor.
   *
   * @param resourceStore  the resource store with which this manager has been created
   * @param resourceConf   {@link DatabaseConfiguration} for general setting about the storage
   * @param bufferManager  the cache of in-memory pages shared amongst all node transactions
   * @param storage        the storage itself, used for I/O
   * @param uberPage       the UberPage, which is the main entry point into a resource
   * @param writeLock      the write lock, which ensures, that only a single read-write transaction is
   *                       opened on a resource
   * @param user           a user, which interacts with SirixDB, might be {@code null}
   * @param pageTrxFactory A factory that creates new {@link PageTrx} instances.
   */
  @Inject
  JsonResourceSessionImpl(final ResourceStore<JsonResourceSession> resourceStore,
      final ResourceConfiguration resourceConf, final BufferManager bufferManager, final IOStorage storage,
      final UberPage uberPage, final Semaphore writeLock, final User user, @DatabaseName final String databaseName,
      final PageTrxFactory pageTrxFactory) {
    super(resourceStore, resourceConf, bufferManager, storage, uberPage, writeLock, user, pageTrxFactory);

    this.databaseName = databaseName;
    rtxIndexControllers = new ConcurrentHashMap<>();
    wtxIndexControllers = new ConcurrentHashMap<>();
  }

  @Override
  public InternalJsonNodeReadOnlyTrx createNodeReadOnlyTrx(long nodeTrxId, PageReadOnlyTrx pageReadTrx,
      Node documentNode) {
    return new JsonNodeReadOnlyTrxImpl(this, nodeTrxId, pageReadTrx, (ImmutableJsonNode) documentNode);
  }

  @Override
  public JsonNodeTrx createNodeReadWriteTrx(long nodeTrxId, PageTrx pageTrx, int maxNodeCount,
      Duration autoCommitDelay, Node documentNode, AfterCommitState afterCommitState, boolean doAsyncCommit) {
    // The node read-only transaction.
    final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx = createNodeReadOnlyTrx(nodeTrxId, pageTrx, documentNode);

    // Node factory.
    final JsonNodeFactory nodeFactory = new JsonNodeFactoryImpl(getResourceConfig().nodeHashFunction, pageTrx);

    // Path summary.
    final boolean buildPathSummary = getResourceConfig().withPathSummary;
    final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter;
    if (buildPathSummary) {
      pathSummaryWriter = new PathSummaryWriter<>(pageTrx, this, nodeFactory, nodeReadOnlyTrx);
    } else {
      pathSummaryWriter = null;
    }

    // Synchronize commit and other public methods if needed.
    final var isAutoCommitting = maxNodeCount > 0 || !autoCommitDelay.isZero();
    final var lock = !autoCommitDelay.isZero() ? new ReentrantLock() : null;
    final var resourceConfig = getResourceConfig();
    return new JsonNodeTrxImpl(this.databaseName,
                               this,
                               nodeReadOnlyTrx,
                               pathSummaryWriter,
                               maxNodeCount,
                               lock,
                               autoCommitDelay,
                               new JsonNodeHashing(resourceConfig, nodeReadOnlyTrx, pageTrx),
                               nodeFactory,
                               afterCommitState,
                               new RecordToRevisionsIndex(pageTrx),
                               isAutoCommitting,
                               doAsyncCommit);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized JsonIndexController getRtxIndexController(final int revision) {
    return rtxIndexControllers.computeIfAbsent(revision, unused -> createIndexController(revision));
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized JsonIndexController getWtxIndexController(final int revision) {
    return wtxIndexControllers.computeIfAbsent(revision, unused -> createIndexController(revision));
  }

  private JsonIndexController createIndexController(int revision) {
    final var controller = new JsonIndexController();
    initializeIndexController(revision, controller);
    return controller;
  }
}
