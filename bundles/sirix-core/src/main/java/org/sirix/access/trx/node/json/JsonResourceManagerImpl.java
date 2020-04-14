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

package org.sirix.access.trx.node.json;

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.json.JsonResourceStore;
import org.sirix.access.trx.node.AbstractResourceManager;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.Database;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.io.Storage;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Provides node transactions on different revisions of JSON resources.
 */
public final class JsonResourceManagerImpl extends AbstractResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx>
    implements JsonResourceManager, InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /** {@link XmlIndexController}s used for this session. */
  private final ConcurrentMap<Integer, JsonIndexController> rtxIndexControllers;

  /** {@link XmlIndexController}s used for this session. */
  private final ConcurrentMap<Integer, JsonIndexController> wtxIndexControllers;

  /**
   * Constructor.
   *
   * @param database {@link Database} for centralized operations on related sessions
   * @param resourceStore the resource store with which this manager has been created
   * @param resourceConf {@link DatabaseConfiguration} for general setting about the storage
   * @param bufferManager the cache of in-memory pages shared amongst all node transactions
   * @param storage the storage itself, used for I/O
   * @param uberPage the UberPage, which is the main entry point into a resource
   * @param writeLock the write lock, which ensures, that only a single read-write transaction is
   *        opened on a resource
   * @param user a user, which interacts with SirixDB, might be {@code null}
   */
  public JsonResourceManagerImpl(final Database<JsonResourceManager> database,
      final @Nonnull JsonResourceStore resourceStore, final @Nonnull ResourceConfiguration resourceConf,
      final @Nonnull BufferManager bufferManager, final @Nonnull Storage storage, final @Nonnull UberPage uberPage, final @Nonnull Lock writeLock, final @Nullable User user) {
    super(database, resourceStore, resourceConf, bufferManager, storage, uberPage, writeLock, user);

    rtxIndexControllers = new ConcurrentHashMap<>();
    wtxIndexControllers = new ConcurrentHashMap<>();
  }

  @Override
  public JsonNodeReadOnlyTrx createNodeReadOnlyTrx(long nodeTrxId, PageReadOnlyTrx pageReadTrx, Node documentNode) {
    return new JsonNodeReadOnlyTrxImpl(this, nodeTrxId, pageReadTrx, (ImmutableJsonNode) documentNode);
  }

  @Override
  public JsonNodeTrx createNodeReadWriteTrx(long nodeTrxId, PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx,
      int maxNodeCount, TimeUnit timeUnit, int maxTime, Node documentNode) {
    // The node read-only transaction.
    final InternalJsonNodeReadOnlyTrx nodeReadTrx =
        new JsonNodeReadOnlyTrxImpl(this, nodeTrxId, pageWriteTrx, (ImmutableJsonNode) documentNode);

    // Node factory.
    final JsonNodeFactory nodeFactory = new JsonNodeFactoryImpl(getResourceConfig().nodeHashFunction, pageWriteTrx);

    // Path summary.
    final boolean buildPathSummary = getResourceConfig().withPathSummary;
    final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter;
    if (buildPathSummary) {
      pathSummaryWriter = new PathSummaryWriter<>(pageWriteTrx, this, nodeFactory, nodeReadTrx);
    } else {
      pathSummaryWriter = null;
    }

    return new JsonNodeTrxImpl(this, nodeReadTrx, pathSummaryWriter, maxNodeCount, timeUnit, maxTime,
        new JsonNodeHashing(getResourceConfig().hashType, nodeReadTrx, pageWriteTrx), nodeFactory);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized JsonIndexController getRtxIndexController(final int revision) {
    return rtxIndexControllers.computeIfAbsent(revision, unused -> new JsonIndexController());
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized JsonIndexController getWtxIndexController(final int revision) {
    return wtxIndexControllers.computeIfAbsent(revision, unused -> {
      final var controller = new JsonIndexController();
      initializeIndexController(revision, controller);
      return controller;
    });
  }
}
