/**
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

package org.sirix.access.trx.node.xml;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalXmlDatabase;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.AbstractResourceManager;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.xml.XmlResourceStore;
import org.sirix.api.Database;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.cache.BufferManager;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.io.Storage;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * <h1>XdmResourceManager</h1>
 *
 * <p>
 * Makes sure that there only is a single resource manager instance per thread bound to a resource.
 * </p>
 */
public final class XmlResourceManagerImpl extends AbstractResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx>
    implements XmlResourceManager, InternalResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /** {@link XmlIndexController}s used for this session. */
  private final ConcurrentMap<Integer, XmlIndexController> mRtxIndexControllers;

  /** {@link XmlIndexController}s used for this session. */
  private final ConcurrentMap<Integer, XmlIndexController> mWtxIndexControllers;

  /**
   * Package private constructor.
   *
   * @param database {@link LocalXmlDatabase} for centralized operations on related sessions
   * @param resourceStore the resource store with which this manager has been created
   * @param resourceConf {@link DatabaseConfiguration} for general setting about the storage
   * @param pageCache the cache of in-memory pages shared amongst all sessions / resource transactions
   * @throws SirixException if Sirix encounters an exception
   */
  public XmlResourceManagerImpl(final Database<XmlResourceManager> database,
      final @Nonnull XmlResourceStore resourceStore, final @Nonnull ResourceConfiguration resourceConf,
      final @Nonnull BufferManager bufferManager, final @Nonnull Storage storage, final @Nonnull UberPage uberPage,
      final @Nonnull Semaphore readSemaphore, final @Nonnull Lock writeLock) {
    super(database, resourceStore, resourceConf, bufferManager, storage, uberPage, readSemaphore, writeLock);

    mRtxIndexControllers = new ConcurrentHashMap<>();
    mWtxIndexControllers = new ConcurrentHashMap<>();
  }

  @Override
  public XmlNodeReadOnlyTrx createNodeReadOnlyTrx(long nodeTrxId, PageReadOnlyTrx pageReadTrx, Node documentNode) {
    return new XmlNodeReadOnlyTrxImpl(this, nodeTrxId, pageReadTrx, (ImmutableXmlNode) documentNode);
  }

  @Override
  public XmlNodeTrx createNodeReadWriteTrx(long nodeTrxId,
      PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, int maxNodeCount, TimeUnit timeUnit, int maxTime,
      Node documentNode) {
    // The node read-only transaction.
    final InternalXmlNodeReadTrx nodeReadTrx =
        new XmlNodeReadOnlyTrxImpl(this, nodeTrxId, pageWriteTrx, (ImmutableXmlNode) documentNode);

    // Node factory.
    final XmlNodeFactory nodeFactory = new XmlNodeFactoryImpl(pageWriteTrx);

    // Path summary.
    final boolean buildPathSummary = getResourceConfig().withPathSummary;
    final PathSummaryWriter<XmlNodeReadOnlyTrx> pathSummaryWriter;
    if (buildPathSummary) {
      pathSummaryWriter = new PathSummaryWriter<>(pageWriteTrx, this, nodeFactory, nodeReadTrx);
    } else {
      pathSummaryWriter = null;
    }

    return new XmlNodeTrxImpl(nodeTrxId, this, nodeReadTrx, pathSummaryWriter, maxNodeCount, timeUnit, maxTime,
        documentNode, nodeFactory);
  }

  // TODO: Change for Java9 and above.
  @SuppressWarnings("unchecked")
  @Override
  public synchronized XmlIndexController getRtxIndexController(final int revision) {
    XmlIndexController controller = mRtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new XmlIndexController();
      mRtxIndexControllers.put(revision, controller);
    }
    return controller;
  }

  // TODO: Change for Java9 and above.
  @SuppressWarnings("unchecked")
  @Override
  public synchronized XmlIndexController getWtxIndexController(final int revision) {
    XmlIndexController controller = mWtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new XmlIndexController();
      mWtxIndexControllers.put(revision, controller);
    }
    return controller;
  }
}
