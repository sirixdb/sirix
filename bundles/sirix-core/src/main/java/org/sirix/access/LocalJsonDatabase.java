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

package org.sirix.access;

import com.google.common.base.MoreObjects;
import org.sirix.access.json.JsonResourceStore;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.cache.BufferManagerImpl;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class represents one concrete database for enabling several {@link ResourceManager}
 * instances.
 *
 * @see Database
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class LocalJsonDatabase extends AbstractLocalDatabase<JsonResourceManager> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(LocalJsonDatabase.class));

  /** The resource store to open/close resource-managers. */
  private final JsonResourceStore mResourceStore;

  /**
   * Package private constructor.
   *
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   * @throws SirixException if something weird happens
   */
  LocalJsonDatabase(final DatabaseConfiguration dbConfig, final JsonResourceStore store) {
    super(dbConfig);
    mResourceStore = store;
  }

  @Override
  public synchronized void close() throws SirixException {
    if (mClosed)
      return;

    mClosed = true;
    mResourceStore.close();
    mTransactionManager.close();

    // Remove from database mapping.
    Databases.removeDatabase(mDBConfig.getFile(), this);

    // Remove lock file.
    SirixFiles.recursiveRemove(mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()));
  }

  @Override
  public synchronized JsonResourceManager openResourceManager(final String resource) {
    assertNotClosed();

    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resource);

    if (!Files.exists(resourceFile)) {
      throw new SirixUsageException("Resource could not be opened (since it was not created?) at location",
          resourceFile.toString());
    }

    if (mResourceStore.hasOpenResourceManager(resourceFile))
      return mResourceStore.getOpenResourceManager(resourceFile);

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourceFile);

    // Resource of must be associated to this database.
    assert resourceConfig.resourcePath.getParent().getParent().equals(mDBConfig.getFile());

    // Keep track of the resource-ID.
    mResources.forcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    if (!mBufferManagers.containsKey(resourceFile))
      mBufferManagers.put(resourceFile, new BufferManagerImpl());

    final JsonResourceManager resourceManager =
        mResourceStore.openResource(this, resourceConfig, mBufferManagers.get(resourceFile), resourceFile);

    return resourceManager;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dbConfig", mDBConfig).toString();
  }

  @Override
  protected boolean bootstrapResource(ResourceConfiguration resConfig) {
    boolean returnVal = true;

    try (
        final JsonResourceManager resourceTrxManager =
            openResourceManager(resConfig.getResource().getFileName().toString());
        final JsonNodeTrx wtx = resourceTrxManager.beginNodeTrx()) {
      wtx.commit();
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
      returnVal = false;
    }

    return returnVal;
  }
}
