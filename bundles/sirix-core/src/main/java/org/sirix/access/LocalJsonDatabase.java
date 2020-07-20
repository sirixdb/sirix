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
import org.sirix.io.StorageType;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class represents one concrete database for enabling several {@link ResourceManager}
 * instances.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 * @see Database
 */
public final class LocalJsonDatabase extends AbstractLocalDatabase<JsonResourceManager> {

  /**
   * {@link LogWrapper} reference.
   */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(LocalJsonDatabase.class));

  /**
   * The resource store to open/close resource-managers.
   */
  private final JsonResourceStore resourceStore;

  /**
   * Package private constructor.
   *
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   * @throws SirixException if something weird happens
   */
  LocalJsonDatabase(final DatabaseConfiguration dbConfig, final JsonResourceStore store) {
    super(dbConfig);
    resourceStore = store;
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      return;
    }

    isClosed = true;
    resourceStore.close();
    transactionManager.close();

    // Remove from database mapping.
    Databases.removeDatabase(dbConfig.getFile(), this);

    // Remove lock file.
    SirixFiles.recursiveRemove(dbConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()));
  }

  @Override
  public synchronized JsonResourceManager openResourceManager(final String resource) {
    assertNotClosed();

    final Path resourceFile =
        dbConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resource);

    if (!Files.exists(resourceFile)) {
      throw new SirixUsageException("Resource could not be opened (since it was not created?) at location",
                                    resourceFile.toString());
    }

    if (resourceStore.hasOpenResourceManager(resourceFile)) {
      return resourceStore.getOpenResourceManager(resourceFile);
    }

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourceFile);

    // Resource of must be associated to this database.
    assert resourceConfig.resourcePath.getParent().getParent().equals(dbConfig.getFile());

    // Keep track of the resource-ID.
    resourceIDsToResourceNames.forcePut(resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    if (!bufferManagers.containsKey(resourceFile)) {
      addResourceToBufferManagerMapping(resourceFile, resourceConfig);
    }

    return resourceStore.openResource(this, resourceConfig, bufferManagers.get(resourceFile), resourceFile);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dbConfig", dbConfig).toString();
  }

  @Override
  protected boolean bootstrapResource(ResourceConfiguration resConfig) {
    boolean returnVal = true;

    try (final JsonResourceManager resourceTrxManager = openResourceManager(resConfig.getResource()
                                                                                     .getFileName()
                                                                                     .toString());
         final JsonNodeTrx wtx = resourceTrxManager.beginNodeTrx()) {
      wtx.commit();
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
      returnVal = false;
    }

    return returnVal;
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }
}
