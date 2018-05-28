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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.trx.TransactionManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.cache.BufferManager;
import org.sirix.cache.BufferManagerImpl;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;
import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

/**
 * This class represents one concrete database for enabling several {@link ResourceManager}
 * instances.
 *
 * @see Database
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class DatabaseImpl implements Database {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(DatabaseImpl.class));

  /** Unique ID of a resource. */
  private final AtomicLong mResourceID = new AtomicLong();

  /** The resource store to open/close resource-managers. */
  private final ResourceStore mResourceStore;

  /** Buffers / page cache for each resource. */
  private final ConcurrentMap<Path, BufferManager> mBufferManagers;

  /** Central repository of all resource-ID/resource-name tuples. */
  private final BiMap<Long, String> mResources;

  /** DatabaseConfiguration with fixed settings. */
  private final DatabaseConfiguration mDBConfig;

  /** The transaction manager. */
  private final TransactionManager mTransactionManager;

  /** Determines if the database instance is in the closed state or not. */
  private boolean mClosed;

  /**
   * Package private constructor.
   *
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   * @throws SirixException if something weird happens
   */
  DatabaseImpl(final DatabaseConfiguration dbConfig) {
    mDBConfig = checkNotNull(dbConfig);
    mResources = Maps.synchronizedBiMap(HashBiMap.create());
    mBufferManagers = new ConcurrentHashMap<>();
    mResourceStore = new ResourceStore();
    mTransactionManager = new TransactionManagerImpl();
  }

  // //////////////////////////////////////////////////////////
  // START Creation/Deletion of Resources /////////////////////
  // //////////////////////////////////////////////////////////

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resConfig) {
    assertNotClosed();

    boolean returnVal = true;
    final Path path =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(
            resConfig.mPath);
    // If file is existing, skip.
    if (Files.exists(path)) {
      return false;
    } else {
      try {
        Files.createDirectory(path);
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }

      if (returnVal) {
        // Creation of the folder structure.
        try {
          for (final ResourceConfiguration.ResourcePaths resourcePath : ResourceConfiguration.ResourcePaths.values()) {
            final Path toCreate = path.resolve(resourcePath.getPath());

            if (resourcePath.isFolder()) {
              Files.createDirectory(toCreate);
            } else {
              Files.createFile(toCreate);
            }

            if (!returnVal)
              break;
          }
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
          returnVal = false;
        }
      }
    }

    if (returnVal) {
      // If everything was correct so far, initialize storage.

      // Serialization of the config.
      mResourceID.set(mDBConfig.getMaxResourceID());
      ResourceConfiguration.serialize(resConfig.setID(mResourceID.getAndIncrement()));
      mDBConfig.setMaximumResourceID(mResourceID.get());
      mResources.forcePut(mResourceID.get(), resConfig.getResource().getFileName().toString());

      try {
        try (
            final ResourceManager resourceTrxManager =
                this.getResourceManager(resConfig.getResource().getFileName().toString());
            final XdmNodeWriteTrx wtx = resourceTrxManager.beginNodeWriteTrx()) {
          wtx.commit();
        }
      } catch (final SirixException e) {
        LOGWRAPPER.error(e.getMessage(), e);
        returnVal = false;
      }
    }

    if (!returnVal) {
      // If something was not correct, delete the partly created substructure.
      SirixFiles.recursiveRemove(resConfig.mPath);
    }

    return returnVal;
  }

  @Override
  public synchronized Database removeResource(final String name) {
    assertNotClosed();

    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(
            name);
    // Check that no running resource managers / sessions are opened.
    if (Databases.hasOpenResourceManagers(resourceFile)) {
      throw new IllegalStateException("Opened resource managers found, must be closed first.");
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile)
        && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0) {
      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      // mReadSemaphores.remove(resourceFile);
      // mWriteSemaphores.remove(resourceFile);
      mBufferManagers.remove(resourceFile);
    }

    return this;
  }

  // //////////////////////////////////////////////////////////
  // END Creation/Deletion of Resources ///////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START resource name <=> ID handling //////////////////////
  // //////////////////////////////////////////////////////////

  @Override
  public synchronized String getResourceName(final @Nonnegative long id) {
    assertNotClosed();
    checkArgument(id >= 0, "pID must be >= 0!");
    return mResources.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    return mResources.inverse().get(checkNotNull(name));
  }

  // //////////////////////////////////////////////////////////
  // END resource name <=> ID handling ////////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START DB-Operations //////////////////////////////////////
  // //////////////////////////////////////////////////////////

  @Override
  public synchronized ResourceManager getResourceManager(final String resource)
      throws SirixException {
    assertNotClosed();

    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(
            resource);

    if (!Files.exists(resourceFile)) {
      throw new SirixUsageException(
          "Resource could not be opened (since it was not created?) at location",
          resourceFile.toString());
    }

    if (mResourceStore.hasOpenResourceManager(resourceFile))
      return mResourceStore.getOpenResourceManager(resourceFile);

    final ResourceConfiguration resourceConfig = ResourceConfiguration.deserialize(resourceFile);

    // Resource of must be associated to this database.
    assert resourceConfig.mPath.getParent().getParent().equals(mDBConfig.getFile());

    // Keep track of the resource-ID.
    mResources.forcePut(
        resourceConfig.getID(), resourceConfig.getResource().getFileName().toString());

    if (!mBufferManagers.containsKey(resourceFile))
      mBufferManagers.put(resourceFile, new BufferManagerImpl());

    final ResourceManager resourceManager = mResourceStore.openResource(
        this, resourceConfig, mBufferManagers.get(resourceFile), resourceFile);

    return resourceManager;
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
    SirixFiles.recursiveRemove(
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()));
  }

  private void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Database is already closed.");
    }
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    assertNotClosed();
    return mDBConfig;
  }

  @Override
  public synchronized boolean existsResource(final String resourceName) {
    assertNotClosed();
    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(
            resourceName);
    return Files.exists(resourceFile)
        && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0
            ? true
            : false;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    try (final Stream<Path> stream = Files.list(
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.collect(Collectors.toList());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  // //////////////////////////////////////////////////////////
  // END DB-Operations ////////////////////////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START general methods ////////////////////////////////////
  // //////////////////////////////////////////////////////////

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dbConfig", mDBConfig).toString();
  }

  // //////////////////////////////////////////////////////////
  // END general methods //////////////////////////////////////
  // //////////////////////////////////////////////////////////

  BufferManager getPageCache(final Path resourceFile) {
    return mBufferManagers.get(resourceFile);
  }

  @Override
  public Transaction beginTransaction() {
    return null;
  }
}
