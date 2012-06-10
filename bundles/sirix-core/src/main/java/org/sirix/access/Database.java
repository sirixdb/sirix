/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.IConfigureSerializable;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.exception.TTUsageException;
import org.sirix.io.EStorage;

/**
 * This class represents one concrete database for enabling several {@link ISession} objects.
 * 
 * @see IDatabase
 * @author Sebastian Graf, University of Konstanz
 */
public final class Database implements IDatabase {

  /** Central repository of all running databases. */
  private static final ConcurrentMap<File, Database> DATABASEMAP = new ConcurrentHashMap<>();

  /** Central repository of all running sessions. */
  private final ConcurrentMap<File, Session> mSessions;

  /** DatabaseConfiguration with fixed settings. */
  private final DatabaseConfiguration mDBConfig;

  /**
   * Private constructor.
   * 
   * @param pDBConf
   *          {@link ResourceConfiguration} reference to configure the {@link IDatabase}
   * @throws AbsTTException
   *           if something weird happens
   */
  private Database(@Nonnull final DatabaseConfiguration pDBConf) throws AbsTTException {
    mDBConfig = checkNotNull(pDBConf);
    mSessions = new ConcurrentHashMap<>();
  }

  // //////////////////////////////////////////////////////////
  // START Creation/Deletion of Databases /////////////////////
  // //////////////////////////////////////////////////////////
  /**
   * Creating a database. This includes loading the database configuration,
   * building up the structure and preparing everything for login.
   * 
   * @param pDBConfig
   *          which are used for the database, including storage location
   * @return true if creation is valid, false otherwise
   * @throws TTIOException
   *           if something odd happens within the creation process.
   */
  public static synchronized boolean createDatabase(@Nonnull final DatabaseConfiguration pDBConfig)
    throws TTIOException {
    boolean returnVal = true;
    // if file is existing, skipping
    if (pDBConfig.getFile().exists()) {
      return false;
    } else {
      returnVal = pDBConfig.getFile().mkdirs();
      if (returnVal) {
        // creation of folder structure
        for (DatabaseConfiguration.Paths paths : DatabaseConfiguration.Paths.values()) {
          final File toCreate = new File(pDBConfig.getFile().getAbsoluteFile(), paths.getFile().getName());
          if (paths.isFolder()) {
            returnVal = toCreate.mkdir();
          } else {
            try {
              returnVal = toCreate.createNewFile();
            } catch (final IOException exc) {
              throw new TTIOException(exc);
            }
          }
          if (!returnVal) {
            break;
          }
        }
      }
      // serialization of the config
      try {
        serializeConfiguration(pDBConfig);
      } catch (final IOException exc) {
        throw new TTIOException(exc);
      }
      // if something was not correct, delete the partly created
      // substructure
      if (!returnVal) {
        pDBConfig.getFile().delete();
      }
      return returnVal;
    }
  }

  /**
   * Truncate a database. This deletes all relevant data. All running sessions
   * must be closed beforehand.
   * 
   * @param pConf
   *          the database at this path should be deleted.
   * @throws AbsTTException
   *           any kind of false sirix behaviour
   */
  public static synchronized void truncateDatabase(@Nonnull final DatabaseConfiguration pConf)
    throws AbsTTException {
    // check that database must be closed beforehand
    if (!DATABASEMAP.containsKey(pConf.getFile())) {
      // if file is existing and folder is a tt-dataplace, delete it
      if (pConf.getFile().exists() && DatabaseConfiguration.Paths.compareStructure(pConf.getFile()) == 0) {
        // instantiate the database for deletion
        EStorage.recursiveDelete(pConf.getFile());
      }
    }
  }

  // //////////////////////////////////////////////////////////
  // END Creation/Deletion of Databases ///////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START Creation/Deletion of Resources /////////////////////
  // //////////////////////////////////////////////////////////

  @Override
  public synchronized boolean createResource(@Nonnull final ResourceConfiguration pResConf)
    throws TTIOException {
    boolean returnVal = true;
    // Setting the missing parameters in the settings, this overrides already
    // set data.
    final File path =
      new File(new File(mDBConfig.getFile().getAbsoluteFile(), DatabaseConfiguration.Paths.Data.getFile()
        .getName()), pResConf.mPath.getName());
    // if file is existing, skipping
    if (path.exists()) {
      return false;
    } else {
      returnVal = path.mkdir();
      if (returnVal) {
        // creation of the folder structure
        for (ResourceConfiguration.Paths paths : ResourceConfiguration.Paths.values()) {
          final File toCreate = new File(path, paths.getFile().getName());
          if (paths.isFolder()) {
            returnVal = toCreate.mkdir();
          } else {
            try {
              returnVal = toCreate.createNewFile();
            } catch (final IOException exc) {
              throw new TTIOException(exc);
            }
          }
          if (!returnVal) {
            break;
          }
        }
      }
      // serialization of the config
      try {
        serializeConfiguration(pResConf);
      } catch (final IOException exc) {
        throw new TTIOException(exc);
      }
      // if something was not correct, delete the partly created
      // substructure
      if (!returnVal) {
        pResConf.mPath.delete();
      }
      return returnVal;
    }
  }

  @Override
  public synchronized void truncateResource(final ResourceConfiguration pResConf) {
    final File resourceFile =
      new File(new File(mDBConfig.getFile(), DatabaseConfiguration.Paths.Data.getFile().getName()),
        pResConf.mPath.getName());
    // check that database must be closed beforehand
    if (!mSessions.containsKey(resourceFile)) {
      // if file is existing and folder is a tt-dataplace, delete it
      if (resourceFile.exists() && ResourceConfiguration.Paths.compareStructure(resourceFile) == 0) {
        // instantiate the database for deletion
        EStorage.recursiveDelete(resourceFile);
      }
    }
  }

  // //////////////////////////////////////////////////////////
  // END Creation/Deletion of Resources ///////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START Opening of Databases ///////////////////////
  // //////////////////////////////////////////////////////////
  /**
   * Open database. A database can be opened only once. Afterwards the
   * singleton instance bound to the File is given back.
   * 
   * @param pFile
   *          determines where the database is located sessionConf a {@link SessionConfiguration} object to
   *          set up the session
   * @return {@link IDatabase} instance.
   * @throws AbsTTException
   *           if something odd happens
   */
  public static synchronized IDatabase openDatabase(@Nonnull final File pFile) throws AbsTTException {
    if (!pFile.exists()) {
      throw new TTUsageException("DB could not be opened (since it was not created?) at location", pFile
        .toString());
    }
    FileInputStream is = null;
    DatabaseConfiguration config = null;
    try {
      is =
        new FileInputStream(new File(pFile.getAbsoluteFile(), DatabaseConfiguration.Paths.ConfigBinary
          .getFile().getName()));
      final ObjectInputStream de = new ObjectInputStream(is);
      config = (DatabaseConfiguration)de.readObject();
      de.close();
      is.close();
    } catch (final IOException exc) {
      throw new TTIOException(exc);
    } catch (final ClassNotFoundException exc) {
      throw new TTIOException(exc.toString());
    }
    final Database database = new Database(config);
    final IDatabase returnVal = DATABASEMAP.putIfAbsent(pFile, database);
    if (returnVal == null) {
      return database;
    } else {
      return returnVal;
    }
  }

  // //////////////////////////////////////////////////////////
  // END Opening of Databases ///////////////////////
  // //////////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////////
  // START DB-Operations//////////////////////////////////
  // /////////////////////////////////////////////////////////

  @Override
  public synchronized ISession getSession(final SessionConfiguration pSessionConf) throws AbsTTException {
    final File resourceFile =
      new File(new File(mDBConfig.getFile(), DatabaseConfiguration.Paths.Data.getFile().getName()),
        pSessionConf.getResource());
    Session returnVal = mSessions.get(resourceFile);
    if (returnVal == null) {
      if (!resourceFile.exists()) {
        throw new TTUsageException("Resource could not be opened (since it was not created?) at location",
          resourceFile.toString());
      }
      FileInputStream is = null;
      ResourceConfiguration config = null;
      try {
        is =
          new FileInputStream(new File(resourceFile, ResourceConfiguration.Paths.ConfigBinary.getFile()
            .getName()));
        final ObjectInputStream de = new ObjectInputStream(is);
        config = (ResourceConfiguration)de.readObject();
        de.close();
        is.close();
      } catch (final ClassNotFoundException exc) {
        throw new TTIOException(exc.toString());
      } catch (final IOException exc) {
        throw new TTIOException(exc);
      }

      // Resource of session must be associated to this database
      assert config.mPath.getParentFile().getParentFile().equals(mDBConfig.getFile());
      returnVal = new Session(this, config, pSessionConf);
      mSessions.put(resourceFile, returnVal);
    }
    return returnVal;
  }

  @Override
  public synchronized void close() throws AbsTTException {
    for (final ISession session : mSessions.values()) {
      session.close();
    }
    DATABASEMAP.remove(mDBConfig.getFile());
  }

  // //////////////////////////////////////////////////////////
  // End DB-Operations//////////////////////////////////
  // /////////////////////////////////////////////////////////

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(mDBConfig);
    return builder.toString();
  }

  /**
   * Closing a resource. This callback is necessary due to centralized
   * handling of all sessions within a database.
   * 
   * @param pFile
   *          {@link File} to be closed
   * @return {@code true} if close successful, {@code false} otherwise
   */
  protected boolean removeSession(@Nonnull final File pFile) {
    return mSessions.remove(pFile) != null ? true : false;
  }

  /**
   * Serializing any {@link IConfigureSerializable} instance to a denoted
   * file.
   * 
   * @param pConf
   *          to be serializied, containing the file
   * @throws IOException
   *           if serialization fails
   */
  private static void serializeConfiguration(@Nonnull final IConfigureSerializable pConf) throws IOException {
    FileOutputStream os = null;
    os = new FileOutputStream(pConf.getConfigFile());
    final ObjectOutputStream en = new ObjectOutputStream(os);
    en.writeObject(pConf);
    en.close();
    os.close();
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    return mDBConfig;
  }
}
