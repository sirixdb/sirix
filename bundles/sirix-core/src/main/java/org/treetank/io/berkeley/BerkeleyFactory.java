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

package org.treetank.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;

import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.exception.TTIOException;
import org.treetank.io.IKey;
import org.treetank.io.IReader;
import org.treetank.io.IStorage;
import org.treetank.io.IWriter;
import org.treetank.io.KeyDelegate;
import org.treetank.io.berkeley.binding.AbstractPageBinding;
import org.treetank.io.berkeley.binding.KeyBinding;
import org.treetank.io.berkeley.binding.PageBinding;
import org.treetank.io.berkeley.binding.PageReferenceUberPageBinding;
import org.treetank.page.PageReference;
import org.treetank.page.interfaces.IPage;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Factory class to build up {@link IReader}/{@link IWriter} instances for the
 * Treetank Framework.
 * 
 * After all this class is implemented as a Singleton to hold one {@link BerkeleyFactory} per
 * {@link SessionConfiguration}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class BerkeleyFactory implements IStorage {

  /** Binding for {@link KeyDelegate}. */
  public static final TupleBinding<IKey> KEY = new KeyBinding();

  /** Binding for {@link PageDelegate}. */
  public static final PageBinding PAGE_VAL_B = new PageBinding();

  /** Binding for {@link PageReference}. */
  public static final TupleBinding<PageReference> FIRST_REV_VAL_B = new PageReferenceUberPageBinding();

  /** Binding for {@link Long}. */
  public static final TupleBinding<Long> DATAINFO_VAL_B = TupleBinding.getPrimitiveBinding(Long.class);

  /**
   * Name for the database.
   */
  private static final String NAME = "berkeleyDatabase";

  /**
   * Berkeley Environment for the database.
   */
  private final Environment mEnv;

  /**
   * Database instance per session.
   */
  private final Database mDatabase;

  /** Storage of DB. */
  private final File mFile;

  /**
   * Constructor.
   * 
   * @param pFile
   *          the file associated with the database
   * @param pDatabase
   *          for the Database settings
   * @param pSession
   *          for the settings
   * @throws TTIOException
   *           if something odd happens while database-connection
   * @throws NullPointerException
   *           if {@code pFile} is {@code null}
   */
  public BerkeleyFactory(final File pFile) throws TTIOException {
    mFile = checkNotNull(pFile);

    final File repoFile = new File(pFile, ResourceConfiguration.Paths.Data.getFile().getName());
    if (!repoFile.exists()) {
      repoFile.mkdirs();
    }

    final DatabaseConfig conf = generateDBConf();
    final EnvironmentConfig config = generateEnvConf();

    if (repoFile.listFiles().length == 0
      || (repoFile.listFiles().length == 1 && "tt.tnk".equals(repoFile.listFiles()[0].getName()))) {
      conf.setAllowCreate(true);
      config.setAllowCreate(true);
    }

    try {
      mEnv = new Environment(repoFile, config);

      mDatabase = mEnv.openDatabase(null, NAME, conf);
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }

  }

  @Override
  public IReader getReader() throws TTIOException {
    try {
      return new BerkeleyReader(mEnv, mDatabase);
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public IWriter getWriter() throws TTIOException {
    return new BerkeleyWriter(mEnv, mDatabase);
  }

  @Override
  public void close() throws TTIOException {
    try {
      mDatabase.close();
      mEnv.close();
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public boolean exists() throws TTIOException {
    final DatabaseEntry valueEntry = new DatabaseEntry();
    final DatabaseEntry keyEntry = new DatabaseEntry();
    boolean returnVal = false;
    try {
      final IReader reader = new BerkeleyReader(mEnv, mDatabase);
      BerkeleyFactory.KEY.objectToEntry(BerkeleyKey.getFirstRevKey(), keyEntry);

      final OperationStatus status = mDatabase.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        returnVal = true;
      }
      reader.close();
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
    return returnVal;

  }

  @Override
  public void truncate() throws TTIOException {
    try {
      final Environment env =
        new Environment(new File(mFile, ResourceConfiguration.Paths.Data.getFile().getName()),
          generateEnvConf());
      if (env.getDatabaseNames().contains(NAME)) {
        env.removeDatabase(null, NAME);
      }
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }

  }

  /**
   * Generate {@link EnvironmentConfig} reference.
   * 
   * @return transactional environment configuration
   */
  private static EnvironmentConfig generateEnvConf() {
    final EnvironmentConfig config = new EnvironmentConfig();
    config.setTransactional(true);
    config.setCacheSize(1024 * 1024);
    return config;
  }

  /**
   * Generate {@link DatabaseConfig} reference.
   * 
   * @return transactional database configuration
   */
  private static DatabaseConfig generateDBConf() {
    final DatabaseConfig conf = new DatabaseConfig();
    conf.setTransactional(true);
    conf.setKeyPrefixing(true);
    return conf;
  }

}
