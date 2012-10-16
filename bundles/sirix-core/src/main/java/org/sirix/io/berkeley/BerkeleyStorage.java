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

package org.sirix.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import java.io.File;

import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.berkeley.binding.PageBinding;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.delegates.PageDelegate;

/**
 * Factory class to build up {@link Reader}/{@link Writer} instances for Sirix.
 * 
 * After all this class is implemented as a Singleton to hold one {@link BerkeleyStorage} per
 * {@link SessionConfiguration}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class BerkeleyStorage implements Storage {

  /** Binding for {@link PageDelegate}. */
  public final PageBinding mPageBinding;

  /** Binding for {@link Long}. */
  public static final TupleBinding<Long> DATAINFO_VAL_B = TupleBinding
    .getPrimitiveBinding(Long.class);

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

  /** Byte handler pipeline. */
  private final ByteHandlePipeline mByteHandler;

  /**
   * Constructor.
   * 
   * @param file
   *          the file associated with the database
   * @param pDatabase
   *          for the Database settings
   * @param pSession
   *          for the settings
   * @throws SirixIOException
   *           if something odd happens while database-connection
   * @throws NullPointerException
   *           if {@code pFile} is {@code null}
   */
  public BerkeleyStorage(final @Nonnull File file,
    final @Nonnull ByteHandlePipeline handler) throws SirixIOException {
    final File repoFile =
      new File(checkNotNull(file), ResourceConfiguration.Paths.Data.getFile()
        .getName());
    if (!repoFile.exists()) {
      repoFile.mkdirs();
    }

    mByteHandler = checkNotNull(handler);
    mPageBinding = new PageBinding(mByteHandler);

    final DatabaseConfig conf = generateDBConf();
    final EnvironmentConfig config = generateEnvConf();

    if (repoFile.listFiles().length == 0
      || (repoFile.listFiles().length == 1 && "tt.tnk".equals(repoFile
        .listFiles()[0].getName()))) {
      conf.setAllowCreate(true);
      config.setAllowCreate(true);
    }

    try {
      mEnv = new Environment(repoFile, config);
      mDatabase = mEnv.openDatabase(null, NAME, conf);
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }

  }

  @Override
  public Reader getReader() throws SirixIOException {
    try {
      return new BerkeleyReader(mEnv, mDatabase, new PageBinding(mPageBinding));
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }
  }

  @Override
  public Writer getWriter() throws SirixIOException {
    return new BerkeleyWriter(mEnv, mDatabase, new PageBinding(mPageBinding));
  }

  @Override
  public void close() throws SirixIOException {
    try {
      mDatabase.close();
      mEnv.close();
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }
  }

  @Override
  public boolean exists() throws SirixIOException {
    final DatabaseEntry valueEntry = new DatabaseEntry();
    final DatabaseEntry keyEntry = new DatabaseEntry();
    boolean returnVal = false;
    try {
      final Reader reader = new BerkeleyReader(mEnv, mDatabase, mPageBinding);
      TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-1l, keyEntry);

      final OperationStatus status =
        mDatabase.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        returnVal = true;
      }
      reader.close();
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }
    return returnVal;

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

  @Override
  public ByteHandler getByteHandler() {
    return mByteHandler;
  }

  /**
   * Get the page binding.
   * 
   * @return page binding
   */
  public PageBinding getPageBinding() {
    return mPageBinding;
  }

}
