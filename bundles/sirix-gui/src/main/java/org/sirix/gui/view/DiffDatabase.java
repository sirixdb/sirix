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
package org.sirix.gui.view;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;
import org.sirix.diff.DiffTuple;
import org.sirix.utils.Files;
import org.sirix.utils.LogWrapper;

/** Database to store generated diffs. */
public class DiffDatabase implements AutoCloseable {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(DiffDatabase.class));

  private static final String CLASS_CATALOG = "java_class_catalog";

  /**
   * Berkeley {@link Environment} for the database.
   */
  private transient Environment mEnv;

  /**
   * {@link File} where to store the database.
   */
  private final File mStorageFile;

  /**
   * Counter to give every instance a different place.
   */
  private AtomicInteger mCounter;

  /**
   * Name for the database.
   */
  private static final String NAME = "DiffDatabase";

  private ClassCatalog mCatalog;

  private Database mDatabase;

  private StoredMap<Integer, DiffTuple> mMap;

  /**
   * Constructor.
   * 
   * @param paramFile
   *          {@link File} where the database should be stored
   * @throws NullPointerException
   *           if {@code paramFile} is {@code null}
   */
  public DiffDatabase(final File paramFile) {
    Objects.requireNonNull(paramFile);
    mCounter = new AtomicInteger();
    mStorageFile =
      new File(paramFile, new StringBuilder(new File("diff").getName()).append(File.separator).append(
        mCounter.incrementAndGet()).toString());
    try {
      if (mStorageFile.exists()) {
        Files.recursiveRemove(mStorageFile.toPath());
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    if (!mStorageFile.mkdirs()) {
      throw new IllegalStateException("Couldn't create directory for storage of diffs!");
    }

    // Create environment configuration and environment.
    final EnvironmentConfig environmentConfig = new EnvironmentConfig();
    environmentConfig.setAllowCreate(true);
    environmentConfig.setTransactional(false);
    mEnv = new Environment(mStorageFile, environmentConfig);

    // Create database configuration.
    final DatabaseConfig conf = new DatabaseConfig();
    conf.setAllowCreate(true);
    conf.setTemporary(true);

    // Catalog is needed for serial bindings (java serialization).
    final Database catalogDb = mEnv.openDatabase(null, CLASS_CATALOG, conf);
    mCatalog = new StoredClassCatalog(catalogDb);

    // Use Integer tuple binding for key entries.
    final EntryBinding<Integer> keyBinding = TupleBinding.getPrimitiveBinding(Integer.class);

    // Use Diff serial binding for data entries.
    final EntryBinding<DiffTuple> dataBinding = new SerialBinding<>(mCatalog, DiffTuple.class);

    // Create a database.
    mDatabase = mEnv.openDatabase(null, NAME, conf);

    // Create a map view of the database.
    mMap = new StoredMap<>(mDatabase, keyBinding, dataBinding, true);
  }

  /**
   * Get a map view.
   * 
   * @return the {@link StoredMap} instance
   */
  public StoredMap<Integer, DiffTuple> getMap() {
    return mMap;
  }

  /**
   * Get {@link Environment}.
   * 
   * @return the {@link Environment} instance
   */
  public Environment getEnvironment() {
    return mEnv;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    mCatalog.close();
    mDatabase.close();
    mEnv.close();
  }
}
