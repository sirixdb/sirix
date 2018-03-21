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

package org.sirix.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Factory class to build up {@link Reader}/{@link Writer} instances for Sirix.
 *
 * After all this class is implemented as a Singleton to hold one {@link BerkeleyStorage} per
 * {@link ResourceManagerConfiguration}.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class BerkeleyStorage implements Storage {

  /** Binding for {@link Long}. */
  public static final TupleBinding<Long> DATAINFO_VAL_B =
      TupleBinding.getPrimitiveBinding(Long.class);

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
   * @param env the Berkeley-DB environment
   * @param db the Berkeley-DB database
   * @param byteHandlePipe the bye handle pipeline configuration
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  public BerkeleyStorage(final Environment env, final Database db,
      final ByteHandlePipeline byteHandlePipe) {
    mEnv = checkNotNull(env);
    mDatabase = checkNotNull(db);
    mByteHandler = checkNotNull(byteHandlePipe);
  }

  @Override
  public Reader createReader() throws SirixIOException {
    try {
      return new BerkeleyReader(mEnv, mDatabase, mByteHandler);
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }
  }

  @Override
  public Writer createWriter() throws SirixIOException {
    return new BerkeleyWriter(mEnv, mDatabase, mByteHandler);
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
      final Reader reader = new BerkeleyReader(mEnv, mDatabase, mByteHandler);
      TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-1l, keyEntry);

      final OperationStatus status = mDatabase.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        returnVal = true;
      }
      reader.close();
    } catch (final DatabaseException exc) {
      throw new SirixIOException(exc);
    }
    return returnVal;

  }

  @Override
  public ByteHandler getByteHandler() {
    return mByteHandler;
  }
}
