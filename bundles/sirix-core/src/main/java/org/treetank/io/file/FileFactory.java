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

package org.treetank.io.file;

import java.io.File;

import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.exception.TTIOException;
import org.treetank.io.EStorage;
import org.treetank.io.IReader;
import org.treetank.io.IStorage;
import org.treetank.io.IWriter;

/**
 * Factory to provide File access as a backend.
 * 
 * @author Sebastian Graf, University of Konstanz.
 * 
 */
public final class FileFactory implements IStorage {

  /** private constant for fileName. */
  private static final String FILENAME = "tt.tnk";

  /** Instance to storage. */
  private final File mFile;

  /**
   * Constructor.
   * 
   * @param paramFile
   *          the location of the database
   * 
   */
  public FileFactory(final File paramFile) {
    mFile = paramFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IReader getReader() throws TTIOException {
    return new FileReader(getConcreteStorage());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IWriter getWriter() throws TTIOException {
    return new FileWriter(getConcreteStorage());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    // not used over here
  }

  /**
   * Getting concrete storage for this file.
   * 
   * @return the concrete storage for this database
   */
  private File getConcreteStorage() {
    return new File(mFile, new StringBuilder(ResourceConfiguration.Paths.Data.getFile().getName()).append(
      File.separator).append(FILENAME).toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists() throws TTIOException {
    final File file = getConcreteStorage();
    final boolean returnVal = file.length() > 0;
    return returnVal;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void truncate() throws TTIOException {
    EStorage.recursiveDelete(mFile);
  }

}
