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

package org.treetank.access.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import com.google.common.base.Objects;

/**
 * <h1>Database Configuration</h1>
 * 
 * <p>
 * Represents a configuration of a database. Includes all settings which have to be made when it comes to the
 * creation of the database. Since the settings are persisted after creation, it must contain a link to the
 * file defined by the interface {@link IConfigureSerializable}.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 */
public final class DatabaseConfiguration implements IConfigureSerializable {

  /** For serialization. */
  private static final long serialVersionUID = -5005030622296323912L;

  /**
   * Paths for a {@link org.access.Database}. Each {@link org.access.Database} has the same folder.layout.
   */
  public enum Paths {

    /** File to store db settings. */
    ConfigBinary(new File("dbsetting.obj"), false),
    /** File to store encryption db settings. */
    KEYSELECTOR(new File("keyselector"), true),
    /** File to store the data. */
    Data(new File("resources"), true);

    /** Location of the file. */
    private final File mFile;

    /** Is the location a folder or no? */
    private final boolean mIsFolder;

    /**
     * Constructor.
     * 
     * @param pFile
     *          to be set
     * @param pIsFolder
     *          to be set.
     */
    private Paths(final File pFile, final boolean pIsFolder) {
      mFile = checkNotNull(pFile);
      mIsFolder = checkNotNull(pIsFolder);
    }

    /**
     * Getting the file for the kind.
     * 
     * @return the file to the kind
     */
    public File getFile() {
      return mFile;
    }

    /**
     * Check if file is denoted as folder or not.
     * 
     * @return boolean if file is folder
     */
    public boolean isFolder() {
      return mIsFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this
     * enum.
     * 
     * @param pFile
     *          to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to
     *         the one expected, 1 if the structure has more folders
     */
    public static int compareStructure(final File pFile) {
      checkNotNull(pFile);
      int existing = 0;
      for (final Paths paths : values()) {
        final File currentFile = new File(pFile, paths.getFile().getName());
        if (currentFile.exists()) {
          existing++;
        }
      }
      return existing - values().length;
    }

  }

  // STATIC STANDARD FIELDS
  /** Identification for string. */
  public static final String BINARY = "5.4.0";
  // END STATIC STANDARD FIELDS

  /** Binary version of storage. */
  private final String mBinaryVersion;

  /** Path to file. */
  private final File mFile;

  /**
   * Constructor with the path to be set.
   * 
   * @param paramFile
   *          file to be set
   */
  public DatabaseConfiguration(final File paramFile) {
    mBinaryVersion = BINARY;
    mFile = paramFile.getAbsoluteFile();
  }

  /**
   * Getting the database file.
   * 
   * @return the database file
   */
  public File getFile() {
    return mFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("File", mFile).add("Binary Version", mBinaryVersion).toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(final Object pObj) {
    if (this == pObj) {
      return true;
    }
    if (pObj instanceof DatabaseConfiguration) {
      final DatabaseConfiguration other = (DatabaseConfiguration)pObj;
      return Objects.equal(mFile, other.mFile) && Objects.equal(mBinaryVersion, other.mBinaryVersion);
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(mFile, mBinaryVersion);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public File getConfigFile() {
    return new File(mFile, Paths.ConfigBinary.getFile().getName());
  }
}
