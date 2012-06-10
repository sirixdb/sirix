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

package org.sirix.settings;

import java.io.File;

/**
 * 
 * Enumeration for determining all storage relevant stuff like paths to
 * different resources, etc.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 * 
 */
public enum EStoragePaths {

  /** Folder for storage of data. */
  TT(new File("tt"), true),
  /** Folder for transaction log. */
  TRANSACTIONLOG(new File("transactionLog"), true),
  /** File to store the db settings. */
  DBSETTINGS(new File("dbsettings.xml"), false),
  /** File to store encryption db settings. */
  KEYSELECTOR(new File("keyselector"), true);

  private final File mFile;

  private final boolean mIsFolder;

  private EStoragePaths(final File mFile, final boolean mIsFolder) {
    this.mFile = mFile;
    this.mIsFolder = mIsFolder;
  }

  /**
   * Getting the file for the kind.
   * 
   * @return the File to the kind
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
   * Checking a structure in a folder to be equal with the data in this enum.
   * 
   * @param mFile
   *          to be checked
   * @return -1 if less folders are there, 0 if the structure is equal to the
   *         one expected, 1 if the structure has more folders
   */
  public static int compareStructure(final File mFile) {
    int existing = 0;
    for (final EStoragePaths paths : values()) {
      final File currentFile = new File(mFile, paths.getFile().getName());
      if (currentFile.exists()) {
        existing++;
      }
    }
    return existing - values().length;
  }

}
