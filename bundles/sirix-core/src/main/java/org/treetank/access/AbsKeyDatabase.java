package org.treetank.access;

import java.io.File;

import org.treetank.settings.EStoragePaths;

/**
 * Abstract class for holding common data for all key databases involved
 * in encryption process. Each instance of this class stores the data in a
 * place related to the {@link DatabaseConfiguration} at a different subfolder.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public abstract class AbsKeyDatabase {

  /**
   * Place to store the data.
   */
  protected final File place;

  /**
   * Counter to give every instance a different place.
   */
  private static int counter;

  /**
   * Constructor with the place to store the data.
   * 
   * @param paramFile
   *          {@link File} which holds the place to store
   *          the data.
   */
  protected AbsKeyDatabase(final File paramFile) {
    place =
      new File(paramFile, new StringBuilder(EStoragePaths.KEYSELECTOR.getFile().getName()).append(
        File.separator).append(counter).toString());
    place.mkdirs();
    counter++;
  }

}
