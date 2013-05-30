package org.sirix.index.bplustree;

import org.sirix.io.Storage;

/**
 * Constants for the B+ Tree disk structure. They can be modified which
 * might be helpful for testing purposes.
 * 
 * @author Christian Gruen, DBIS, University of Konstanz
 */
public final class Constants {
	
	/**
	 * Private constructor.
	 */
	private Constants() {
		throw new AssertionError();
	}
	
  /**
   * Fixed disk block size, represented in bytes.
   */
  int BUFFERSIZE = 4096;

  /**
   * Available buffers used in the {@link Storage} class.
   */
  int NRBUFFERS = 32;

  /**
   * Number of available values in a disk block which is usually
   * a quarter of the {@link #BUFFERSIZE}. 
   */
  int NRENTRIES = BUFFERSIZE >> 2;

  /**
   * Number of available leaf values. The chosen number must be an even
   * value; it is calculated by bisecting and doubling the {@link #NRENTRIES}
   * integer.
   */
  int NRLEAVES = NRENTRIES - 1 >> 1 << 1;

  /**
   * Number of available values. The chosen number must be an even
   * value; it is calculated by dividing {@link #NRLEAVES} by four and
   * doubling the result.
   */
  int NRVALUES = NRLEAVES >> 2 << 1;

  /**
   * Number of available children - which is {@link #NRVALUES} plus 1.
   */
  int NRCHILDREN = NRVALUES + 1;
}
