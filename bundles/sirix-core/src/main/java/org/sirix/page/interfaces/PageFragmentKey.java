package org.sirix.page.interfaces;

/**
 * A page fragment key, used to reference page fragments on durable storage.
 *
 * @author Johannes Lichtenberger
 */
public interface PageFragmentKey {
  /**
   * Get the offset key into the storage file.
   * @return The offset key.
   */
  long key();

  /**
   * Get the revision number.
   * @return The revision number.
   */
  int revision();
}
