package io.sirix.page.interfaces;

/**
 * A page fragment key, used to reference page fragments on durable storage.
 *
 * @author Johannes Lichtenberger
 */
public interface PageFragmentKey {
  /**
   * Get the offset key into the storage file.
   * 
   * @return The offset key.
   */
  long key();

  /**
   * Get the revision number.
   * 
   * @return The revision number.
   */
  int revision();

  /**
   * Get the database ID.
   * 
   * @return The database ID.
   */
  long databaseId();

  /**
   * Get the resource ID.
   * 
   * @return The resource ID.
   */
  long resourceId();
}
