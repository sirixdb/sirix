package org.sirix.encryption;

/**
 * A singleton class to create several unique keys for encryption processes.
 */
public class RightKey {

  /**
   * Singleton instance.
   */
  private static RightKey mINSTANCE = new RightKey();

  /**
   * Right key counter.
   */
  private int mRightKey = -1;

  /**
   * Selector key counter.
   */
  private int mSelectorKey = -1;

  /**
   * Material key counter.
   */
  private int mMaterialKey = -1;

  /**
   * Returns singleton instance.
   * 
   * @return
   *         singleton instance.
   */
  public static RightKey getInstance() {
    return mINSTANCE;
  }

  /**
   * Create new right key by increasing current state by 1.
   * 
   * @return
   *         new unique right key.
   */
  public final int newRightKey() {
    return ++mRightKey;
  }

  /**
   * Create new selector key by increasing current state by 1.
   * 
   * @return
   *         new unique selector key.
   */
  public final int newSelectorKey() {
    return ++mSelectorKey;
  }

  /**
   * Create new material key by increasing current state by 1.
   * 
   * @return
   *         new material right key.
   */
  public final int newMaterialKey() {
    return ++mMaterialKey;
  }

}
