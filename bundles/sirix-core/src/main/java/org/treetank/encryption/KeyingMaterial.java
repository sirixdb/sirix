package org.treetank.encryption;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * This class represents the secret material model holding
 * all data for KEKs(key encryption keys) and TEKs
 * (traffic encryption keys) with its selector key and its
 * revision and version respectively.
 * 
 * @author Patrick Lang, University of Konstanz
 */
@Entity
public class KeyingMaterial {

  /**
   * Unique material key and primary key for database.
   */
  @PrimaryKey
  private long mMaterialKey;

  /**
   * Selector key for having a relation to key selector database.
   */
  private long mSelectorKey;

  /**
   * Revision of keying material.
   */
  private int mRevsion;

  /**
   * Version of keying material.
   */
  private int mVersion;

  /**
   * Secret key using for data en-/decryption.
   */
  private byte[] mSecretKey;

  /**
   * Parent node id.
   */
  private long mParent;

  /**
   * Standard constructor.
   */
  public KeyingMaterial() {
    super();
  }

  /**
   * Constructor for building a new keying material instance.
   * 
   * @param paramKey
   *          selector key of keying material.
   * @param paramRev
   *          revision of keying material.
   * @param paramVer
   *          version of keying material.
   * @param paramSKey
   *          secret key of keying material.
   */
  public KeyingMaterial(final long paramKey, final int paramRev, final int paramVer, final byte[] paramSKey) {
    this.mMaterialKey = RightKey.getInstance().newMaterialKey();
    this.mSelectorKey = paramKey;
    this.mRevsion = paramRev;
    this.mVersion = paramVer;
    this.mSecretKey = paramSKey;
  }

  /**
   * Returns selector key.
   * 
   * @return
   *         selector key.
   */
  public final long getSelectorKey() {
    return mSelectorKey;
  }

  /**
   * Returns revision.
   * 
   * @return
   *         revsion.
   */
  public final int getRevsion() {
    return mRevsion;
  }

  /**
   * Returns version.
   * 
   * @return
   *         version.
   */
  public final int getVersion() {
    return mVersion;
  }

  /**
   * Returns unqiue material key.
   * 
   * @return
   *         material key.
   */
  public final long getMaterialKey() {
    return mMaterialKey;
  }

  /**
   * Returns secret key.
   * 
   * @return
   *         secret key.
   */
  public final byte[] getSecretKey() {
    return mSecretKey;
  }

  /**
   * Sets a new secret key.
   * 
   * @param paramSKey
   *          new secret key.
   */
  public final void setSecretKey(final byte[] paramSKey) {
    this.mSecretKey = paramSKey;
  }

  /**
   * Returns parent node id.
   * 
   * @return
   *         node id of parent.
   */
  public final long getParent() {
    return mParent;
  }

}
