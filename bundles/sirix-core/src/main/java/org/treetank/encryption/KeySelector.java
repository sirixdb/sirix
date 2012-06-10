package org.treetank.encryption;

import java.util.LinkedList;
import java.util.List;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * This class represents the key selector model holding all data
 * for a node of the right tree consisting of group and user nodes.
 * 
 * @author Patrick Lang, University of Konstanz
 */
@Entity
public class KeySelector {

  /**
   * Selector key and primary key of database.
   */
  @PrimaryKey
  private long mKeyId;

  /**
   * Name of the node (group or user name).
   */
  private String mName;

  /**
   * List of parent nodes.
   */
  private List<Long> mParents;

  /**
   * Current revision of node.
   */
  private int mRevision;

  /**
   * Current version of node.
   */
  private int mVersion;

  /**
   * Type of node (group or user).
   */
  private EntityType mType;

  /**
   * Standard constructor.
   */
  public KeySelector() {
    super();
  }

  /**
   * Constructor for building an new key selector instance.
   * 
   * @param paramName
   *          node name.
   */
  public KeySelector(final String paramName, final EntityType paramType) {
    this.mKeyId = RightKey.getInstance().newSelectorKey();
    this.mName = paramName;
    this.mParents = new LinkedList<Long>();
    this.mRevision = 0;
    this.mVersion = 0;
    this.mType = paramType;
  }

  /**
   * Returns selector id.
   * 
   * @return
   *         selector id.
   */
  public final long getKeyId() {
    return mKeyId;
  }

  /**
   * Returns node name.
   * 
   * @return
   *         node name.
   */
  public final String getName() {
    return mName;
  }

  /**
   * Returns a set of parent nodes for node.
   * 
   * @return
   *         set of parent nodes.
   */
  public final List<Long> getParents() {
    return mParents;
  }

  /**
   * Add a new parent node to the set.
   * 
   * @param paramParent
   *          parent to add to set.
   */
  public final void addParent(final long paramParent) {
    mParents.add(paramParent);
  }

  /**
   * Returns current revision of node.
   * 
   * @return
   *         node's revision.
   */
  public final int getRevision() {
    return mRevision;
  }

  /**
   * Increases node revision by 1.
   */
  public final void increaseRevision() {
    this.mRevision += 1;
  }

  /**
   * Returns current version of node.
   * 
   * @return
   *         node's version.
   */
  public final int getVersion() {
    return mVersion;
  }

  /**
   * Increases node version by 1.
   */
  public final void increaseVersion() {
    this.mVersion += 1;
  }

  /**
   * Returns type of entity.
   */
  public EntityType getType() {
    return mType;
  }
}
