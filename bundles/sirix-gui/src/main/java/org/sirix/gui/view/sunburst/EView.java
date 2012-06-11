/**
 * 
 */
package org.sirix.gui.view.sunburst;

/**
 * Determines the kind of view.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EView {
  /** Normal diff view. */
  DIFF(false),

  /** View without showing differences. */
  NODIFF(false);

  /** Determines if for the diff view the */
  private transient boolean mValue;

  /**
   * Initialization.
   * 
   * @param pValue
   *          value
   */
  EView(final boolean pValue) {
    mValue = pValue;
  }

  /**
   * Get value.
   * 
   * @return value
   */
  public boolean getValue() {
    return mValue;
  }

  /**
   * Set value.
   * 
   * @param paramValue
   *          value to set
   */
  public void setValue(final boolean paramValue) {
    mValue = paramValue;
  }
}
