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

package org.treetank.gui;

/**
 * <h1>GUIProps</h1>
 * 
 * <p>
 * GUI properties.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class GUIProp {
  /** Show views enum. */
  public enum EShowViews {
    /** Show tree view. */
    SHOWTREE(false),

    /** Show text view. */
    SHOWTEXT(false),

    /** Show small multiples view. */
    SHOWSMALLMULTIPLES(false),

    /** Show sunburst view. */
    SHOWSUNBURST(true);

    /** Determines if view should be shown. */
    private boolean mShow;

    /**
     * Constructor.
     * 
     * @param paramShow
     *          determines if view should be shown
     */
    EShowViews(final boolean paramShow) {
      mShow = paramShow;
    }

    /**
     * Invert show value.
     */
    public void invert() {
      mShow = !mShow;
    }

    /**
     * Get show value.
     * 
     * @return the show value.
     */
    public boolean getValue() {
      return mShow;
    }
  }

  /** Indent spaces. */
  private transient int mIndentSpaces = 2;

  /**
   * Default constructor.
   */
  public GUIProp() {
  }

  // ACCESSORS ==============================================

  /**
   * Set how many spaces should be used per level to indent.
   * 
   * @param paramIndentSpaces
   *          spaces to indent
   */
  public void setIndentSpaces(final int paramIndentSpaces) {
    mIndentSpaces = paramIndentSpaces;
  }

  /**
   * Get spaces to indent.
   * 
   * @return spaces to indent
   */
  public int getIndentSpaces() {
    return mIndentSpaces;
  }
}
