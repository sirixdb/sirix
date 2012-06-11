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

import java.awt.*;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

/**
 * FlowLayout subclass that fully supports wrapping of components.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public class WrapLayout extends FlowLayout {
  /**
   * Default Serial UID.
   */
  private static final long serialVersionUID = 1L;

  /** Preferred layout size. */
  private transient Dimension mPreferredLayoutSize;

  /**
   * Constructs a new <code>WrapLayout</code> with a left
   * alignment and a default 5-unit horizontal and vertical gap.
   */
  public WrapLayout() {
    super();
  }

  /**
   * Constructs a new <code>FlowLayout</code> with the specified
   * alignment and a default 5-unit horizontal and vertical gap.
   * The value of the alignment argument must be one of <code>WrapLayout</code>, <code>WrapLayout</code>,
   * or <code>WrapLayout</code>.
   * 
   * @param paramAlign
   *          the alignment value
   */
  public WrapLayout(final int paramAlign) {
    super(paramAlign);
  }

  /**
   * Creates a new flow layout manager with the indicated alignment
   * and the indicated horizontal and vertical gaps.
   * <p>
   * The value of the alignment argument must be one of <code>WrapLayout</code>, <code>WrapLayout</code>, or
   * <code>WrapLayout</code>.
   * 
   * @param paramAlign
   *          the alignment value
   * @param paramHgap
   *          the horizontal gap between components
   * @param paramVgap
   *          the vertical gap between components
   */
  public WrapLayout(final int paramAlign, final int paramHgap, final int paramVgap) {
    super(paramAlign, paramHgap, paramVgap);
  }

  /**
   * Returns the preferred dimensions for this layout given the
   * <i>visible</i> components in the specified target container.
   * 
   * @param paramTarget
   *          the component which needs to be laid out
   * @return the preferred dimensions to lay out the
   *         subcomponents of the specified container
   */
  @Override
  public final Dimension preferredLayoutSize(final Container paramTarget) {
    return layoutSize(paramTarget, true);
  }

  /**
   * Returns the minimum dimensions needed to layout the <i>visible</i>
   * components contained in the specified target container.
   * 
   * @param paramTarget
   *          the component which needs to be laid out
   * @return the minimum dimensions to lay out the
   *         subcomponents of the specified container
   */
  @Override
  public final Dimension minimumLayoutSize(final Container paramTarget) {
    final Dimension minimum = layoutSize(paramTarget, false);
    minimum.width -= getHgap() + 1;
    return minimum;
  }

  /**
   * Returns the minimum or preferred dimension needed to layout the target
   * container.
   * 
   * @param paramTarget
   *          target to get layout size for
   * @param paramPreferred
   *          should preferred size be calculated
   * @return the dimension to layout the target container
   */
  private Dimension layoutSize(final Container paramTarget, final boolean paramPreferred) {
    synchronized (paramTarget.getTreeLock()) {
      // Each row must fit with the width allocated to the containter.
      // When the container width = 0, the preferred width of the container
      // has not yet been calculated so lets ask for the maximum.

      int targetWidth = paramTarget.getSize().width;

      if (targetWidth == 0) {
        targetWidth = Integer.MAX_VALUE;
      }

      final int hgap = getHgap();
      final int vgap = getVgap();
      final Insets insets = paramTarget.getInsets();
      final int horizontalInsetsAndGap = insets.left + insets.right + (hgap * 2);
      final int maxWidth = targetWidth - horizontalInsetsAndGap;

      // Fit components into the allowed width

      final Dimension dim = new Dimension(0, 0);
      int rowWidth = 0;
      int rowHeight = 0;

      final int nmembers = paramTarget.getComponentCount();

      for (int i = 0; i < nmembers; i++) {
        final Component m = paramTarget.getComponent(i);

        if (m.isVisible()) {
          final Dimension d = paramPreferred ? m.getPreferredSize() : m.getMinimumSize();

          // Can't add the component to current row. Start a new row.

          if (rowWidth + d.width > maxWidth) {
            addRow(dim, rowWidth, rowHeight);
            rowWidth = 0;
            rowHeight = 0;
          }

          // Add a horizontal gap for all components after the first

          if (rowWidth != 0) {
            rowWidth += hgap;
          }

          rowWidth += d.width;
          rowHeight = Math.max(rowHeight, d.height);
        }
      }

      addRow(dim, rowWidth, rowHeight);

      dim.width += horizontalInsetsAndGap;
      dim.height += insets.top + insets.bottom + vgap * 2;

      // When using a scroll pane or the DecoratedLookAndFeel we need to
      // make sure the preferred size is less than the size of the
      // target containter so shrinking the container size works
      // correctly. Removing the horizontal gap is an easy way to do this.

      final Container scrollPane = SwingUtilities.getAncestorOfClass(JScrollPane.class, paramTarget);

      if (scrollPane != null) {
        dim.width -= hgap + 1;
      }

      return dim;
    }
  }

  /**
   * Layout the components in the Container using the layout logic of the
   * parent FlowLayout class.
   * 
   * @param paramTarget
   *          the Container using this WrapLayout
   */
  @Override
  public final void layoutContainer(final Container paramTarget) {
    final Dimension size = preferredLayoutSize(paramTarget);

    // When a frame is minimized or maximized the preferred size of the
    // Container is assumed not to change. Therefore we need to force a
    // validate() to make sure that space, if available, is allocated to
    // the panel using a WrapLayout.

    if (size.equals(mPreferredLayoutSize)) {
      super.layoutContainer(paramTarget);
    } else {
      mPreferredLayoutSize = size;
      Container top = paramTarget;

      while (!(top instanceof Window) && top.getParent() != null) {
        top = top.getParent();
      }

      top.validate();
    }
  }

  /**
   * A new row has been completed. Use the dimensions of this row
   * to update the preferred size for the container.
   * 
   * @param paramDim
   *          update the width and height when appropriate
   * 
   * @param paramRowWidth
   *          the width of the row to add
   * 
   * @param paramRowHeight
   *          the height of the row to add
   */
  private void addRow(final Dimension paramDim, final int paramRowWidth, final int paramRowHeight) {
    paramDim.width = Math.max(paramDim.width, paramRowWidth);

    if (paramDim.height > 0) {
      paramDim.height += getVgap();
    }

    paramDim.height += paramRowHeight;
  }
}
