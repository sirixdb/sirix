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
package org.sirix.gui.view.controls;

import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.util.EventListener;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.model.interfaces.Container;
import org.sirix.gui.view.model.interfaces.Model;
import controlP5.ControlListener;

/**
 * Listener interface for processing user interface events on a Display.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface Control
    extends EventListener, MouseListener, MouseMotionListener, MouseWheelListener, KeyListener, ControlListener {
  /** Represents the use of the left mouse button */
  int LEFT_MOUSE_BUTTON = InputEvent.BUTTON1_DOWN_MASK;
  /** Represents the use of the middle mouse button */
  int MIDDLE_MOUSE_BUTTON = InputEvent.BUTTON2_DOWN_MASK;
  /** Represents the use of the right mouse button */
  int RIGHT_MOUSE_BUTTON = InputEvent.BUTTON3_DOWN_MASK;

  /**
   * Indicates if this Control is currently enabled.
   *
   * @return true if the control is enabled, false if disabled
   */
  boolean isEnabled();

  /**
   * Sets the enabled status of this control.
   *
   * @param enabled true to enable the control, false to disable it
   */
  void setEnabled(boolean enabled);

  // -- Actions performed on VisualItems ------------------------------------

  /**
   * Invoked when a mouse button is pressed on a VisualItem and then dragged.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemDragged(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when the mouse cursor has been moved onto a VisualItem but no buttons have been pushed.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemMoved(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when the mouse wheel is rotated while the mouse is over a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseWheelEvent} instance
   */
  void itemWheelMoved(VisualItem paramItem, MouseWheelEvent paramEvent);

  /**
   * Invoked when the mouse button has been clicked (pressed and released) on a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemClicked(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when a mouse button has been pressed on a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemPressed(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when a mouse button has been released on a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemReleased(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when the mouse enters a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemEntered(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when the mouse exits a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link MouseEvent} instance
   */
  void itemExited(VisualItem paramItem, MouseEvent paramEvent);

  /**
   * Invoked when a key has been pressed, while the mouse is over a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link KeyEvent} instance
   */
  void itemKeyPressed(VisualItem paramItem, KeyEvent paramEvent);

  /**
   * Invoked when a key has been released, while the mouse is over a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link KeyEvent} instance
   */
  void itemKeyReleased(VisualItem paramItem, KeyEvent paramEvent);

  /**
   * Invoked when a key has been typed, while the mouse is over a VisualItem.
   *
   * @param paramItem {@link VisualItem} instance
   * @param paramEvent {@link KeyEvent} instance
   */
  void itemKeyTyped(VisualItem paramItem, KeyEvent paramEvent);

  // -- Actions performed on the Display ------------------------------------

  /**
   * Invoked when the mouse enters the Display.
   */
  @Override
  void mouseEntered(MouseEvent paramEvent);

  /**
   * Invoked when the mouse exits the Display.
   */
  @Override
  void mouseExited(MouseEvent paramEvent);

  /**
   * Invoked when a mouse button has been pressed on the Display but NOT on a VisualItem.
   */
  @Override
  void mousePressed(MouseEvent paramEvent);

  /**
   * Invoked when a mouse button has been released on the Display but NOT on a VisualItem.
   */
  @Override
  void mouseReleased(MouseEvent paramEvent);

  /**
   * Invoked when the mouse button has been clicked (pressed and released) on the Display, but NOT on
   * a VisualItem.
   */
  @Override
  void mouseClicked(MouseEvent paramEvent);

  /**
   * Invoked when a mouse button is pressed on the Display (but NOT a VisualItem) and then dragged.
   */
  @Override
  void mouseDragged(MouseEvent paramEvent);

  /**
   * Invoked when the mouse cursor has been moved on the Display (but NOT a VisualItem) and no buttons
   * have been pushed.
   */
  @Override
  void mouseMoved(MouseEvent paramEvent);

  /**
   * Invoked when the mouse wheel is rotated while the mouse is over the Display (but NOT a
   * VisualItem).
   */
  @Override
  void mouseWheelMoved(MouseWheelEvent paramEvent);

  /**
   * Invoked when a key has been pressed, while the mouse is NOT over a VisualItem.
   */
  @Override
  void keyPressed(KeyEvent paramEvent);

  /**
   * Invoked when a key has been released, while the mouse is NOT over a VisualItem.
   */
  @Override
  void keyReleased(KeyEvent paramEvent);

  /**
   * Invoked when a key has been pressed (for processing), while the mouse is NOT over a VisualItem.
   */
  void keyPressed();

  /**
   * Invoked when a key has been released (for processing), while the mouse is NOT over a VisualItem.
   */
  void keyReleased();

  /**
   * Invoked when a key has been typed, while the mouse is NOT over a VisualItem.
   */
  @Override
  void keyTyped(KeyEvent paramEvent);

  /**
   * Get model.
   *
   * @return Model associated with the Controller
   */
  Model<? extends Container<?>, ? extends VisualItem> getModel();

  /** Reset the controller. */
  void resetControl();
}
