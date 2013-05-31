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

package org.sirix.gui.view;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides methods to add and remove {@link PropertyChangeListener}s as well as firing property changes.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractObservableComponent {

  /** {@link PropertyChangeSupport} to register listeners. */
  private final PropertyChangeSupport mPropertyChangeSupport;

  /**
   * Constructor.
   */
  public AbstractObservableComponent() {
    mPropertyChangeSupport = new PropertyChangeSupport(this);
  }

  /**
   * Add a {@link PropertyChangeListener}.
   * 
   * @param pListener
   *          the listener to add
   */
  public final synchronized void addPropertyChangeListener(final PropertyChangeListener pListener) {
    mPropertyChangeSupport.addPropertyChangeListener(pListener);
  }

  /**
   * Remove a {@link PropertyChangeListener}.
   * 
   * @param pListener
   *          the listener to remove
   */
  public final synchronized void removePropertyChangeListener(final PropertyChangeListener pListener) {
    mPropertyChangeSupport.removePropertyChangeListener(pListener);
  }

  /**
   * Fire a property change.
   * 
   * @param pPropertyName
   *          name of the property
   * @param pOldValue
   *          old value
   * @param pNewValue
   *          new value
   */
  public final synchronized void firePropertyChange(final String pPropertyName,
    @Nullable final Object pOldValue, @Nonnull final Object pNewValue) {
    mPropertyChangeSupport.firePropertyChange(pPropertyName, pOldValue, pNewValue);
  }
}
