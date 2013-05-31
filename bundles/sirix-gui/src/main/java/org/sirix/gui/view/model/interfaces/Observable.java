package org.sirix.gui.view.model.interfaces;

import java.beans.PropertyChangeListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Observable {
  void addPropertyChangeListener(final PropertyChangeListener pListener);

  void removePropertyChangeListener(final PropertyChangeListener pListener);

  void firePropertyChange(final String pPropertyName, @Nullable final Object pOldValue,
    @Nonnull final Object pNewValue);
}
