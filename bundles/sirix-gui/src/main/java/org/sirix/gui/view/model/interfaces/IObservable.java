package org.sirix.gui.view.model.interfaces;

import java.beans.PropertyChangeListener;

public interface IObservable {
  void addPropertyChangeListener(final PropertyChangeListener paramListener);

  void removePropertyChangeListener(final PropertyChangeListener paramListener);

  void firePropertyChange(final String paramPropertyName, final Object paramOldValue,
    final Object paramNewValue);
}
