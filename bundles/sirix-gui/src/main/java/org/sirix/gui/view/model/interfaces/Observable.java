package org.sirix.gui.view.model.interfaces;

import java.beans.PropertyChangeListener;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Observable {
	void addPropertyChangeListener(final PropertyChangeListener listener);

	void removePropertyChangeListener(final PropertyChangeListener listener);

	void firePropertyChange(final String propertyName,
			@Nullable final Object oldValue, @NonNull final Object newValue);
}
