package org.sirix.gui.view.model.interfaces;

import java.beans.PropertyChangeListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Observable {
	void addPropertyChangeListener(final PropertyChangeListener listener);

	void removePropertyChangeListener(final PropertyChangeListener listener);

	void firePropertyChange(final String propertyName,
			@Nullable final Object oldValue, @Nonnull final Object newValue);
}
