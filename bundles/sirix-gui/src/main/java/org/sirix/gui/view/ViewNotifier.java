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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.sirix.gui.GUI;

import com.google.common.base.Optional;

/**
 * <h1>ViewNotifier</h1>
 * 
 * <p>
 * Notifies views of changes (observer pattern).
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class ViewNotifier implements Serializable {
	/**
	 * SerialUID.
	 */
	private static final long serialVersionUID = 1L;

	/** Reference to main window. */
	private final GUI mGUI;

	/** Attached views. */
	private final Set<View> mViews;

	/**
	 * Constructor.
	 * 
	 * @param paramGUI
	 *          Reference to {@link GUI}.
	 */
	public ViewNotifier(final GUI paramGUI) {
		mGUI = paramGUI;
		mViews = new HashSet<View>();
	}

	/**
	 * Adds a new view.
	 * 
	 * @param paramView
	 *          view to be added
	 */
	public void add(final View paramView) {
		mViews.add(paramView);
	}

	/**
	 * Notifies all views of a data reference change.
	 * 
	 * @param paramView
	 *          the calling {@link View}
	 */
	public void init(final View pView) {
		for (final View view : mViews) {
			if (view.isVisible()) {
				if (pView != null) {
					if (view != pView) {
						view.refreshInit();
					}
				} else {
					view.refreshInit();
				}
			}
		}
	}

	/**
	 * Notifies all views of updates in the data structure.
	 * 
	 * @param paramView
	 *          the calling {@link View}
	 */
	public void update(final View pView, Optional<VisualItemAxis> pItems) {
		checkNotNull(pView);
		checkNotNull(pItems);
		for (final View view : mViews) {
			if (view.isVisible() && view != pView) {
				view.refreshUpdate(pItems);
			}
		}
	}

	/**
	 * Dispose all views.
	 */
	public void dispose() {
		for (final View view : mViews) {
			if (view.isVisible()) {
				view.dispose();
			}
		}
	}

	/**
	 * Get the main {@link GUI} frame.
	 * 
	 * @return the gui
	 */
	public GUI getGUI() {
		return mGUI;
	}

	/**
	 * Hover current {@link VisualItem} implementation.
	 * 
	 * @param paramItem
	 *          {@link VisualItem} implementation
	 */
	public void hover(final VisualItem paramItem) {
		for (final View view : mViews) {
			if (view.isVisible()) {
				view.hover(paramItem);
			}
		}
	}
}
