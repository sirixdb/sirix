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

package org.sirix.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Dimension;
import java.awt.event.MouseEvent;

import org.checkerframework.checker.nullness.qual.Nullable; 
import javax.swing.JComponent;

import org.sirix.gui.GUIProp;
import org.sirix.gui.ProgressGlassPane;
import org.sirix.gui.view.AbstractView;
import org.sirix.gui.view.ProcessingEmbeddedView;
import org.sirix.gui.view.ProcessingView;
import org.sirix.gui.view.View;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.model.SunburstModel;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PApplet;

import com.google.common.base.Optional;

/**
 * 
 * <p>
 * Main sunburst class.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstView extends AbstractView implements View {

	/**
	 * SerialUID.
	 */
	private static final long serialVersionUID = 1L;

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(SunburstView.class));

	/** Name of the sunburst view. */
	private static final String NAME = "SunburstView";

	/** {@link SunburstView} instance. */
	private static SunburstView mView;
	//
	// /** {@link ViewNotifier} to notify views of changes. */
	// private final ViewNotifier mNotifier;

	// /** {@link ReadDB} instance to interact with sirix. */
	// private transient ReadDB mDB;

	// /** Processing {@link PApplet} reference. */
	// private transient Embedded mEmbed;

	/** {@link VisualItem} implementation. */
	private transient VisualItem mItem;

	/**
	 * Constructor.
	 * 
	 * @param paramNotifier
	 *          {@link ViewNotifier} instance.
	 */
	private SunburstView(final ViewNotifier pNotifier) {
		super(pNotifier);
	}

	/**
	 * Singleton factory method.
	 * 
	 * @param paramNotifier
	 *          {@link ViewNotifier} to notify views of changes etc.pp.
	 * @return {@link SunburstView} instance
	 */
	public static synchronized SunburstView getInstance(
			final ViewNotifier paramNotifier) {
		if (mView == null) {
			mView = new SunburstView(paramNotifier);
		}

		return mView;
	}

	// /** Update window size. */
	// void updateWindowSize() {
	// assert mGUI != null;
	// final Dimension dim = mGUI.getSize();
	// setSize(dim.width, dim.height - 42);
	// if (mEmbed != null && mEmbed.focused) {
	// // mEmbed.size(dim.width, dim.height - 42, PConstants.JAVA2D);
	//
	// if (mEmbed.mControl.getGUIInstance().mDone) {
	// mEmbed.update();
	// }
	// }
	// }

	/**
	 * Not supported.
	 * 
	 * @see Object#clone()
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

	@Override
	public boolean isVisible() {
		return GUIProp.EShowViews.SHOWSUNBURST.getValue();
	}

	@Override
	public String name() {
		return NAME;
	};

	@Override
	public JComponent component() {
		return this;
	}

	// /**
	// * {@inheritDoc}
	// */
	// @Override
	// public void refreshInit() {
	// mDB = mNotifier.getGUI().getReadDB();
	// boolean firstInit = mEmbed == null ? true : false;
	//
	// // Create instance of processing innerclass.
	// if (!firstInit) {
	// mEmbed.mControl.getGUIInstance().getApplet().dispose();
	// mEmbed.mControl.getGUIInstance().resetGUI();
	// mEmbed.mControl.resetControl();
	// mEmbed.mEmbeddedView.resetEmbedded();
	// mEmbed.mEmbeddedView = null;
	// mEmbed.dispose();
	// mEmbed.stop();
	// remove(mEmbed);
	// mEmbed = null;
	// }
	// mEmbed = new Embedded(this, mNotifier);
	// add(mEmbed);
	// /*
	// * Important to call this whenever embedding a PApplet.
	// * It ensures that the animation thread is started and
	// * that other internal variables are properly set.
	// */
	// mEmbed.init();
	// }

	// /**
	// * {@inheritDoc}
	// */
	// @Override
	// public void dispose() {
	// if (mEmbed != null) {
	// mEmbed.noLoop();
	// mEmbed.mControl.getGUIInstance().getApplet().dispose();
	// mEmbed.mControl.getGUIInstance().resetGUI();
	// mEmbed.mControl.resetControl();
	// mEmbed.mEmbeddedView.resetEmbedded();
	// mEmbed.mEmbeddedView = null;
	// mEmbed.dispose();
	// mEmbed.stop();
	// remove(mEmbed);
	// mEmbed = null;
	// }
	// }

	@Override
	public Dimension getPreferredSize() {
		final Dimension parentFrame = getGUI().getSize();
		return new Dimension(parentFrame.width, parentFrame.height - 60);
	}

	/** Embedded processing view. */
	public final class Embedded extends PApplet implements ProcessingView {

		/** Serial UID. */
		private static final long serialVersionUID = 1L;

		/** The sirix {@link SunburstModel}. */
		private transient SunburstModel mModel;

		/** {@link ProcessingEmbeddedView} reference. */
		private transient ProcessingEmbeddedView mEmbeddedView;

		/** The actual view. */
		private final SunburstView mView;

		/** {@link SunburstControl} reference for user interaction. */
		private transient SunburstControl mControl;

		/** {@link ProgressGlassPane} reference. */
		private transient ProgressGlassPane mGlassPane;

		/**
		 * Constructor.
		 * 
		 * @param paramView
		 *          reference of class which implements the {@link View} interface
		 */
		public Embedded(final SunburstView paramView) {
			mView = checkNotNull(paramView);
		}

		@Override
		public void setup() {
			final Dimension dim = mView.component().getSize();
			setSize(dim.width - 2, dim.height);
			size(dim.width - 2, dim.height);
			// frame.setResizable(true);
			refreshInit();
		}

		/** Setup processing view. */
		public void refreshInit() {
			// Set glass pane.
			mView.getGUI().setGlassPane(mGlassPane = new ProgressGlassPane());
			mGlassPane.setVisible(true);

			// Initialization with no draw() loop.
			noLoop();

			// Frame rate reduced to 35.
			frameRate(35);

			// Create Model.
			mModel = new SunburstModel(this, getDB());

			// Create Controller.
			mControl = SunburstControl.getInstance(this, mModel, getDB());

			// Use embedded view.
			mEmbeddedView = ProcessingEmbeddedView.getInstance(this, mView,
					mControl.getGUIInstance(), mControl, getNotifier());
		}

		@Override
		public void draw() {
			if (mEmbeddedView != null) {
				mEmbeddedView.draw();
			}
		}

		@Override
		public void mouseEntered(final MouseEvent paramEvent) {
			if (mEmbeddedView != null) {
				mEmbeddedView.mouseEntered(paramEvent);
			}
		}

		@Override
		public void mouseExited(final MouseEvent paramEvent) {
			if (mEmbeddedView != null) {
				mEmbeddedView.mouseExited(paramEvent);
			}
		}

		@Override
		public void keyReleased() {
			if (mEmbeddedView != null) {
				mEmbeddedView.keyReleased();
			}
		}

		@Override
		public void mousePressed(final MouseEvent paramEvent) {
			if (mEmbeddedView != null) {
				mEmbeddedView.mousePressed(paramEvent);
			}
		}

		/** Refresh. */
		@Override
		public void refreshUpdate() {
			mControl.refreshUpdate(getDB());
			mEmbeddedView.handleHLWeight();
		}

		/** Refresh. Thus sirix storage has been updated to a new revision. */
		public void refresh(final Optional<VisualItemAxis> pAxis) {
			checkNotNull(pAxis);
			getNotifier().update(mView, pAxis);
		}

		/** Update Processing GUI. */
		@Override
		public void update() {
			if (mEmbeddedView != null) {
				mEmbeddedView.updateGUI();
			}
		}

		@Override
		public SunburstControl getController() {
			assert mControl != null;
			return mControl;
		}

		@Override
		public View getView() {
			assert mView != null;
			return mView;
		}

		@Override
		public ProgressGlassPane getGlassPane() {
			assert mGlassPane != null;
			return mGlassPane;
		}

		@Override
		public AbstractSunburstGUI getGUI() {
			return mControl.getGUIInstance();
		}

		@Override
		public ProcessingEmbeddedView getEmbeddedView() {
			return mEmbeddedView;
		}

		@Override
		public void setEmbeddedView(
				@Nullable final ProcessingEmbeddedView pEmbeddedView) {
			mEmbeddedView = pEmbeddedView;
		}

		@Override
		public PApplet getApplet() {
			return this;
		}

		@Override
		public boolean isFocused() {
			return focused;
		}

		@Override
		public boolean isDone() {
			return mControl.getGUIInstance().isDone();
		}

		@Override
		public Model<?, ?> getModel() {
			return mModel;
		}
	}

	@Override
	public void hover(final VisualItem paramItem) {
		checkNotNull(paramItem);
		if (mItem == null || mItem != paramItem) {
			mItem = paramItem;
			getNotifier().hover(paramItem);
		}
	}

	@Override
	protected ProcessingView getEmbeddedInstance() {
		return new Embedded(this);
	}
}
