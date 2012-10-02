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

package org.sirix.gui;

import java.awt.AlphaComposite;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.SplashScreen;
import java.awt.Toolkit;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.WindowConstants;

import org.sirix.exception.SirixException;
import org.sirix.gui.view.IView;
import org.sirix.gui.view.ViewContainer;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.gui.view.smallmultiple.SmallmultipleView;
import org.sirix.gui.view.sunburst.SunburstView;
import org.sirix.gui.view.text.TextView;
import org.sirix.gui.view.tree.TreeView;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * <h1>sirix GUI</h1>
 * 
 * <p>
 * Main GUI frame.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class GUI extends JFrame {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(GUI.class));

	/** Serialization UID. */
	private static final long serialVersionUID = 7396552752125858796L;

	/** Optionally set the look and feel. */
	private static boolean mUseSystemLookAndFeel;

	/** Width of the frame. */
	private static final int WIDTH = 1200;

	/** Height of the frame. */
	private static final int HEIGHT = 900;

	// /** {@link GUIProp} reference. */
	// private final GUIProp mProp; // Will be used in future versions (more GUI
	// properties).

	/**
	 * {@link ViewNotifier} to notify all views of changes in the underlying data
	 * structure.
	 */
	private final ViewNotifier mNotifier;

	/**
	 * {@link ViewContainer} which contains all {@link IView} implementations
	 * available.
	 */
	private final ViewContainer mContainer;

	/** {@link ReadDB}. */
	private transient ReadDB mReadDB;

	/**
	 * Constructor.
	 * 
	 * @param pProp
	 *          {@link GUIProp}
	 */
	public GUI(final GUIProp pProp) {
		// mProp = pProp;

		// ===== Setup GUI ======
		// Title of the frame.
		setTitle("sirix GUI");

		// Set default size and close operation.
		final Dimension frameSize = new Dimension(WIDTH, HEIGHT);
		setSize(frameSize);
		addWindowListener(new WindowListener() {
			@Override
			public void windowOpened(WindowEvent e) {
			}
			
			@Override
			public void windowIconified(WindowEvent e) {
			}
			
			@Override
			public void windowDeiconified(WindowEvent e) {
			}
			
			@Override
			public void windowDeactivated(WindowEvent e) {
			}
			
			@Override
			public void windowClosing(WindowEvent e) {
				dispose();
			}
			
			@Override
			public void windowClosed(final WindowEvent e) {
				dispose();
			}
			
			@Override
			public void windowActivated(WindowEvent e) {
			}
		});
		setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

		// // Create Panels.
		// final JPanel top = new JPanel();
		// top.setLayout(new BorderLayout());

		// Create views.
		mNotifier = new ViewNotifier(this);
		mContainer = ViewContainer.getInstance(this,
				TreeView.getInstance(mNotifier), SunburstView.getInstance(mNotifier),
				TextView.getInstance(mNotifier),
				SmallmultipleView.getInstance(mNotifier));
		mContainer.layoutViews();
		// top.add(mContainer, BorderLayout.PAGE_START);
		getContentPane().add(mContainer);

		// Component listener, to revalidate layout manager.
		addComponentListener(new ComponentAdapter() {
			/**
			 * Relayout all components.
			 * 
			 * @param pEvent
			 *          {@link ComponentEvent} reference
			 */
			@Override
			public void componentResized(final ComponentEvent pEvent) {
				mContainer.revalidate();
			}
		});

		// Create a splash screen.
		try {
			final SplashScreen splash = SplashScreen.getSplashScreen();
			final Graphics2D g = (Graphics2D) splash.createGraphics();
			final Dimension size = splash.getSize();
			g.setComposite(AlphaComposite.Clear);
			g.fillRect(0, 0, size.width, size.height);
			g.setPaintMode();
			splash.close();
		} catch (final NullPointerException e) {
			LOGWRAPPER.warn(e.getMessage(), e);
		}

		// Add menubar.
		final JMenuBar menuBar = new GUIMenuBar(this);
		setJMenuBar(menuBar);

		// Center the frame.
		setLocationRelativeTo(null);

		// Size the frame.
		pack();

		// Display the window.
		setVisible(true);

		// Bring this window to the front.
		toFront();
	}

	/**
	 * Execute command.
	 * 
	 * @param pFile
	 *          {@link File} to open
	 * @param pRevision
	 *          determines the revision to open
	 */
	public void execute(final @Nonnull File pFile,
			final @Nonnegative int pRevision) {
		if (mReadDB == null
				|| !pFile.equals(mReadDB.getDatabase().getDatabaseConfig().getFile())
				|| pRevision != mReadDB.getRevisionNumber()) {
			if (mReadDB != null) {
				mNotifier.dispose();
				mReadDB.close();
			}
			try {
				mReadDB = new ReadDB(pFile, pRevision);
			} catch (final SirixException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
		mNotifier.init(null);
	}

	@Override
	public void dispose() {
		mNotifier.dispose();
		if (mReadDB != null) {
			mReadDB.close();
		}
		super.dispose();
		System.exit(0);
	}

	/**
	 * Get the {@link ReadDB} instance.
	 * 
	 * @return the {@link ReadDB} instance
	 */
	public ReadDB getReadDB() {
		return mReadDB;
	}

	/**
	 * Get view container.
	 * 
	 * @return the Container
	 */
	public ViewContainer getViewContainer() {
		return mContainer;
	}

	/**
	 * Create the GUI and show it. For thread safety, this method should be
	 * invoked from the event dispatch thread.
	 */
	private static void createAndShowGUI() {
		ExceptionReporting.registerExceptionReporter();

		// Added to handle possible JDK 1.6 bug (thanks to Makoto Yui and the BaseX
		// guys).
		UIManager.getInstalledLookAndFeels();

		// Refresh views when windows are resized (thanks to the BaseX guys).
		Toolkit.getDefaultToolkit().setDynamicLayout(true);

		if (mUseSystemLookAndFeel) {
			try {
				UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			} catch (final ClassNotFoundException | InstantiationException
					| IllegalAccessException | UnsupportedLookAndFeelException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		} else {
			try {
				for (final LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
					if ("Nimbus".equals(info.getName())) {
						UIManager.setLookAndFeel(info.getClassName());
						break;
					}
				}
			} catch (final Exception e) {
				// If Nimbus is not available, you can set the GUI to another look and
				// feel.
				try {
					UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException | UnsupportedLookAndFeelException exc) {
					LOGWRAPPER.error(exc.getMessage(), exc);
				}
			}
		}

		// Create GUI.
		new GUI(new GUIProp());
	}

	/**
	 * Main method.
	 * 
	 * @param args
	 *          Not used.
	 */
	public static void main(final String[] args) {
		/*
		 * Schedule a job for the event dispatch thread: creating and showing this
		 * application's GUI.
		 */
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				createAndShowGUI();
			}
		});
	}

	/**
	 * Get {@link ViewNotifier}.
	 * 
	 * @return {@link ViewNotifier} reference
	 */
	public ViewNotifier getNotifier() {
		return mNotifier;
	}
}
