package org.sirix.gui.view;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;

import java.awt.Dimension;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowStateListener;

import javax.swing.JPanel;

import org.slf4j.LoggerFactory;
import org.sirix.exception.AbsTTException;
import org.sirix.gui.GUI;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.sunburst.SunburstView;
import org.sirix.utils.LogWrapper;

import processing.core.PApplet;

public abstract class AbsView extends JPanel implements IView {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(AbsView.class));

  /**
   * Default serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  /** {@link GUI} reference. */
  private final GUI mGUI;

  /** {@link ViewNotifier} to notify views of changes. */
  private final ViewNotifier mNotifier;

  /** Processing {@link PApplet} reference. */
  private transient IProcessingView mEmbed;

  /** {@link ReadDB} instance to interact with sirix. */
  private transient ReadDB mDB;

  protected AbsView(final ViewNotifier pNotifier) {
    // Add view to notifier.
    mNotifier = checkNotNull(pNotifier);
    mNotifier.add(this);

    setSize(mNotifier.getGUI().getSize());

    // Main GUI frame.
    mGUI = mNotifier.getGUI();

    // Simple scroll mode, because we are adding a heavyweight component (PApplet to the JScrollPane).
    // getViewport().setScrollMode(JViewport.SIMPLE_SCROLL_MODE);
    // setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
    // setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);

    mGUI.addWindowStateListener(new WindowStateListener() {
      @Override
      public void windowStateChanged(WindowEvent e) {
        updateWindowSize();
      }
    });

    mGUI.addComponentListener(new ComponentListener() {
      @Override
      public void componentResized(final ComponentEvent paramEvt) {
        updateWindowSize();
      }

      @Override
      public void componentHidden(final ComponentEvent paramEvt) {

      }

      @Override
      public void componentMoved(final ComponentEvent paramEvt) {

      }

      @Override
      public void componentShown(final ComponentEvent paramEvt) {

      }
    });
  }

  /** Update window size. */
  protected void updateWindowSize() {
    assert mGUI != null;
    final Dimension dim = mGUI.getSize();
    setSize(dim.width, dim.height - 42);
    if (mEmbed != null && mEmbed.isFocused()) {
      // mEmbed.size(dim.width, dim.height - 42, PConstants.JAVA2D);

      if (mEmbed.isDone()) {
        mEmbed.update();
      }
    }
  }

  /**
   * Get view notifier.
   * 
   * @return {@link ViewNotifier} reference
   */
  public ViewNotifier getNotifier() {
    return mNotifier;
  }

  /**
   * Get main GUI reference.
   * 
   * @return {@link GUI} reference
   */
  public GUI getGUI() {
    return mGUI;
  }

  /**
   * Get database handle.
   * 
   * @return {@link ReadDB} instance
   */
  public ReadDB getDB() {
    return mDB;
  }

  @Override
  public void resize() {
    updateWindowSize();
  }

  @Override
  public void refreshInit() {
    mDB = mNotifier.getGUI().getReadDB();
    boolean firstInit = mEmbed == null ? true : false;

    // Create instance of processing innerclass.
    if (!firstInit) {
      mEmbed.getGUI().getApplet().dispose();
      mEmbed.getGUI().resetGUI();
      mEmbed.getController().resetControl();
      mEmbed.getEmbeddedView().resetEmbedded();
      mEmbed.setEmbeddedView(null);
      mEmbed.dispose();
      mEmbed.stop();
      remove(mEmbed.getApplet());
      mEmbed = null;
    }
    mEmbed = getEmbeddedInstance();
    final PApplet applet = mEmbed.getApplet();
    add(applet);
    /*
     * Important to call this whenever embedding a PApplet.
     * It ensures that the animation thread is started and
     * that other internal variables are properly set.
     */
    applet.init();
  }

  /** Create a concrete {@link IProcessingView} implementation instance. */
  protected abstract IProcessingView getEmbeddedInstance();

  @Override
  public void refreshUpdate(final Optional<VisualItemAxis> pAxis) {
    try {
      mDB = ViewUtilities.refreshResource(mDB);
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    mEmbed.refreshUpdate();
  }

  @Override
  public void dispose() {
    if (mEmbed != null) {
      mEmbed.noLoop();
      mEmbed.getGUI().getApplet().dispose();
      mEmbed.getGUI().resetGUI();
      mEmbed.getController().resetControl();
      mEmbed.getEmbeddedView().resetEmbedded();
      mEmbed.setEmbeddedView(null);
      mEmbed.dispose();
      mEmbed.stop();
      remove(mEmbed.getApplet());
      mEmbed = null;
    }
  }
}
