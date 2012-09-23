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

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sirix.access.Database;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.gui.view.smallmultiple.SmallmultipleView;
import org.sirix.gui.view.sunburst.SunburstView;
import org.sirix.gui.view.text.TextView;
import org.sirix.gui.view.tree.TreeView;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;

/**
 * <h1>GUICommands</h1>
 * 
 * <p>
 * All available GUI commands.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum GUICommands implements IGUICommand {

  /**
   * Open a sirix file.
   */
  OPEN("Open resource", EMenu.MENU) {
    /** {@inheritDoc} */
    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;

      // Create file chooser.
      final MyActionListener mActionListener = new MyActionListener();
      final JFileChooser fc = createFileChooser(mActionListener);

      // Handle open button action.
      if (fc.showOpenDialog(pGUI) == JFileChooser.APPROVE_OPTION) {
        final File file = fc.getSelectedFile();
        LOGWRAPPER.debug("RevNumber: " + mActionListener.getRevision());
        pGUI.execute(file, mActionListener.getRevision());
      }
    }
  },

  /**
   * Shredder an XML-document.
   */
  SHREDDER("Shredder XML-document", EMenu.MENU) {
    /** {@inheritDoc} */
    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      shredder(pGUI, EShredder.NORMAL);
    }
  },

  /**
   * Update a shreddered file.
   */
  SHREDDER_UPDATE("Update resource", EMenu.MENU) {
    /** {@inheritDoc} */
    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      shredder(pGUI, EShredder.UPDATEONLY);
    }
  },

  /**
   * Serialize a sirix storage.
   */
  SERIALIZE("Serialize", EMenu.MENU) {
    /** {@inheritDoc} */
    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;

      // Create file chooser.
      final MyActionListener mActionListener = new MyActionListener();
      final JFileChooser fc = createFileChooser(mActionListener);

      if (fc.showOpenDialog(pGUI) == JFileChooser.APPROVE_OPTION) {
        final File source = fc.getSelectedFile();
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fc.setAcceptAllFileFilterUsed(true);

        final JFileChooser chooser = new JFileChooser();
        if (chooser.showSaveDialog(pGUI) == JFileChooser.APPROVE_OPTION) {
          final File target = chooser.getSelectedFile();
          try {
            final FileOutputStream outputStream = new FileOutputStream(target);

            final IDatabase db = Database.openDatabase(source);
            final ISession session =
              db.getSession(new org.sirix.access.conf.SessionConfiguration.Builder("shredded").build());

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final XMLSerializer serializer =
              new XMLSerializerBuilder(session, outputStream).setIndend(true).setVersions(new int[] {
                mActionListener.getRevision()
              }).build();
            executor.submit(serializer);
            executor.shutdown();
            try {
              executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              LOGWRAPPER.error(e.getMessage(), e);
              return;
            }

            session.close();
            outputStream.close();
            JOptionPane.showMessageDialog(pGUI, "Serializing done!");
          } catch (final SirixException e) {
            LOGWRAPPER.error(e.getMessage(), e);
          } catch (final IOException e) {
            LOGWRAPPER.error(e.getMessage(), e);
          }
        }
      }
    }
  },

  /**
   * Separator.
   */
  SEPARATOR("", EMenu.SEPARATOR) {
    @Override
    public void execute(final GUI pGUI) {
    }
  },

  /**
   * Close sirix GUI.
   */
  QUIT("Quit", EMenu.MENU) {
    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      pGUI.dispose();
    }
  },

  /**
   * Show tree view.
   */
  TREE("Tree", EMenu.CHECKBOXITEM) {
    @Override
    public boolean selected() {
      return GUIProp.EShowViews.SHOWTREE.getValue();
    }

    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      GUIProp.EShowViews.SHOWTREE.invert();
      if (!GUIProp.EShowViews.SHOWTREE.getValue()) {
        TreeView.getInstance(pGUI.getNotifier()).dispose();
      }
      pGUI.getViewContainer().layoutViews();
    }
  },

  /**
   * Show text view.
   */
  TEXT("Text", EMenu.CHECKBOXITEM) {
    @Override
    public boolean selected() {
      return GUIProp.EShowViews.SHOWTEXT.getValue();
    }

    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      GUIProp.EShowViews.SHOWTEXT.invert();
      if (!GUIProp.EShowViews.SHOWTEXT.getValue()) {
        TextView.getInstance(pGUI.getNotifier()).dispose();
      }
      pGUI.getViewContainer().layoutViews();
    }
  },

  /**
   * Show small multiples view.
   */
  SMALLMULTIPLES("Small multiples", EMenu.CHECKBOXITEM) {
    @Override
    public boolean selected() {
      return GUIProp.EShowViews.SHOWSMALLMULTIPLES.getValue();
    }

    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      GUIProp.EShowViews.SHOWSMALLMULTIPLES.invert();
      if (!GUIProp.EShowViews.SHOWSMALLMULTIPLES.getValue()) {
        SmallmultipleView.getInstance(pGUI.getNotifier()).dispose();
      }
      pGUI.getViewContainer().layoutViews();
    }
  },

  /**
   * Show sunburst view.
   */
  SUNBURST("Sunburst", EMenu.CHECKBOXITEM) {
    @Override
    public boolean selected() {
      return GUIProp.EShowViews.SHOWSUNBURST.getValue();
    }

    @Override
    public void execute(final GUI pGUI) {
      assert pGUI != null;
      GUIProp.EShowViews.SHOWSUNBURST.invert();
      if (!GUIProp.EShowViews.SHOWSUNBURST.getValue()) {
        SunburstView.getInstance(pGUI.getNotifier()).dispose();
      }
      pGUI.getViewContainer().layoutViews();
    }
  };

  /** Logger. */
  private static final Logger LOGWRAPPER = LoggerFactory.getLogger(GUICommands.class);

  /** Description of command. */
  private final String mDesc;

  /** Determines menu entry type. */
  private final EMenu mType;

  /**
   * Constructor.
   * 
   * @param pDesc
   *          Description of command
   * @param pType
   *          Determines if menu item is checked or not
   */
  GUICommands(final String pDesc, final EMenu pType) {
    assert pDesc != null;
    assert pType != null;
    mDesc = pDesc;
    mType = pType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String desc() {
    return mDesc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EMenu type() {
    return mType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean selected() {
    throw new IllegalStateException("May not be invoked on this command!");
  }

  /**
   * Create a directory file chooser with the possibility to select a specific revision.
   * 
   * @param pActionListener
   *          {@link MyActionListener} instance
   * @return {@link JFileChooser} instance
   */
  private static JFileChooser createFileChooser(final MyActionListener pActionListener) {
    // Action listener.
    final MyActionListener mActionListener = pActionListener;

    // Create a file chooser.
    final JFileChooser fc = new JFileChooser();
    fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
    fc.setAcceptAllFileFilterUsed(false);

    // Create new panel etc.pp. for choosing the revision at the bottom of the frame.
    final JPanel panel = new JPanel();
    panel.setLayout(new BorderLayout());
    final JComboBox<Long> cb = new JComboBox<Long>();
    cb.addActionListener(mActionListener);

    panel.add(cb, BorderLayout.SOUTH);
    fc.setAccessory(panel);

    final PropertyChangeListener changeListener = new PropertyChangeListener() {
      @Override
      public void propertyChange(final PropertyChangeEvent pEvent) {
        assert pEvent != null;
        assert pEvent.getSource() instanceof JFileChooser;

        // Get last revision number from TT-storage.
        final JFileChooser fileChooser = (JFileChooser)pEvent.getSource();
        final File tmpDir = fileChooser.getSelectedFile();
        long revNumber = 0;

        if (tmpDir != null) {
          // Remove items first.
          cb.removeActionListener(mActionListener);
          cb.removeAllItems();

          // A directory is in focus.
          boolean error = false;

          try {
            final IDatabase db = Database.openDatabase(tmpDir.getAbsoluteFile());
            final INodeReadTrx rtx =
              db.getSession(new org.sirix.access.conf.SessionConfiguration.Builder("shredded").build())
                .beginNodeReadTrx();
            revNumber = rtx.getRevisionNumber();
            rtx.close();
          } catch (final SirixException e) {
            // Selected directory is not a sirix storage.
            error = true;
          }

          if (!error) {
            // Create items, which are used as available revisions.
            for (long i = 0; i <= revNumber; i++) {
              cb.addItem(i);
            }
          }

          cb.addActionListener(mActionListener);
        }
      }
    };
    fc.addPropertyChangeListener(changeListener);

    return fc;
  }

  /**
   * Shredder or shredder into.
   * 
   * @param pGUI
   *          Main GUI frame
   * @param pShredding
   *          Determines which shredder to use
   */
  private static void shredder(final GUI pGUI, final EShredder pShredding) {
    assert pGUI != null;
    assert pShredding != null;

    // Create a file chooser.
    final JFileChooser fc = new JFileChooser();
    fc.setAcceptAllFileFilterUsed(false);
    fc.setFileFilter(new XMLFileFilter());

    if (fc.showOpenDialog(pGUI) == JFileChooser.APPROVE_OPTION) {
      fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
      final File source = fc.getSelectedFile();

      if (fc.showSaveDialog(pGUI) == JFileChooser.APPROVE_OPTION) {
        fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        final File target = fc.getSelectedFile();

        pShredding.shred(source, target);

        try {
          final IDatabase database = Database.openDatabase(target);
          final ISession session = database.getSession(new SessionConfiguration.Builder("shredded").build());
          final INodeReadTrx rtx = session.beginNodeReadTrx();
          final int rev = rtx.getRevisionNumber();
          rtx.close();
          session.close();
          pGUI.execute(target, rev);
        } catch (final SirixException e) {
          LOGWRAPPER.error(e.getMessage(), e);
        }
      }
    }
  }

  /** Action listener to listen for the selection of a revision. */
  private static final class MyActionListener implements ActionListener {
    /** Selected revision. */
    private transient int mRevision;

    @Override
    public void actionPerformed(final ActionEvent pEvent) {
      assert pEvent != null;
      assert pEvent.getSource() instanceof JComboBox;
      final JComboBox<?> cb = (JComboBox<?>)pEvent.getSource();
      if (cb.getSelectedItem() != null) {
        mRevision = (Integer)cb.getSelectedItem();
      }
    };

    /**
     * Get selected revision number.
     * 
     * @return the Revision
     */
    int getRevision() {
      return mRevision;
    }
  }
}
