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

package org.sirix.settings;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.cache.PageContainer;
import org.sirix.page.NamePage;
import org.sirix.page.NodePage;

/**
 * Enum for providing different revision algorithms. Each kind must implement
 * one method to reconstruct NodePages for Modification and for Reading.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum ERevisioning {

  /**
   * FullDump, just dumping the complete older revision.
   */
  FULL {
    @Override
    public NodePage combineNodePages(@Nonnull final NodePage[] pPages,
      @Nonnegative final int pRevToRestore) {
      assert pPages.length == 1 : "Only one version of the page!";
//      final long nodePageKey = pPages[0].getNodePageKey();
//      final NodePage returnVal =
//        new NodePage(nodePageKey, pPages[0].getRevision());
//
//      for (int i = 0; i < pPages[0].getNodes().length; i++) {
//        returnVal.setNode(i, pPages[0].getNode(i));
//      }

      return pPages[0];
    }

    @Override
    public PageContainer combineNodePagesForModification(
      @Nonnull final NodePage[] pPages,
      @Nonnegative final int pMileStoneRevision) {
      final long nodePageKey = pPages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pPages[0].getRevision() + 1),
          new NodePage(nodePageKey, pPages[0].getRevision() + 1)
        };

      for (int i = 0; i < pPages[0].getNodes().length; i++) {
        returnVal[0].setNode(i, pPages[0].getNode(i));
        if (pPages[0].getNode(i) != null) {
          returnVal[1].setNode(i, pPages[0].getNode(i));
        }
      }

      final PageContainer cont =
        new PageContainer(returnVal[0], returnVal[1]);
      return cont;
    }

    @Override
    public NamePage combineNamePages(@Nonnull final NamePage[] pPages,
      @Nonnegative final int pRevToRestore) {
      assert pPages.length == 1 : "Only one version of the page!";
      return pPages[0];
    }

    @Override
    public NamePage combinePagesForModification(
      @Nonnull final NamePage[] pPages,
      @Nonnegative final int pMileStoneRevision) {
      return null;
    }
  },

  /**
   * Differential. Only the diffs are stored related to the last milestone
   * revision.
   */
  DIFFERENTIAL {
    @Override
    public NodePage combineNodePages(final NodePage[] pPages,
      final int pRevToRestore) {
      assert pPages.length <= 2;
      final long nodePageKey = pPages[0].getNodePageKey();
      final NodePage returnVal =
        new NodePage(nodePageKey, pPages[0].getRevision());
      final NodePage latest = pPages[0];
      NodePage referencePage = pPages.length == 1 ? pPages[0] : pPages[1];

//      for (int i = 1; i < pPages.length; i++) {
//        if (pPages[i].getRevision() % pRevToRestore == 0) {
//          referencePage = pPages[i];
//          break;
//        }
//      }
      assert latest.getNodePageKey() == nodePageKey;
      assert referencePage.getNodePageKey() == nodePageKey;
      for (int i = 0; i < referencePage.getNodes().length; i++) {
        if (latest.getNode(i) != null) {
          returnVal.setNode(i, latest.getNode(i));
        } else {
          returnVal.setNode(i, referencePage.getNode(i));
        }
      }
      return returnVal;
    }

    @Override
    public PageContainer combineNodePagesForModification(
      final NodePage[] pPages, final int pRevToRestore) {
      assert pPages.length <= 2;
      final long nodePageKey = pPages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pPages[0].getRevision() + 1),
          new NodePage(nodePageKey, pPages[0].getRevision() + 1)
        };

      final NodePage latest = pPages[0];
      NodePage fullDump = pPages.length == 1 ? pPages[0] : pPages[1];
//      if (pPages.length >= pRevToRestore + 1) {
//        fullDump = pPages[pRevToRestore];
//      }
//      for (int i = 1; i < pPages.length; i++) {
//        if (pPages[i].getRevision() % pRevToRestore == 0) {
//          fullDump = pPages[i];
//          break;
//        }
//      }

      // iterate through all nodes
      for (int j = 0; j < returnVal[0].getNodes().length; j++) {
        if (latest.getNode(j) != null) {
          returnVal[0].setNode(j, latest.getNode(j));
          returnVal[1].setNode(j, latest.getNode(j));
        } else {
          if (fullDump.getNode(j) != null) {
            returnVal[0].setNode(j, fullDump.getNode(j));
            if ((latest.getRevision() + 1) % pRevToRestore == 0) {
              returnVal[1].setNode(j, fullDump.getNode(j));
            }
          }
        }
      }

      final PageContainer cont =
        new PageContainer(returnVal[0], returnVal[1]);
      return cont;
    }

    @Override
    public NamePage combineNamePages(@Nonnull final NamePage[] pPages,
      @Nonnegative final int pRevToRestore) {
      return null;
    }

    @Override
    public NamePage combinePagesForModification(
      @Nonnull final NamePage[] pPages,
      @Nonnegative final int pMileStoneRevision) {
      return null;
    }
  },

  /**
   * Incremental Revisioning. Each Revision can be reconstructed with the help
   * of the last full-dump plus the incremental steps between.
   */
  INCREMENTAL {
    @Override
    public NodePage combineNodePages(final NodePage[] pPages,
      final int pRevToRestore) {
      final long nodePageKey = pPages[0].getNodePageKey();
      final NodePage returnVal =
        new NodePage(nodePageKey, pPages[0].getRevision());
      for (final NodePage page : pPages) {
        assert page.getNodePageKey() == nodePageKey;
        for (int i = 0, length = page.getNodes().length; i < length; i++) {
          if (page.getNode(i) != null && returnVal.getNode(i) == null) {
            returnVal.setNode(i, page.getNode(i));
          }
        }

        if (page.getRevision() % pRevToRestore == 0) {
          break;
        }
      }

      return returnVal;
    }

    @Override
    public PageContainer combineNodePagesForModification(
      final NodePage[] pPages, final int pRevToRestore) {
      final long nodePageKey = pPages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pPages[0].getRevision() + 1),
          new NodePage(nodePageKey, pPages[0].getRevision() + 1)
        };

      for (final NodePage page : pPages) {
        assert page.getNodePageKey() == nodePageKey;
        for (int i = 0, length = page.getNodes().length; i < length; i++) {
          // Caching the complete page
          if (page.getNode(i) != null && returnVal[0].getNode(i) == null) {
            returnVal[0].setNode(i, page.getNode(i));

            if (returnVal[0].getRevision() % pRevToRestore == 0) {
              returnVal[1].setNode(i, page.getNode(i));
            }
          }
        }
      }

      final PageContainer cont =
        new PageContainer(returnVal[0], returnVal[1]);
      return cont;
    }

    @Override
    public NamePage combineNamePages(@Nonnull final NamePage[] pPages,
      @Nonnegative final int pRevToRestore) {
      return null;
    }

    @Override
    public NamePage combinePagesForModification(
      @Nonnull final NamePage[] pPages,
      @Nonnegative final int pMileStoneRevision) {
      return null;
    }
  };

  public abstract NamePage combineNamePages(@Nonnull final NamePage[] pPages,
    @Nonnegative final int pRevToRestore);

  public abstract NamePage
    combinePagesForModification(@Nonnull final NamePage[] pPages,
      @Nonnegative final int pMileStoneRevision);

  /**
   * Method to reconstruct a complete {@link NodePage} with the help of partly filled
   * pages plus a revision-delta which determines the necessary steps back.
   * 
   * @param pPages
   *          the base of the complete {@link NodePage}
   * @param pRevToRestore
   *          the revision needed to build up the complete milestone
   * @return the complete {@link NodePage}
   */
  public abstract NodePage combineNodePages(@Nonnull final NodePage[] pPages,
    @Nonnegative final int pRevToRestore);

  /**
   * Method to reconstruct a complete {@link NodePage} for reading as well as a
   * NodePage for serializing with the Nodes to write already on there.
   * 
   * @param pPages
   *          the base of the complete {@link NodePage}
   * @param pMileStoneRevision
   *          the revision needed to build up the complete milestone
   * @return a {@link PageContainer} holding a complete {@link NodePage} for reading and one
   *         for writing
   */
  public abstract PageContainer
    combineNodePagesForModification(@Nonnull final NodePage[] pPages,
      @Nonnegative final int pMileStoneRevision);
}
