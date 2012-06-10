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

package org.treetank.settings;

import org.treetank.cache.NodePageContainer;
import org.treetank.page.NodePage;

/**
 * Enum for providing different revision algorithms. Each kind must implement
 * one method to reconstruct NodePages for Modification and for Reading.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public enum ERevisioning {

  /**
   * FullDump, Just dumping the complete older revision.
   */
  FULLDUMP {
    @Override
    public NodePage combinePages(final NodePage[] pages, final int revToRestore) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage returnVal = new NodePage(nodePageKey, pages[0].getRevision());

      for (int i = 0; i < pages[0].getNodes().length; i++) {
        returnVal.setNode(i, pages[0].getNode(i));
      }

      return returnVal;
    }

    @Override
    public NodePageContainer combinePagesForModification(final NodePage[] pages, final int mileStoneRevision) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pages[0].getRevision() + 1),
          new NodePage(nodePageKey, pages[0].getRevision() + 1)
        };

      for (int i = 0; i < pages[0].getNodes().length; i++) {
        returnVal[0].setNode(i, pages[0].getNode(i));
        if (pages[0].getNode(i) != null) {
          returnVal[1].setNode(i, pages[0].getNode(i));
        }
      }

      final NodePageContainer cont = new NodePageContainer(returnVal[0], returnVal[1]);
      return cont;
    }
  },

  /**
   * Differential. Only the diffs are stored related to the last milestone
   * revision
   */
  DIFFERENTIAL {
    @Override
    public NodePage combinePages(final NodePage[] pages, final int revToRestore) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage returnVal = new NodePage(nodePageKey, pages[0].getRevision());
      final NodePage latest = pages[0];

      NodePage referencePage = pages[0];

      for (int i = 1; i < pages.length; i++) {
        if (pages[i].getRevision() % revToRestore == 0) {
          referencePage = pages[i];
          break;
        }
      }
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
    public NodePageContainer combinePagesForModification(final NodePage[] pages, final int revToRestore) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pages[0].getRevision() + 1),
          new NodePage(nodePageKey, pages[0].getRevision() + 1)
        };

      final NodePage latest = pages[0];
      NodePage fullDump = pages[0];
      for (int i = 1; i < pages.length; i++) {
        if (pages[i].getRevision() % revToRestore == 0) {
          fullDump = pages[i];
          break;
        }
      }

      // iterate through all nodes
      for (int j = 0; j < returnVal[0].getNodes().length; j++) {
        if (latest.getNode(j) != null) {
          returnVal[0].setNode(j, latest.getNode(j));
          returnVal[1].setNode(j, latest.getNode(j));
        } else {
          if (fullDump.getNode(j) != null) {
            returnVal[0].setNode(j, fullDump.getNode(j));
            if ((latest.getRevision() + 1) % revToRestore == 0) {
              returnVal[1].setNode(j, fullDump.getNode(j));
            }
          }
        }

      }

      final NodePageContainer cont = new NodePageContainer(returnVal[0], returnVal[1]);
      return cont;
    }
  },

  /**
   * Incremental Revisioning. Each Revision can be reconstructed with the help
   * of the last full-dump plus the incremental steps between.
   */
  INCREMENTAL {
    @Override
    public NodePage combinePages(final NodePage[] pages, final int revToRestore) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage returnVal = new NodePage(nodePageKey, pages[0].getRevision());
      for (final NodePage page : pages) {
        assert page.getNodePageKey() == nodePageKey;
        for (int i = 0, length = page.getNodes().length; i < length; i++) {
          if (page.getNode(i) != null && returnVal.getNode(i) == null) {
            // final int offset =
            // (page.hasDeletedNodes() && length < IConstants.NDP_NODE_COUNT)
            // ? (int)((float)(i / (float)(length - 1)) * (IConstants.NDP_NODE_COUNT - 1))
            // : i;
            returnVal.setNode(i, page.getNode(i));
          }
        }

        if (page.getRevision() % revToRestore == 0) {
          break;
        }
      }

      return returnVal;
    }

    @Override
    public NodePageContainer combinePagesForModification(final NodePage[] pages, final int revToRestore) {
      final long nodePageKey = pages[0].getNodePageKey();
      final NodePage[] returnVal =
        {
          new NodePage(nodePageKey, pages[0].getRevision() + 1),
          new NodePage(nodePageKey, pages[0].getRevision() + 1)
        };

      for (final NodePage page : pages) {
        assert page.getNodePageKey() == nodePageKey;
        for (int i = 0, length = page.getNodes().length; i < length; i++) {
          // Caching the complete page
          if (page.getNode(i) != null && returnVal[0].getNode(i) == null) {
            // final int offset =
            // (page.hasDeletedNodes() && length < IConstants.NDP_NODE_COUNT)
            // ? (int)((float)(i / (float)(length - 1)) * (IConstants.NDP_NODE_COUNT - 1))
            // : i;
            returnVal[0].setNode(i, page.getNode(i));

            if (returnVal[0].getRevision() % revToRestore == 0) {
              returnVal[1].setNode(i, page.getNode(i));
            }
          }
        }
      }

      final NodePageContainer cont = new NodePageContainer(returnVal[0], returnVal[1]);
      return cont;
    }
  };

  /**
   * Method to reconstruct a complete NodePage with the help of partly filled
   * pages plus a revision-delta which determines the necessary steps back.
   * 
   * @param pages
   *          the base of the complete Nodepage
   * @param revToRestore
   *          the revision needed to build up the complete milestone.
   * @return the complete NodePage
   */
  public abstract NodePage combinePages(final NodePage[] pages, final int revToRestore);

  /**
   * Method to reconstruct a complete NodePage for reading as well as a
   * NodePage for serializing with the Nodes to write already on there.
   * 
   * @param pages
   *          the base of the complete Nodepage
   * @param mileStoneRevision
   *          the revision needed to build up the complete milestone.
   * @return a NodePageContainer holding a complete NodePage for reading a one
   *         for writing
   */
  public abstract NodePageContainer combinePagesForModification(final NodePage[] pages,
    final int mileStoneRevision);
}
