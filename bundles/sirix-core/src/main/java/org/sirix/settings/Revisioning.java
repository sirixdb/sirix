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

import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.cache.NodePageContainer;
import org.sirix.node.interfaces.Record;
import org.sirix.page.RecordPage;

/**
 * Enum for providing different revision algorithms. Each kind must implement
 * one method to reconstruct NodePages for Modification and for Reading.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum Revisioning {

	/**
	 * FullDump, just dumping the complete older revision.
	 */
	FULL {
		@Override
		public RecordPage combineNodePages(final @Nonnull RecordPage[] pages,
				final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.length == 1 : "Only one version of the page!";
			return pages[0];
		}

		@Override
		public NodePageContainer combineNodePagesForModification(
				final @Nonnull RecordPage[] pages,
				final @Nonnegative int mileStoneRevision,
				final @Nonnull PageReadTrx pageReadTrx) {
			final long nodePageKey = pages[0].getNodePageKey();
			final RecordPage[] returnVal = {
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx),
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx) };

			for (final Record nodes : pages[0].values()) {
				returnVal[0].setNode(nodes);
				returnVal[1].setNode(nodes);
			}

			final NodePageContainer cont = new NodePageContainer(returnVal[0],
					returnVal[1]);
			return cont;
		}
	},

	/**
	 * Differential. Only the diffs are stored related to the last milestone
	 * revision.
	 */
	DIFFERENTIAL {
		@Override
		public RecordPage combineNodePages(final @Nonnull RecordPage[] pages,
				final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.length <= 2;
			final long nodePageKey = pages[0].getNodePageKey();
			final RecordPage returnVal = new RecordPage(nodePageKey,
					pages[0].getRevision(), pageReadTrx);
			final RecordPage latest = pages[0];
			RecordPage fullDump = pages.length == 1 ? pages[0] : pages[1];

			assert latest.getNodePageKey() == nodePageKey;
			assert fullDump.getNodePageKey() == nodePageKey;

			for (final Record node : fullDump.values()) {
				returnVal.setNode(node);
			}

			for (final Record node : latest.values()) {
				returnVal.setNode(node);
			}
			return returnVal;
		}

		@Override
		public NodePageContainer combineNodePagesForModification(
				final @Nonnull RecordPage[] pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.length <= 2;
			final long nodePageKey = pages[0].getNodePageKey();
			final RecordPage[] returnVal = {
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx),
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx) };

			final RecordPage latest = pages[0];
			RecordPage fullDump = pages.length == 1 ? pages[0] : pages[1];

			for (final Record node : fullDump.values()) {
				returnVal[0].setNode(node);

				if ((latest.getRevision() + 1) % revToRestore == 0) {
					// Fulldump.
					returnVal[1].setNode(node);
				}
			}

			// iterate through all nodes
			for (final Record node : latest.values()) {
				returnVal[0].setNode(node);
				returnVal[1].setNode(node);
			}

			final NodePageContainer cont = new NodePageContainer(returnVal[0],
					returnVal[1]);
			return cont;
		}
	},

	/**
	 * Incremental Revisioning. Each Revision can be reconstructed with the help
	 * of the last full-dump plus the incremental steps between.
	 */
	INCREMENTAL {
		@Override
		public RecordPage combineNodePages(final @Nonnull RecordPage[] pages,
				final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.length <= revToRestore;
			final long nodePageKey = pages[0].getNodePageKey();
			final RecordPage returnVal = new RecordPage(nodePageKey,
					pages[0].getRevision(), pageReadTrx);

			for (final RecordPage page : pages) {
				assert page.getNodePageKey() == nodePageKey;
				for (final Entry<Long, Record> node : page.entrySet()) {
					final long nodeKey = node.getKey();
					if (returnVal.getNode(nodeKey) == null) {
						returnVal.setNode(node.getValue());
					}
				}

				if (page.getRevision() % revToRestore == 0) {
					break;
				}
			}

			return returnVal;
		}

		@Override
		public NodePageContainer combineNodePagesForModification(
				final @Nonnull RecordPage[] pages, final int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			final long nodePageKey = pages[0].getNodePageKey();
			final RecordPage[] returnVal = {
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx),
					new RecordPage(nodePageKey, pages[0].getRevision() + 1, pageReadTrx) };

			for (final RecordPage page : pages) {
				assert page.getNodePageKey() == nodePageKey;

				for (final Entry<Long, Record> node : page.entrySet()) {
					// Caching the complete page.
					final long nodeKey = node.getKey();
					if (node != null && returnVal[0].getNode(nodeKey) == null) {
						returnVal[0].setNode(node.getValue());

						if (returnVal[1].getNode(node.getKey()) == null
								&& returnVal[0].getRevision() % revToRestore == 0) {
							returnVal[1].setNode(node.getValue());
						}
					}
				}
			}

			final NodePageContainer cont = new NodePageContainer(returnVal[0],
					returnVal[1]);
			return cont;
		}
	};

	/**
	 * Method to reconstruct a complete {@link RecordPage} with the help of partly
	 * filled pages plus a revision-delta which determines the necessary steps
	 * back.
	 * 
	 * @param pages
	 *          the base of the complete {@link RecordPage}
	 * @param revToRestore
	 *          the revision needed to build up the complete milestone
	 * @return the complete {@link RecordPage}
	 */
	public abstract RecordPage combineNodePages(final @Nonnull RecordPage[] pages,
			final @Nonnegative int revToRestore,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Method to reconstruct a complete {@link RecordPage} for reading as well as a
	 * NodePage for serializing with the Nodes to write already on there.
	 * 
	 * @param pages
	 *          the base of the complete {@link RecordPage}
	 * @param mileStoneRevision
	 *          the revision needed to build up the complete milestone
	 * @return a {@link NodePageContainer} holding a complete {@link RecordPage} for
	 *         reading and one for writing
	 */
	public abstract NodePageContainer combineNodePagesForModification(
			final @Nonnull RecordPage[] pages,
			final @Nonnegative int mileStoneRevision,
			final @Nonnull PageReadTrx pageReadTrx);
}
