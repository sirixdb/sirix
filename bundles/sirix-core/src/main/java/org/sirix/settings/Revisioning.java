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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.cache.RecordPageContainer;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.RecordPage;

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
		public <S, T extends RecordPage<S>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() == 1 : "Only one version of the page!";
			return pages.get(0);
		}

		@Override
		public <S, T extends RecordPage<S>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final @Nonnegative int mileStoneRevision,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() == 1;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getRecordPageKey();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));

			for (final Record nodes : pages.get(0).values()) {
				returnVal.get(0).setRecord(nodes);
				returnVal.get(1).setRecord(nodes);
			}

			final RecordPageContainer<T> cont = new RecordPageContainer<>(
					returnVal.get(0), returnVal.get(1));
			return cont;
		}
	},

	/**
	 * Differential. Only the diffs are stored related to the last milestone
	 * revision.
	 */
	DIFFERENTIAL {
		@Override
		public <S, T extends RecordPage<S>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= 2;
			final long recordPageKey = pages.get(0).getRecordPageKey();
			final T returnVal = pages.get(0).newInstance(recordPageKey,
					pages.get(0).getRevision(), pageReadTrx);
			if (pages.size() == 2) {
				returnVal.setDirty(true);
			}
			final T latest = pages.get(0);
			T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

			assert latest.getRecordPageKey() == recordPageKey;
			assert fullDump.getRecordPageKey() == recordPageKey;

			for (final Record node : fullDump.values()) {
				returnVal.setRecord(node);
			}

			if (pages.size() == 2) {
				for (final Record node : latest.values()) {
					returnVal.setRecord(node);
				}
			}
			return returnVal;
		}

		@Override
		public <S, T extends RecordPage<S>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= 2;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getRecordPageKey();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));

			final T latest = firstPage;
			T fullDump = pages.size() == 1 ? firstPage : pages.get(1);

			for (final Record node : fullDump.values()) {
				returnVal.get(0).setRecord(node);

				if ((latest.getRevision() + 1) % revToRestore == 0) {
					// Fulldump.
					returnVal.get(1).setRecord(node);
				}
			}

			// iterate through all nodes
			for (final Record node : latest.values()) {
				returnVal.get(0).setRecord(node);
				returnVal.get(1).setRecord(node);
			}

			final RecordPageContainer<T> cont = new RecordPageContainer<>(
					returnVal.get(0), returnVal.get(1));
			return cont;
		}
	},

	/**
	 * Incremental Revisioning. Each Revision can be reconstructed with the help
	 * of the last full-dump plus the incremental steps between.
	 */
	INCREMENTAL {
		@Override
		public <S, T extends RecordPage<S>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= revToRestore;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getRecordPageKey();
			final T returnVal = firstPage.newInstance(firstPage.getRecordPageKey(),
					firstPage.getRevision(), firstPage.getPageReadTrx());
			if (pages.size() > 1) {
				returnVal.setDirty(true);
			}

			for (final RecordPage<S> page : pages) {
				assert page.getRecordPageKey() == recordPageKey;
				for (final Entry<S, Record> node : page.entrySet()) {
					final S nodeKey = node.getKey();
					if (returnVal.getRecord(nodeKey) == null) {
						returnVal.setRecord(node.getValue());
					}
				}
			}

			return returnVal;
		}

		@Override
		public <S, T extends RecordPage<S>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getRecordPageKey();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getRevision() + 1, pageReadTrx));

			for (final T page : pages) {
				assert page.getRecordPageKey() == recordPageKey;

				for (final Entry<S, Record> node : page.entrySet()) {
					// Caching the complete page.
					final S nodeKey = node.getKey();
					if (node != null && returnVal.get(0).getRecord(nodeKey) == null) {
						returnVal.get(0).setRecord(node.getValue());

						if (returnVal.get(1).getRecord(node.getKey()) == null
								&& returnVal.get(0).getRevision() % revToRestore == 0) {
							returnVal.get(1).setRecord(node.getValue());
						}
					}
				}
			}

			final RecordPageContainer<T> cont = new RecordPageContainer<>(
					returnVal.get(0), returnVal.get(1));
			return cont;
		}
	};

	/**
	 * Method to reconstruct a complete {@link RecordPage} with the
	 * help of partly filled pages plus a revision-delta which determines the
	 * necessary steps back.
	 * 
	 * @param pages
	 *          the base of the complete {@link RecordPage}
	 * @param revToRestore
	 *          the revision needed to build up the complete milestone
	 * @return the complete {@link RecordPage}
	 */
	public abstract <S, T extends RecordPage<S>> T combineRecordPages(
			final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Method to reconstruct a complete {@link RecordPage} for reading as well
	 * as a {@link RecordPage} for serializing with the nodes to write.
	 * 
	 * @param pages
	 *          the base of the complete {@link RecordPage}
	 * @param mileStoneRevision
	 *          the revision needed to build up the complete milestone
	 * @return a {@link RecordPageContainer} holding a complete
	 *         {@link RecordPage} for reading and one for writing
	 */
	public abstract <S, T extends RecordPage<S>> RecordPageContainer<T> combineRecordPagesForModification(
			final @Nonnull List<T> pages, final @Nonnegative int mileStoneRevision,
			final @Nonnull PageReadTrx pageReadTrx);
}
