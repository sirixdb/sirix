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
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.cache.RecordPageContainer;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;

/**
 * Enum for providing different revision algorithms. Each kind must implement
 * one method to reconstruct key/value pages for modification and for reading.
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
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() == 1 : "Only one version of the page!";
			return pages.get(0);
		}

		@Override
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final @Nonnegative int mileStoneRevision,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() == 1;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getPageKey();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), firstPage.getRevision() + 1, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), firstPage.getRevision() + 1, pageReadTrx));

			for (final Map.Entry<K, V> entry : pages.get(0).entrySet()) {
				returnVal.get(0).setEntry(entry.getKey(), entry.getValue());
				returnVal.get(1).setEntry(entry.getKey(), entry.getValue());
			}

			return new RecordPageContainer<>(returnVal.get(0), returnVal.get(1));
		}

		@Override
		public int[] getRevisionRoots(@Nonnegative int previousRevision,
				@Nonnegative int revsToRestore) {
			return new int[] { previousRevision };
		}
	},

	/**
	 * Differential. Only the diffs are stored related to the last milestone
	 * revision.
	 */
	DIFFERENTIAL {
		@Override
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= 2;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getPageKey();
			final T returnVal = firstPage.newInstance(recordPageKey,
					firstPage.getPageKind(), firstPage.getRevision(), pageReadTrx);
			if (pages.size() == 2) {
				returnVal.setDirty(true);
			}
			final T latest = pages.get(0);
			T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

			assert latest.getPageKey() == recordPageKey;
			assert fullDump.getPageKey() == recordPageKey;

			for (final Map.Entry<K, V> entry : latest.entrySet()) {
				returnVal.setEntry(entry.getKey(), entry.getValue());
			}

			// Skip full dump if not needed (fulldump equals latest page).
			if (pages.size() == 2) {
				for (final Entry<K, V> entry : fullDump.entrySet()) {
					if (returnVal.getValue(entry.getKey()) == null) {
						returnVal.setEntry(entry.getKey(), entry.getValue());
						if (returnVal.entrySet().size() == Constants.NDP_NODE_COUNT) {
							break;
						}
					}
				}
			}
			return returnVal;
		}

		@Override
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= 2;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getPageKey();
			final int revision = pageReadTrx.getUberPage().getRevision();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), revision, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), revision, pageReadTrx));

			final T latest = firstPage;
			T fullDump = pages.size() == 1 ? firstPage : pages.get(1);
			final boolean isFullDump = revision % revToRestore == 0;

			// Iterate through all nodes of the latest revision.
			for (final Map.Entry<K, V> entry : latest.entrySet()) {
				returnVal.get(0).setEntry(entry.getKey(), entry.getValue());
				returnVal.get(1).setEntry(entry.getKey(), entry.getValue());
			}

			// If not all entries are filled.
			if (latest.entrySet().size() != Constants.NDP_NODE_COUNT) {
				// Iterate through the full dump.
				for (final Map.Entry<K, V> entry : fullDump.entrySet()) {
					if (returnVal.get(0).getValue(entry.getKey()) == null) {
						returnVal.get(0).setEntry(entry.getKey(), entry.getValue());
					}

					if (isFullDump && returnVal.get(1).getValue(entry.getKey()) == null) {
						returnVal.get(1).setEntry(entry.getKey(), entry.getValue());
					}
					
					if (returnVal.get(0).entrySet().size() == Constants.NDP_NODE_COUNT) {
						// Page is filled, thus skip all other entries of the full dump.
						break;
					}
				}
			}

			return new RecordPageContainer<>(returnVal.get(0), returnVal.get(1));
		}

		@Override
		public int[] getRevisionRoots(@Nonnegative int previousRevision,
				@Nonnegative int revsToRestore) {
			final int revisionsToRestore = previousRevision % revsToRestore;
			final int lastFullDump = previousRevision - revisionsToRestore;
			if (lastFullDump == previousRevision) {
				return new int[] { lastFullDump };
			} else {
				return new int[] { previousRevision, lastFullDump };
			}
		}
	},

	/**
	 * Incremental revisioning. Each revision can be reconstructed with the help
	 * of the last full-dump plus the incremental steps between.
	 */
	INCREMENTAL {
		@Override
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> T combineRecordPages(
				final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert pages.size() <= revToRestore;
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getPageKey();
			final T returnVal = firstPage.newInstance(firstPage.getPageKey(),
					firstPage.getPageKind(), firstPage.getRevision(),
					firstPage.getPageReadTrx());
			if (pages.size() > 1) {
				returnVal.setDirty(true);
			}

			boolean filledPage = false;
			for (final T page : pages) {
				assert page.getPageKey() == recordPageKey;
				if (filledPage) {
					break;
				}
				for (final Entry<K, V> entry : page.entrySet()) {
					final K nodeKey = entry.getKey();
					if (returnVal.getValue(nodeKey) == null) {
						returnVal.setEntry(nodeKey, entry.getValue());
						if (returnVal.entrySet().size() == Constants.NDP_NODE_COUNT) {
							filledPage = true;
							break;
						}
					}
				}
			}

			return returnVal;
		}

		@Override
		public <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> RecordPageContainer<T> combineRecordPagesForModification(
				final @Nonnull List<T> pages, final int revToRestore,
				final @Nonnull PageReadTrx pageReadTrx) {
			final T firstPage = pages.get(0);
			final long recordPageKey = firstPage.getPageKey();
			final int revision = pageReadTrx.getUberPage().getRevision();
			final List<T> returnVal = new ArrayList<>(2);
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), revision, pageReadTrx));
			returnVal.add(firstPage.<T> newInstance(recordPageKey,
					firstPage.getPageKind(), revision, pageReadTrx));
			final boolean isFullDump = revision % revToRestore == 0;

			boolean filledPage = false;
			for (final T page : pages) {
				assert page.getPageKey() == recordPageKey;
				if (filledPage) {
					break;
				}

				for (final Entry<K, V> entry : page.entrySet()) {
					// Caching the complete page.
					final K key = entry.getKey();
					assert key != null;
					if (entry != null && returnVal.get(0).getValue(key) == null) {
						returnVal.get(0).setEntry(key, entry.getValue());

						if (returnVal.get(1).getValue(entry.getKey()) == null && isFullDump) {
							returnVal.get(1).setEntry(key, entry.getValue());
						}
						
						if (returnVal.get(0).entrySet().size() == Constants.NDP_NODE_COUNT) {
							filledPage = true;
							break;
						}
					}
				}
			}

			return new RecordPageContainer<>(returnVal.get(0), returnVal.get(1));
		}

		@Override
		public int[] getRevisionRoots(final @Nonnegative int previousRevision,
				final @Nonnegative int revsToRestore) {
			final int revisionsToRestore = previousRevision % revsToRestore;
			final int lastFullDump = previousRevision - revisionsToRestore;
			final int[] retVal = new int[lastFullDump == previousRevision ? 1
					: revisionsToRestore + 1];
			for (int i = previousRevision, j = 0; i >= lastFullDump; j++, i--) {
				retVal[j] = i;
			}
			return retVal;
		}
	};

	/**
	 * Method to reconstruct a complete {@link KeyValuePage} with the help of
	 * partly filled pages plus a revision-delta which determines the necessary
	 * steps back.
	 * 
	 * @param pages
	 *          the base of the complete {@link KeyValuePage}
	 * @param revToRestore
	 *          the revision needed to build up the complete milestone
	 * @return the complete {@link KeyValuePage}
	 */
	public abstract <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> T combineRecordPages(
			final @Nonnull List<T> pages, final @Nonnegative int revToRestore,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Method to reconstruct a complete {@link KeyValuePage} for reading as well
	 * as a {@link KeyValuePage} for serializing with the nodes to write.
	 * 
	 * @param pages
	 *          the base of the complete {@link KeyValuePage}
	 * @param mileStoneRevision
	 *          the revision needed to build up the complete milestone
	 * @return a {@link RecordPageContainer} holding a complete
	 *         {@link KeyValuePage} for reading and one for writing
	 */
	public abstract <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> RecordPageContainer<T> combineRecordPagesForModification(
			final @Nonnull List<T> pages, final @Nonnegative int mileStoneRevision,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Get all revision root page numbers which are needed to restore a
	 * {@link KeyValuePage}.
	 * 
	 * @param previousRevision
	 *          the previous revision
	 * @param revsToRestore
	 *          number of revisions to restore
	 * @return revision root page numbers needed to restore a {@link KeyValuePage}
	 */
	public abstract int[] getRevisionRoots(
			final @Nonnegative int previousRevision,
			final @Nonnegative int revsToRestore);
}
