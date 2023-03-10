/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.page;

import com.google.common.base.MoreObjects;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.DatabaseType;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Revision root page holds a reference to the name page as well as the static node page tree.
 */
public final class RevisionRootPage extends AbstractForwardingPage {

  /**
   * Offset of indirect page reference.
   */
  private static final int INDIRECT_DOCUMENT_INDEX_REFERENCE_OFFSET = 0;

  /**
   * Offset of indirect page reference.
   */
  private static final int INDIRECT_CHANGED_NODES_INDEX_REFERENCE_OFFSET = 1;

  /**
   * Offset of node to revisions page reference.
   */
  private static final int INDIRECT_RECORD_TO_REVISIONS_INDEX_REFERENCE_OFFSET = 2;

  /**
   * Offset of path summary page reference.
   */
  private static final int PATH_SUMMARY_REFERENCE_OFFSET = 3;

  /**
   * Offset of name page reference.
   */
  private static final int NAME_REFERENCE_OFFSET = 4;

  /**
   * Offset of CAS page reference.
   */
  private static final int CAS_REFERENCE_OFFSET = 5;

  /**
   * Offset of path page reference.
   */
  private static final int PATH_REFERENCE_OFFSET = 6;

  /**
   * Offset of dewey-ID page reference.
   */
  private static final int DEWEYID_REFERENCE_OFFSET = 7;

  /**
   * Last allocated node key.
   */
  private long maxNodeKeyInDocumentIndex;

  /**
   * Last allocated node key.
   */
  private long maxNodeKeyInChangedNodesIndex;

  /**
   * Last allocated node key.
   */
  private long maxNodeKeyInRecordToRevisionsIndex;

  /**
   * Timestamp of revision.
   */
  private long revisionTimestamp;

  /**
   * The references page instance.
   */
  private final Page delegate;

  /**
   * Revision number.
   */
  private final int revision;

  /**
   * Optional commit message.
   */
  private String commitMessage;

  /**
   * Current maximum level of indirect pages in the document index tree.
   */
  private int currentMaxLevelOfDocumentIndexIndirectPages;

  /**
   * Current maximum level of indirect pages in the changed nodes index tree.
   */
  private int currentMaxLevelOfChangedNodesIndirectPages;

  /**
   * Current maximum level of indirect pages in the record to revisions index tree.
   */
  private int currentMaxLevelOfRecordToRevisionsIndirectPages;

  /**
   * The user, which committed or is probably committing the revision.
   */
  private User user;

  private Instant commitTimestamp;

  /**
   * Create revision root page.
   */
  public RevisionRootPage() {
    delegate = new BitmapReferencesPage(8);
    getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(new PathSummaryPage());
    getOrCreateReference(NAME_REFERENCE_OFFSET).setPage(new NamePage());
    getOrCreateReference(CAS_REFERENCE_OFFSET).setPage(new CASPage());
    getOrCreateReference(PATH_REFERENCE_OFFSET).setPage(new PathPage());
    getOrCreateReference(DEWEYID_REFERENCE_OFFSET).setPage(new DeweyIDPage());
    revision = Constants.UBP_ROOT_REVISION_NUMBER;
    maxNodeKeyInDocumentIndex = -1L;
    maxNodeKeyInChangedNodesIndex = -1L;
    maxNodeKeyInRecordToRevisionsIndex = -1L;
    currentMaxLevelOfDocumentIndexIndirectPages = 1;
    currentMaxLevelOfChangedNodesIndirectPages = 1;
    currentMaxLevelOfRecordToRevisionsIndirectPages = 1;
  }

  /**
   * Constructor to deserialized the page.
   *
   * @param delegate                                        Page
   * @param revision                                        int revision
   * @param maxNodeKeyInDocumentIndex                       Last allocated node key.
   * @param maxNodeKeyInChangedNodesIndex                   Last allocated node key.
   * @param maxNodeKeyInRecordToRevisionsIndex              Last allocated node key.
   * @param revisionTimestamp                               Timestamp of revision.
   * @param commitMessage                                   Optional commit message.
   * @param currentMaxLevelOfDocumentIndexIndirectPages     Current maximum level of indirect pages in the document index tree.
   * @param currentMaxLevelOfChangedNodesIndirectPages      Current maximum level of indirect pages in the document index tree.
   * @param currentMaxLevelOfRecordToRevisionsIndirectPages Current maximum level of indirect pages in the document index tree.
   * @param user                                            which committed or is probably committing the revision
   */
  RevisionRootPage(final Page delegate, final int revision, final long maxNodeKeyInDocumentIndex,
      final long maxNodeKeyInChangedNodesIndex, final long maxNodeKeyInRecordToRevisionsIndex,
      final long revisionTimestamp, final String commitMessage, final int currentMaxLevelOfDocumentIndexIndirectPages,
      final int currentMaxLevelOfChangedNodesIndirectPages, final int currentMaxLevelOfRecordToRevisionsIndirectPages,
      final User user) {
    this.delegate = delegate;
    this.revision = revision;
    this.maxNodeKeyInDocumentIndex = maxNodeKeyInDocumentIndex;
    this.maxNodeKeyInChangedNodesIndex = maxNodeKeyInChangedNodesIndex;
    this.maxNodeKeyInRecordToRevisionsIndex = maxNodeKeyInRecordToRevisionsIndex;
    this.revisionTimestamp = revisionTimestamp;
    this.commitMessage = commitMessage;
    this.currentMaxLevelOfDocumentIndexIndirectPages = currentMaxLevelOfDocumentIndexIndirectPages;
    this.currentMaxLevelOfChangedNodesIndirectPages = currentMaxLevelOfChangedNodesIndirectPages;
    this.currentMaxLevelOfRecordToRevisionsIndirectPages = currentMaxLevelOfRecordToRevisionsIndirectPages;
    this.user = user;
  }

  /**
   * Clone revision root page.
   *
   * @param committedRevisionRootPage page to clone
   * @param representRev              revision number to use
   */
  public RevisionRootPage(final RevisionRootPage committedRevisionRootPage, final @NonNegative int representRev) {
    final Page pageDelegate = committedRevisionRootPage.delegate();
    delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
    revision = representRev;
    user = committedRevisionRootPage.user;
    maxNodeKeyInDocumentIndex = committedRevisionRootPage.maxNodeKeyInDocumentIndex;
    maxNodeKeyInChangedNodesIndex = committedRevisionRootPage.maxNodeKeyInChangedNodesIndex;
    maxNodeKeyInRecordToRevisionsIndex = committedRevisionRootPage.maxNodeKeyInRecordToRevisionsIndex;
    revisionTimestamp = committedRevisionRootPage.revisionTimestamp;
    commitMessage = committedRevisionRootPage.commitMessage;
    currentMaxLevelOfDocumentIndexIndirectPages = committedRevisionRootPage.currentMaxLevelOfDocumentIndexIndirectPages;
    currentMaxLevelOfChangedNodesIndirectPages = committedRevisionRootPage.currentMaxLevelOfChangedNodesIndirectPages;
    currentMaxLevelOfRecordToRevisionsIndirectPages =
        committedRevisionRootPage.currentMaxLevelOfRecordToRevisionsIndirectPages;
  }

  /**
   * Get path summary page reference.
   *
   * @return path summary page reference
   */
  public PageReference getPathSummaryPageReference() {
    return getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET);
  }

  /**
   * Get CAS page reference.
   *
   * @return CAS page reference
   */
  public PageReference getCASPageReference() {
    return getOrCreateReference(CAS_REFERENCE_OFFSET);
  }

  /**
   * Get name page reference.
   *
   * @return name page reference
   */
  public PageReference getNamePageReference() {
    return getOrCreateReference(NAME_REFERENCE_OFFSET);
  }

  /**
   * Get path page reference.
   *
   * @return path page reference
   */
  public PageReference getPathPageReference() {
    return getOrCreateReference(PATH_REFERENCE_OFFSET);
  }

  /**
   * Get indirect page reference of document index.
   *
   * @return Indirect page reference of document index.
   */
  public PageReference getIndirectDocumentIndexPageReference() {
    return getOrCreateReference(INDIRECT_DOCUMENT_INDEX_REFERENCE_OFFSET);
  }

  /**
   * Get indirect page reference of changed nodes index.
   *
   * @return Indirect page reference of changed nodes index.
   */
  public PageReference getIndirectChangedNodesIndexPageReference() {
    return getOrCreateReference(INDIRECT_CHANGED_NODES_INDEX_REFERENCE_OFFSET);
  }

  /**
   * Get indirect page reference.
   *
   * @return Indirect changed nodes index page reference.
   */
  public PageReference getIndirectRecordToRevisionsIndexPageReference() {
    return getOrCreateReference(INDIRECT_RECORD_TO_REVISIONS_INDEX_REFERENCE_OFFSET);
  }

  /**
   * Get dewey ID page reference.
   *
   * @return dewey ID page reference.
   */
  public PageReference getDeweyIdPageReference() {
    return getOrCreateReference(DEWEYID_REFERENCE_OFFSET);
  }

  /**
   * Get commitMessage when is serialized page RevisionRootPage
   *
   * @return String commitMessage
   */
  public String getCommitMessage() {
    return commitMessage;
  }

  /**
   * Get timestamp of revision.
   *
   * @return Revision timestamp.
   */
  public long getRevisionTimestamp() {
    return revisionTimestamp;
  }

  /**
   * Get last allocated node key in document index.
   *
   * @return Last allocated node key in document index
   */
  public long getMaxNodeKeyInDocumentIndex() {
    return maxNodeKeyInDocumentIndex;
  }

  /**
   * Increment number of nodes by one while allocating another key in document index.
   */
  public long incrementAndGetMaxNodeKeyInDocumentIndex() {
    return ++maxNodeKeyInDocumentIndex;
  }

  /**
   * Set the maximum node key in the revision in the document index.
   *
   * @param maxNodeKeyInDocumentIndex new maximum node key
   */
  public void setMaxNodeKeyInDocumentIndex(final @NonNegative long maxNodeKeyInDocumentIndex) {
    this.maxNodeKeyInDocumentIndex = maxNodeKeyInDocumentIndex;
  }

  /**
   * Get last allocated node key in changed nodes index.
   *
   * @return Last allocated node key in changed nodes index
   */
  public long getMaxNodeKeyInChangedNodesIndex() {
    return maxNodeKeyInChangedNodesIndex;
  }

  /**
   * Increment number of nodes by one while allocating another key in changed nodes index.
   */
  public long incrementAndGetMaxNodeKeyInChangedNodesIndex() {
    return ++maxNodeKeyInChangedNodesIndex;
  }

  /**
   * Set the maximum node key in the revision in the changed nodes index.
   *
   * @param maxNodeKeyInChangedNodesIndex new maximum node key
   */
  public void setMaxNodeKeyInInChangedNodesIndex(final @NonNegative long maxNodeKeyInChangedNodesIndex) {
    this.maxNodeKeyInChangedNodesIndex = maxNodeKeyInChangedNodesIndex;
  }

  /**
   * Get last allocated node key in record to revisions index.
   *
   * @return Last allocated node key in record to revisions index
   */
  public long getMaxNodeKeyInRecordToRevisionsIndex() {
    return maxNodeKeyInRecordToRevisionsIndex;
  }

  /**
   * Increment number of nodes by one while allocating another key in record to revisions index.
   */
  public long incrementAndGetMaxNodeKeyInRecordToRevisionsIndex() {
    return ++maxNodeKeyInRecordToRevisionsIndex;
  }

  /**
   * Set the maximum node key in the revision in the record to revisions index.
   *
   * @param maxNodeKeyInRecordToRevisionsIndex new maximum node key
   */
  public void setMaxNodeKeyInRecordToRevisionsIndex(final @NonNegative long maxNodeKeyInRecordToRevisionsIndex) {
    this.maxNodeKeyInRecordToRevisionsIndex = maxNodeKeyInRecordToRevisionsIndex;
  }

  /**
   * Only commit whole subtree if it's the currently added revision.
   * <p>
   * {@inheritDoc}
   */
  @Override
  public void commit(@NonNull final PageTrx pageWriteTrx) {
    if (revision == pageWriteTrx.getUberPage().getRevisionNumber()) {
      super.commit(pageWriteTrx);
    }
  }

  /***
   * This value is used to serialize Page
   * @return commitTimestamp
   */

  public Instant getCommitTimestamp() {
    return commitTimestamp;
  }

  public void setCommitTimestamp(final Instant revisionTimestamp) {
    requireNonNull(revisionTimestamp);
    final long revisionTimestampToSet = revisionTimestamp.toEpochMilli();
    if (this.revisionTimestamp > revisionTimestampToSet || revisionTimestamp.isAfter(Instant.now())) {
      throw new IllegalStateException(
          "Revision timestamp must be bigger than previous revision timestamp, but not bigger than current time.");
    }
    this.commitTimestamp = revisionTimestamp;
  }

  public void setRevisionTimestamp(final long revisionTimestamp) {
    this.revisionTimestamp = revisionTimestamp;
  }

  public int getCurrentMaxLevelOfDocumentIndexIndirectPages() {
    return currentMaxLevelOfDocumentIndexIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfDocumentIndexIndirectPages() {
    return ++currentMaxLevelOfDocumentIndexIndirectPages;
  }

  public int getCurrentMaxLevelOfChangedNodesIndexIndirectPages() {
    return currentMaxLevelOfChangedNodesIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfChangedNodesIndexIndirectPages() {
    return ++currentMaxLevelOfChangedNodesIndirectPages;
  }

  public int getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages() {
    return currentMaxLevelOfRecordToRevisionsIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages() {
    return ++currentMaxLevelOfRecordToRevisionsIndirectPages;
  }

  @NonNull
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("revisionTimestamp", revisionTimestamp)
                      .add("maxNodeKey", maxNodeKeyInDocumentIndex)
                      .add("delegate", delegate)
                      .add("nodePage", getOrCreateReference(INDIRECT_DOCUMENT_INDEX_REFERENCE_OFFSET))
                      .add("namePage", getOrCreateReference(NAME_REFERENCE_OFFSET))
                      .add("pathSummaryPage", getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET))
                      .add("pathPage", getOrCreateReference(PATH_REFERENCE_OFFSET))
                      .add("CASPage", getOrCreateReference(CAS_REFERENCE_OFFSET))
                      .add("deweyIDPage", getOrCreateReference(DEWEYID_REFERENCE_OFFSET))
                      .toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize document index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param log          the transaction intent log
   */
  public void createDocumentIndexTree(final DatabaseType databaseType, final PageReadOnlyTrx pageReadTrx,
      final TransactionIntentLog log) {
    PageReference reference = getIndirectDocumentIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.DOCUMENT, pageReadTrx, log);
      incrementAndGetMaxNodeKeyInDocumentIndex();
    }
  }

  /**
   * Initialize record to revisions index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param log          the transaction intent log
   */
  public void createChangedNodesIndexTree(final DatabaseType databaseType, final PageReadOnlyTrx pageReadTrx,
      final TransactionIntentLog log) {
    PageReference reference = getIndirectChangedNodesIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.CHANGED_NODES, pageReadTrx, log);
      incrementAndGetMaxNodeKeyInChangedNodesIndex();
    }
  }

  /**
   * Initialize changed records index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param log          the transaction intent log
   */
  public void createRecordToRevisionsIndexTree(final DatabaseType databaseType, final PageReadOnlyTrx pageReadTrx,
      final TransactionIntentLog log) {
    PageReference reference = getIndirectRecordToRevisionsIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.RECORD_TO_REVISIONS, pageReadTrx, log);
      incrementAndGetMaxNodeKeyInRecordToRevisionsIndex();
    }
  }

  /**
   * Get the revision number.
   *
   * @return revision number
   */
  public int getRevision() {
    return revision;
  }

  public void setCommitMessage(final String commitMessage) {
    this.commitMessage = commitMessage;
  }

  public void setUser(final User user) {
    this.user = user;
  }

  public CommitCredentials getCommitCredentials() {
    return new CommitCredentials(user, commitMessage);
  }

  public Optional<User> getUser() {
    return Optional.ofNullable(user);
  }
}
