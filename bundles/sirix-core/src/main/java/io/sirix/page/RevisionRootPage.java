/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.page;

import io.sirix.utils.CommitTimestamps;
import io.sirix.utils.ToStringHelper;

import io.sirix.access.DatabaseType;
import io.sirix.access.User;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

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
   * Offset of vector page reference.
   */
  private static final int VECTOR_REFERENCE_OFFSET = 8;

  /**
   * Offset of projection index page reference.
   */
  private static final int PROJECTION_REFERENCE_OFFSET = 9;

  /**
   * Offset of valid-time interval index page reference.
   */
  private static final int VALIDTIME_REFERENCE_OFFSET = 10;

  private static final int TEMPORAL_AGGREGATE_REFERENCE_OFFSET = 11;

  private static final int CHANGE_LOG_REFERENCE_OFFSET = 12;

  /**
   * Number of sibling references held in this revision root page (the highest reference offset plus
   * one). Kept in sync with the {@code *_REFERENCE_OFFSET} constants above and with the
   * {@code BitmapReferencesPage} deserialization in {@code PageKind.REVISIONROOTPAGE}.
   */
  public static final int REVISION_ROOT_PAGE_REFERENCE_COUNT = 13;

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
   * Effective commit timestamp of the revision, as the epoch microseconds of VER-201. Before the
   * revision's own timestamp is assigned, a page cloned for a new revision carries the predecessor's
   * effective value, which is the strictly-increasing predecessor of TXN-310.
   */
  private long revisionTimestampMicros;

  /**
   * Whether {@link #revisionTimestampMicros} already holds this revision's own effective commit
   * timestamp. Cloning for a new revision copies the predecessor's value but not this flag, so the
   * first {@link #ensureCommitTimestampMicros()} of a commit assigns and every later call repeats
   * that one value.
   */
  private boolean commitTimestampAssigned;

  /**
   * Whether {@link #revisionTimestampMicros} holds a predecessor timestamp this revision must stay
   * strictly above under TXN-310. The bootstrap revision has no predecessor, so its commit timestamp
   * is the supplied or clock value itself.
   */
  private boolean hasPredecessorTimestamp;

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
    delegate = new BitmapReferencesPage(REVISION_ROOT_PAGE_REFERENCE_COUNT);
    getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(new PathSummaryPage());
    getOrCreateReference(NAME_REFERENCE_OFFSET).setPage(new NamePage());
    getOrCreateReference(CAS_REFERENCE_OFFSET).setPage(new CASPage());
    getOrCreateReference(PATH_REFERENCE_OFFSET).setPage(new PathPage());
    getOrCreateReference(DEWEYID_REFERENCE_OFFSET).setPage(new DeweyIDPage());
    getOrCreateReference(VECTOR_REFERENCE_OFFSET).setPage(new VectorPage());
    getOrCreateReference(PROJECTION_REFERENCE_OFFSET).setPage(new ProjectionIndexPage());
    getOrCreateReference(VALIDTIME_REFERENCE_OFFSET).setPage(new ValidTimeIndexPage());
    revision = Constants.UBP_ROOT_REVISION_NUMBER;
    maxNodeKeyInDocumentIndex = -1L;
    maxNodeKeyInChangedNodesIndex = -1L;
    maxNodeKeyInRecordToRevisionsIndex = -1L;
    currentMaxLevelOfDocumentIndexIndirectPages = 0;
    currentMaxLevelOfChangedNodesIndirectPages = 0;
    currentMaxLevelOfRecordToRevisionsIndirectPages = 0;
  }

  /**
   * Constructor to deserialized the page.
   *
   * @param delegate Page
   * @param revision int revision
   * @param maxNodeKeyInDocumentIndex Last allocated node key.
   * @param maxNodeKeyInChangedNodesIndex Last allocated node key.
   * @param maxNodeKeyInRecordToRevisionsIndex Last allocated node key.
   * @param revisionTimestampMicros Effective commit timestamp of the revision, in epoch microseconds.
   * @param commitMessage Optional commit message.
   * @param currentMaxLevelOfDocumentIndexIndirectPages Current maximum level of indirect pages in the
   *        document index tree.
   * @param currentMaxLevelOfChangedNodesIndirectPages Current maximum level of indirect pages in the
   *        document index tree.
   * @param currentMaxLevelOfRecordToRevisionsIndirectPages Current maximum level of indirect pages in
   *        the document index tree.
   * @param user which committed or is probably committing the revision
   */
  RevisionRootPage(final Page delegate, final int revision, final long maxNodeKeyInDocumentIndex,
      final long maxNodeKeyInChangedNodesIndex, final long maxNodeKeyInRecordToRevisionsIndex,
      final long revisionTimestampMicros, final String commitMessage,
      final int currentMaxLevelOfDocumentIndexIndirectPages,
      final int currentMaxLevelOfChangedNodesIndirectPages, final int currentMaxLevelOfRecordToRevisionsIndirectPages,
      final User user) {
    this.delegate = delegate;
    this.revision = revision;
    this.maxNodeKeyInDocumentIndex = maxNodeKeyInDocumentIndex;
    this.maxNodeKeyInChangedNodesIndex = maxNodeKeyInChangedNodesIndex;
    this.maxNodeKeyInRecordToRevisionsIndex = maxNodeKeyInRecordToRevisionsIndex;
    this.revisionTimestampMicros = revisionTimestampMicros;
    // A deserialized page carries the timestamp the revision committed with.
    this.commitTimestampAssigned = true;
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
   * @param representRev revision number to use
   */
  public RevisionRootPage(final RevisionRootPage committedRevisionRootPage, final int representRev) {
    final Page pageDelegate = committedRevisionRootPage.delegate();
    delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
    revision = representRev;
    user = committedRevisionRootPage.user;
    maxNodeKeyInDocumentIndex = committedRevisionRootPage.maxNodeKeyInDocumentIndex;
    maxNodeKeyInChangedNodesIndex = committedRevisionRootPage.maxNodeKeyInChangedNodesIndex;
    maxNodeKeyInRecordToRevisionsIndex = committedRevisionRootPage.maxNodeKeyInRecordToRevisionsIndex;
    // The clone keeps the source timestamp as its predecessor value; commitTimestampAssigned
    // stays false so this revision assigns its own effective timestamp at commit.
    revisionTimestampMicros = committedRevisionRootPage.revisionTimestampMicros;
    hasPredecessorTimestamp = true;
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
   * Get vector page reference.
   *
   * @return vector page reference.
   */
  public PageReference getVectorPageReference() {
    return getOrCreateReference(VECTOR_REFERENCE_OFFSET);
  }

  /**
   * Get projection index page reference.
   *
   * @return projection index page reference.
   */
  public PageReference getProjectionIndexPageReference() {
    return getOrCreateReference(PROJECTION_REFERENCE_OFFSET);
  }

  /**
   * Get valid-time interval index page reference.
   *
   * @return valid-time interval index page reference.
   */
  public PageReference getValidTimeIndexPageReference() {
    return getOrCreateReference(VALIDTIME_REFERENCE_OFFSET);
  }

  /** Return the revision-owned temporal aggregate container reference. */
  public PageReference getTemporalAggregateIndexPageReference() {
    return getOrCreateReference(TEMPORAL_AGGREGATE_REFERENCE_OFFSET);
  }

  public PageReference getChangeLogIndexPageReference() {
    return getOrCreateReference(CHANGE_LOG_REFERENCE_OFFSET);
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
  public long getRevisionTimestampMicros() {
    return revisionTimestampMicros;
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
  public void setMaxNodeKeyInDocumentIndex(final long maxNodeKeyInDocumentIndex) {
    this.maxNodeKeyInDocumentIndex = maxNodeKeyInDocumentIndex;
  }

  /**
   * Reserves {@code count} consecutive document-index node keys and returns the FIRST reserved key.
   * The parallel bulk importer assigns each build chunk an exact contiguous range up front so workers
   * can encode final record bytes off-thread; the single-writer mint
   * ({@link #incrementAndGetMaxNodeKeyInDocumentIndex()}) and this reservation share the one counter,
   * so the field equals the true high-water mark at all times — the invariant the commit relies on
   * when it serializes this page.
   *
   * @param count how many keys to reserve; must be positive
   * @return the first key of the reserved contiguous range
   */
  public long reserveKeyRangeInDocumentIndex(final long count) {
    if (count <= 0) {
      throw new IllegalArgumentException("count must be positive: " + count);
    }
    final long first = maxNodeKeyInDocumentIndex + 1;
    maxNodeKeyInDocumentIndex += count;
    return first;
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
  public void setMaxNodeKeyInInChangedNodesIndex(final long maxNodeKeyInChangedNodesIndex) {
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
  public void setMaxNodeKeyInRecordToRevisionsIndex(final long maxNodeKeyInRecordToRevisionsIndex) {
    this.maxNodeKeyInRecordToRevisionsIndex = maxNodeKeyInRecordToRevisionsIndex;
  }

  /**
   * Only commit whole subtree if it's the currently added revision.
   * <p>
   * {@inheritDoc}
   */
  @Override
  public void commit(final StorageEngineWriter storageEngineWriter) {
    if (revision == storageEngineWriter.getUberPage().getRevisionNumber()) {
      super.commit(storageEngineWriter);
    }
  }

  /***
   * This value is used to serialize Page
   * 
   * @return commitTimestamp
   */

  public Instant getCommitTimestamp() {
    return commitTimestamp;
  }

  /**
   * Set the commit timestamp supplied by the caller. The value is validated here and resolved
   * against the predecessor by {@link #ensureCommitTimestampMicros()} when the revision commits: a
   * timestamp that ties with or precedes the predecessor is adjusted there under TXN-310, rather
   * than rejected. A timestamp in the future is rejected, and so is one outside the representable
   * epoch-microsecond range of VER-201.
   *
   * @param commitTimestamp the commit timestamp to use
   * @throws NullPointerException if {@code commitTimestamp} is {@code null}
   * @throws IllegalArgumentException if {@code commitTimestamp} is not representable in microseconds
   * @throws IllegalStateException if {@code commitTimestamp} is in the future
   */
  public void setCommitTimestamp(final Instant commitTimestamp) {
    requireNonNull(commitTimestamp);
    // Rejects an unrepresentable instant before it can reach the stored field.
    CommitTimestamps.toEpochMicros(commitTimestamp);
    if (commitTimestamp.isAfter(Instant.now())) {
      throw new IllegalStateException("Revision timestamp must not be bigger than current time.");
    }
    this.commitTimestamp = commitTimestamp;
  }

  /**
   * Assign this revision's effective commit timestamp under TXN-310 and return it, in the epoch
   * microseconds of VER-201. The effective value is
   * {@code max(supplied or wall clock, predecessor + 1 microsecond)}, so commit timestamps strictly
   * increase even when two commits fall inside one millisecond or the wall clock repeats or moves
   * backwards. The first call of a commit assigns; every later call on the same page repeats that
   * one value, so serializing a revision twice yields the same bytes.
   *
   * @return the effective commit timestamp in epoch microseconds
   */
  public long ensureCommitTimestampMicros() {
    if (!commitTimestampAssigned) {
      final long candidate = commitTimestamp == null
          ? CommitTimestamps.nowEpochMicros()
          : CommitTimestamps.toEpochMicros(commitTimestamp);
      // revisionTimestampMicros still holds the predecessor's effective timestamp here. The
      // bootstrap revision has no predecessor and keeps the supplied or clock value as it is.
      revisionTimestampMicros = hasPredecessorTimestamp
          ? Math.max(candidate, revisionTimestampMicros + 1)
          : candidate;
      commitTimestampAssigned = true;
    }
    return revisionTimestampMicros;
  }

  /**
   * Seed the effective commit timestamp of the predecessor this revision must strictly exceed under
   * TXN-310, in the epoch microseconds of VER-201. The predecessor is the last committed revision of
   * the same timeline, which is not the revision a reverting writer represents.
   *
   * @param predecessorTimestampMicros the predecessor's effective commit timestamp
   * @throws IllegalStateException if this revision's own commit timestamp was already assigned
   */
  public void seedPredecessorCommitTimestampMicros(final long predecessorTimestampMicros) {
    if (commitTimestampAssigned) {
      throw new IllegalStateException("Commit timestamp of revision " + revision + " is already assigned.");
    }
    this.revisionTimestampMicros = predecessorTimestampMicros;
    this.hasPredecessorTimestamp = true;
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

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("revisionTimestampMicros", revisionTimestampMicros)
                         .add("maxNodeKey", maxNodeKeyInDocumentIndex)
                         .add("delegate", delegate)
                         .add("nodePage", getOrCreateReference(INDIRECT_DOCUMENT_INDEX_REFERENCE_OFFSET))
                         .add("namePage", getOrCreateReference(NAME_REFERENCE_OFFSET))
                         .add("pathSummaryPage", getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET))
                         .add("pathPage", getOrCreateReference(PATH_REFERENCE_OFFSET))
                         .add("CASPage", getOrCreateReference(CAS_REFERENCE_OFFSET))
                         .add("deweyIDPage", getOrCreateReference(DEWEYID_REFERENCE_OFFSET))
                         .add("vectorPage", getOrCreateReference(VECTOR_REFERENCE_OFFSET))
                         .add("projectionPage", getOrCreateReference(PROJECTION_REFERENCE_OFFSET))
                         .add("validTimePage", getOrCreateReference(VALIDTIME_REFERENCE_OFFSET))
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
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param log the transaction intent log
   */
  public void createDocumentIndexTree(final DatabaseType databaseType, final StorageEngineReader storageEngineReader,
      final TransactionIntentLog log) {
    PageReference reference = getIndirectDocumentIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createKeyedTrie(databaseType, reference, IndexType.DOCUMENT, storageEngineReader, log);
      incrementAndGetMaxNodeKeyInDocumentIndex();
    }
  }

  /**
   * Initialize record to revisions index tree.
   *
   * @param databaseType The type of database.
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param log the transaction intent log
   */
  public void createChangedNodesIndexTree(final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader, final TransactionIntentLog log) {
    PageReference reference = getIndirectChangedNodesIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createKeyedTrie(databaseType, reference, IndexType.CHANGED_NODES, storageEngineReader, log);
      incrementAndGetMaxNodeKeyInChangedNodesIndex();
    }
  }

  /**
   * Initialize changed records index tree.
   *
   * @param databaseType The type of database.
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param log the transaction intent log
   */
  public void createRecordToRevisionsIndexTree(final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader, final TransactionIntentLog log) {
    PageReference reference = getIndirectRecordToRevisionsIndexPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createKeyedTrie(databaseType, reference, IndexType.RECORD_TO_REVISIONS, storageEngineReader, log);
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
