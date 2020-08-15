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
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Revision root page holds a reference to the name page as well as the static node page tree.
 */
public final class RevisionRootPage extends AbstractForwardingPage {

  /**
   * Offset of indirect page reference.
   */
  private static final int INDIRECT_REFERENCE_OFFSET = 0;

  /**
   * Offset of path summary page reference.
   */
  private static final int PATH_SUMMARY_REFERENCE_OFFSET = 1;

  /**
   * Offset of name page reference.
   */
  private static final int NAME_REFERENCE_OFFSET = 2;

  /**
   * Offset of CAS page reference.
   */
  private static final int CAS_REFERENCE_OFFSET = 3;

  /**
   * Offset of path page reference.
   */
  private static final int PATH_REFERENCE_OFFSET = 4;

  /**
   * Offset of path page reference.
   */
  private static final int DEWEYID_REFERENCE_OFFSET = 5;

  /**
   * Last allocated node key.
   */
  private long maxNodeKey;

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
   * Current maximum level of indirect pages in the tree.
   */
  private int currentMaxLevelOfIndirectPages;

  /**
   * The user, which committed or is probably committing the revision.
   */
  private User user;

  /**
   * Create revision root page.
   */
  public RevisionRootPage() {
    delegate = new BitmapReferencesPage(5);
    getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(new PathSummaryPage());
    getOrCreateReference(NAME_REFERENCE_OFFSET).setPage(new NamePage());
    getOrCreateReference(CAS_REFERENCE_OFFSET).setPage(new CASPage());
    getOrCreateReference(PATH_REFERENCE_OFFSET).setPage(new PathPage());
    getOrCreateReference(DEWEYID_REFERENCE_OFFSET).setPage(new DeweyIDPage());
    revision = Constants.UBP_ROOT_REVISION_NUMBER;
    maxNodeKey = -1L;
    currentMaxLevelOfIndirectPages = 1;
  }

  /**
   * Read revision root page.
   *
   * @param in input stream
   */
  protected RevisionRootPage(final DataInput in, final SerializationType type) throws IOException {
    delegate = new BitmapReferencesPage(5, in, type);
    revision = in.readInt();
    maxNodeKey = in.readLong();
    revisionTimestamp = in.readLong();
    if (in.readBoolean()) {
      final byte[] commitMessage = new byte[in.readInt()];
      in.readFully(commitMessage);
      this.commitMessage = new String(commitMessage, Constants.DEFAULT_ENCODING);
    }
    currentMaxLevelOfIndirectPages = in.readByte() & 0xFF;

    if (in.readBoolean()) {
      user = new User(in.readUTF(), UUID.fromString(in.readUTF()));
    } else {
      user = null;
    }
  }

  /**
   * Clone revision root page.
   *
   * @param committedRevisionRootPage page to clone
   * @param representRev              revision number to use
   */
  public RevisionRootPage(final RevisionRootPage committedRevisionRootPage, final @Nonnegative int representRev) {
    final Page pageDelegate = committedRevisionRootPage.delegate();
    delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
    revision = representRev;
    user = committedRevisionRootPage.user;
    maxNodeKey = committedRevisionRootPage.maxNodeKey;
    revisionTimestamp = committedRevisionRootPage.revisionTimestamp;
    commitMessage = committedRevisionRootPage.commitMessage;
    currentMaxLevelOfIndirectPages = committedRevisionRootPage.currentMaxLevelOfIndirectPages;
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
   * Get indirect page reference.
   *
   * @return Indirect page reference.
   */
  public PageReference getIndirectPageReference() {
    return getOrCreateReference(INDIRECT_REFERENCE_OFFSET);
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
   * Get timestamp of revision.
   *
   * @return Revision timestamp.
   */
  public long getRevisionTimestamp() {
    return revisionTimestamp;
  }

  /**
   * Get last allocated node key.
   *
   * @return Last allocated node key
   */
  public long getMaxNodeKey() {
    return maxNodeKey;
  }

  /**
   * Increment number of nodes by one while allocating another key.
   */
  public long incrementAndGetMaxNodeKey() {
    return ++maxNodeKey;
  }

  /**
   * Set the maximum node key in the revision.
   *
   * @param maxNodeKey new maximum node key
   */
  public void setMaxNodeKey(final @Nonnegative long maxNodeKey) {
    this.maxNodeKey = maxNodeKey;
  }

  /**
   * Only commit whole subtree if it's the currently added revision.
   * <p>
   * {@inheritDoc}
   */
  @Override
  public void commit(@Nonnull final PageTrx pageWriteTrx) {
    if (revision == pageWriteTrx.getUberPage().getRevision()) {
      super.commit(pageWriteTrx);
    }
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    revisionTimestamp = Instant.now().toEpochMilli();
    delegate.serialize(checkNotNull(out), checkNotNull(type));
    out.writeInt(revision);
    out.writeLong(maxNodeKey);
    out.writeLong(revisionTimestamp);
    out.writeBoolean(commitMessage != null);
    if (commitMessage != null) {
      final byte[] commitMessage = this.commitMessage.getBytes(Constants.DEFAULT_ENCODING);
      out.writeInt(commitMessage.length);
      out.write(commitMessage);
    }

    out.writeByte(currentMaxLevelOfIndirectPages);
    final boolean hasUser = user != null;
    out.writeBoolean(hasUser);
    if (hasUser) {
      out.writeUTF(user.getName());
      out.writeUTF(user.getId().toString());
    }
  }

  public int getCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages() {
    return ++currentMaxLevelOfIndirectPages;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("revisionTimestamp", revisionTimestamp)
                      .add("maxNodeKey", maxNodeKey)
                      .add("delegate", delegate)
                      .add("namePage", getOrCreateReference(NAME_REFERENCE_OFFSET))
                      .add("pathSummaryPage", getOrCreateReference(PATH_SUMMARY_REFERENCE_OFFSET))
                      .add("pathPage", getOrCreateReference(PATH_REFERENCE_OFFSET))
                      .add("CASPage", getOrCreateReference(CAS_REFERENCE_OFFSET))
                      .add("nodePage", getOrCreateReference(INDIRECT_REFERENCE_OFFSET))
                      .toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize node tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param log         the transaction intent log
   */
  public void createNodeTree(final PageReadOnlyTrx pageReadTrx, final TransactionIntentLog log) {
    PageReference reference = getIndirectPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(reference, PageKind.RECORDPAGE, pageReadTrx, log);
      incrementAndGetMaxNodeKey();
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
