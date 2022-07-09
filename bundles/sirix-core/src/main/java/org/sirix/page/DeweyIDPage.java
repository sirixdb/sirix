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
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.map.ChronicleMap;
import org.sirix.access.DatabaseType;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.node.DeweyIDNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public final class DeweyIDPage extends AbstractForwardingPage {

  /**
   * Offset of reference to index-tree.
   */
  public static final int REFERENCE_OFFSET = 0;

  /**
   * The references page delegate instance.
   */
  private Page delegate;

  /**
   * Maximum node key.
   */
  private long maxNodeKey;

  /**
   * Current maximum levels of indirect pages in the tree.
   */
  private int currentMaxLevelOfIndirectPages;

  private ChronicleMap<SirixDeweyID, Long> deweyIDsToNodeKeys;

  /**
   * Create dewey-ID page.
   */
  public DeweyIDPage() {
    delegate = new ReferencesPage4();
    currentMaxLevelOfIndirectPages = 1;
  }

  /**
   * Read name page.
   *
   * @param in input bytes to read from
   */
  protected DeweyIDPage(final Bytes<ByteBuffer> in, final SerializationType type) {
    delegate = PageUtils.createDelegate(in, type);
    maxNodeKey = in.readLong();
    currentMaxLevelOfIndirectPages = in.readByte() & 0xFF;
  }

  @Override
  public void serialize(final Bytes<ByteBuffer> out, final SerializationType type) {
    if (delegate instanceof ReferencesPage4) {
      out.writeByte((byte) 0);
    } else if (delegate instanceof BitmapReferencesPage) {
      out.writeByte((byte) 1);
    }
    super.serialize(out, type);

    out.writeLong(maxNodeKey);
    out.writeByte((byte) currentMaxLevelOfIndirectPages);
  }

  public int getCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages++;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("currMaxLevelOfIndirectPages", currentMaxLevelOfIndirectPages)
                      .add("maxNodeKey", maxNodeKey)
                      .toString();
  }

  /**
   * Initialize dewey id index tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param log         the transaction intent log
   */
  public void createIndexTree(final DatabaseType databaseType,
                              final PageReadOnlyTrx pageReadTrx,
                              final TransactionIntentLog log) {
    PageReference reference = getIndirectPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(databaseType, reference, IndexType.DEWEYID_TO_RECORDID, pageReadTrx, log);
      incrementAndGetMaxNodeKey();
    }
  }

  /**
   * Get indirect page reference.
   *
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference() {
    return getOrCreateReference(REFERENCE_OFFSET);
  }

  /**
   * Get the maximum node key.
   *
   * @return the maximum node key stored
   */
  public long getMaxNodeKey() {
    return maxNodeKey;
  }

  public long incrementAndGetMaxNodeKey() {
    return ++maxNodeKey;
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }

  public Optional<SirixDeweyID> getDeweyIdForNodeKey(final long nodeKey, final PageReadOnlyTrx pageReadOnlyTrx) {
    final Optional<DeweyIDNode> node = pageReadOnlyTrx.getRecord(nodeKey, IndexType.DEWEYID_TO_RECORDID, 0);
    return node.map(DeweyIDNode::getDeweyID);
  }

  public long getNodeKeyForDeweyId(final SirixDeweyID deweyId, final PageReadOnlyTrx pageReadOnlyTrx) {
    if (deweyIDsToNodeKeys == null) {
      deweyIDsToNodeKeys =
          ChronicleMap.of(SirixDeweyID.class, Long.class).name("deweyIDsToNodeKeysMap").entries(maxNodeKey).create();
      for (long i = 1, l = maxNodeKey; i < l; i += 2) {
        final long nodeKeyOfNode = i;
        final Optional<? extends DataRecord> deweyIDNode =
            pageReadOnlyTrx.getRecord(nodeKeyOfNode, IndexType.DEWEYID_TO_RECORDID, 0);

        if (deweyIDNode.isPresent() && deweyIDNode.get().getKind() != NodeKind.DELETE) {
          deweyIDsToNodeKeys.put(deweyIDNode.get().getDeweyID(), nodeKeyOfNode);
        }
      }
    }
    return deweyIDsToNodeKeys.get(deweyId);
  }

  public void setDeweyID(final SirixDeweyID deweyId, final PageTrx pageTrx) {
    final long nodeKey = maxNodeKey;
    final DeweyIDNode node = new DeweyIDNode(maxNodeKey, deweyId);
    pageTrx.createRecord(maxNodeKey++, node,  IndexType.DEWEYID_TO_RECORDID, 0);
    deweyIDsToNodeKeys.put(deweyId, nodeKey);
  }
}
