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

package io.sirix.page;

import com.google.common.base.MoreObjects;
import io.sirix.access.DatabaseType;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.SirixDeweyID;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

import java.util.Map;

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

  private Map<SirixDeweyID, Long> deweyIDsToNodeKeys;

  /**
   * Create dewey-ID page.
   */
  public DeweyIDPage() {
    delegate = new ReferencesPage4();
    currentMaxLevelOfIndirectPages = 1;
  }

//  /**
//   * Read name page.
//   *
//   * @param in input bytes to read from
//   */
//  DeweyIDPage(final Bytes<?> in, final SerializationType type) {
//    delegate = PageUtils.createDelegate(in, type);
//    maxNodeKey = in.readLong();
//    currentMaxLevelOfIndirectPages = in.readByte() & 0xFF;
//  }

  /**
   *  Constructor to set deserialized data.
   *
   * @param delegate The references page delegate instance.
   * @param maxNodeKey Maximum node key.
   * @param currentMaxLevelOfIndirectPages Current maximum levels of indirect pages in the tree.
   */
  public DeweyIDPage(final Page delegate,final long maxNodeKey, final int currentMaxLevelOfIndirectPages){
    this.delegate = delegate;
    this.maxNodeKey = maxNodeKey;
    this.currentMaxLevelOfIndirectPages=currentMaxLevelOfIndirectPages;
  }

//  @Override
//  public void serialize(final StorageEngineReader pageReadOnlyTrx, final BytesOut<?> out,
//      final SerializationType type) {
//    if (delegate instanceof ReferencesPage4) {
//      out.writeByte((byte) 0);
//    } else if (delegate instanceof BitmapReferencesPage) {
//      out.writeByte((byte) 1);
//    }
//    super.serialize(pageReadOnlyTrx, out, type);
//
//    out.writeLong(maxNodeKey);
//    out.writeByte((byte) currentMaxLevelOfIndirectPages);
//  }

  public int getCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages++;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("currMaxLevelOfIndirectPages", currentMaxLevelOfIndirectPages)
                      .add("maxNodeKey", maxNodeKey)
                      .toString();
  }

  /**
   * Initialize dewey id index tree.
   *
   * @param pageReadTrx {@link StorageEngineReader} instance
   * @param log         the transaction intent log
   */
  public void createIndexTree(final DatabaseType databaseType, final StorageEngineReader pageReadTrx,
      final TransactionIntentLog log) {
    PageReference reference = getIndirectPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
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

  public SirixDeweyID getDeweyIdForNodeKey(final long nodeKey, final StorageEngineReader pageReadOnlyTrx) {
    final DeweyIDNode node = pageReadOnlyTrx.getRecord(nodeKey, IndexType.DEWEYID_TO_RECORDID, 0);
    if (node == null) {
      return null;
    }
    return node.getDeweyID();
  }

  public long getNodeKeyForDeweyId(final SirixDeweyID deweyId, final StorageEngineReader pageReadOnlyTrx) {
    if (deweyIDsToNodeKeys == null) {
//      deweyIDsToNodeKeys =
//          ChronicleMap.of(SirixDeweyID.class, Long.class).name("deweyIDsToNodeKeysMap").entries(maxNodeKey).create();
//      for (long i = 1, l = maxNodeKey; i < l; i += 2) {
//        final long nodeKeyOfNode = i;
//        final var deweyIDNode = pageReadOnlyTrx.getRecord(nodeKeyOfNode, IndexType.DEWEYID_TO_RECORDID, 0);
//
//        if (deweyIDNode != null && deweyIDNode.getKind() != NodeKind.DELETE) {
//          deweyIDsToNodeKeys.put(deweyIDNode.getDeweyID(), nodeKeyOfNode);
//        }
//      }
    }
    return deweyIDsToNodeKeys.get(deweyId);
  }

  public void setDeweyID(final SirixDeweyID deweyId, final StorageEngineWriter pageTrx) {
    final long nodeKey = maxNodeKey;
    final DeweyIDNode node = new DeweyIDNode(maxNodeKey++, deweyId);
    pageTrx.createRecord(node, IndexType.DEWEYID_TO_RECORDID, 0);
    deweyIDsToNodeKeys.put(deweyId, nodeKey);
  }
}
