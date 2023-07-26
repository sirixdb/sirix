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

package io.sirix.node.interfaces;

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.immutable.ImmutableNode;

/**
 * <p>
 * Common interface for all node kinds.
 * </p>
 */
public interface Node extends ImmutableNode {
  @Override
  NodeKind getKind();

  /**
   * Set a new DeweyID (may only be necessary during moves.)
   *
   * @param id new dewey ID
   */
  void setDeweyID(SirixDeweyID id);

  /**
   * Set the type key.
   *
   * @param typeKey the type to set
   */
  void setTypeKey(int typeKey);

  /**
   * Set the actual hash of the structure. The hash of one node should have the entire integrity of
   * the related subtree.
   *
   * @param hash hash for this node
   */
  void setHash(long hash);

  /**
   * Set the parent key.
   *
   * @param nodeKey the parent nodeKey
   */
  void setParentKey(long nodeKey);

  /**
   * Set the previous revision of this record.
   *
   * @param revision the previous revision
   */
  void setPreviousRevision(int revision);

  /**
   * Set the last modified revision of this record.
   *
   * @param revision the last modified revision
   */
  void setLastModifiedRevision(int revision);
}
