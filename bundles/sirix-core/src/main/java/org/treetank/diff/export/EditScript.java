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
package org.treetank.diff.export;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.treetank.diff.DiffTuple;
import org.treetank.diff.DiffFactory.EDiff;

/**
 * Builds an edit script.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class EditScript implements Iterator<DiffTuple>, Iterable<DiffTuple> {

  /** Preserves the order of changes and is used to iterate over all changes. */
  private final List<DiffTuple> mChanges;

  /** To do a lookup; we use node/object identities. */
  private final IdentityHashMap<Long, DiffTuple> mChangeByNode;

  /** Index in the {@link List} of {@link DiffTuple}s. */
  private transient int mIndex;

  /**
   * Constructor.
   */
  public EditScript() {
    mChanges = new ArrayList<DiffTuple>();
    mChangeByNode = new IdentityHashMap<Long, DiffTuple>();
    mIndex = 0;
  }

  /**
   * Calculates the size of the edit script. This can be used to
   * estimate the amicability of an algorithm.
   * 
   * @return number of changes
   */
  public int size() {
    return mChanges.size();
  }

  /**
   * Checks if the edit script is empty.
   * 
   * @return true if empty
   */
  public boolean isEmpty() {
    return mChanges.isEmpty();
  }

  /**
   * Checks if a node has been added(changed).
   * 
   * @param paramKey
   *          key of node
   * @return true if the changes {@link List} already contains the nodeKey, false otherwise
   */
  public boolean containsNode(final long paramKey) {
    if (paramKey < 0) {
      throw new IllegalArgumentException("paramKey may not be < 0!");
    }
    return mChangeByNode.containsKey(paramKey);
  }

  /**
   * Clears the edit script.
   */
  public void clear() {
    mChanges.clear();
    mChangeByNode.clear();
  }

  /**
   * Look up a change for the given nodeKey.
   * 
   * @param paramKey
   *          (not) changed key of node
   * @return the change assigned to the node or null
   */
  public DiffTuple get(final long paramKey) {
    return mChangeByNode.get(paramKey);
  }

  /**
   * Adds a change to the edit script.
   * 
   * @param paramChange
   *          {@link DiffTuple} reference
   * @return the change
   */
  public DiffTuple add(final DiffTuple paramChange) {
    assert paramChange != null;
    final long nodeKey =
      paramChange.getDiff() == EDiff.DELETED ? paramChange.getOldNodeKey() : paramChange.getNewNodeKey();
    if (mChangeByNode.containsKey(nodeKey)) {
      return paramChange;
    }

    mChanges.add(paramChange);
    return mChangeByNode.put(nodeKey, paramChange);
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<DiffTuple> iterator() {
    return mChanges.iterator();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    if (mIndex < mChanges.size() - 1) {
      return true;
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public DiffTuple next() {
    if (mIndex < mChanges.size()) {
      return mChanges.get(mIndex++);
    } else {
      throw new NoSuchElementException("No more elements in the change list!");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported!");
  }

}
