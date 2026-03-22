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

package io.sirix.node;

import io.sirix.api.ItemList;
import io.sirix.node.interfaces.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link ItemList} for storing XDM items (atomic values or nodes).
 *
 * <p>Items are stored with negative keys to distinguish them from regular nodes (which use
 * positive keys). The key is computed as {@code -(index + 2)} to avoid collision with
 * {@code NULL_NODE_KEY = -1}.</p>
 *
 * @param <T> the node type
 */
public final class DefaultItemList<T extends Node> implements ItemList<T> {

  /** Internal storage of items. */
  private final List<T> items;

  /** Constructor. Initializes the list. */
  public DefaultItemList() {
    items = new ArrayList<>();
  }

  @Override
  public int addItem(final T item) {
    final int key = items.size();
    item.setNodeKey(key);
    final int itemKey = (key + 2) * (-1);
    item.setNodeKey(itemKey);
    items.add(item);
    return itemKey;
  }

  @Override
  public T getItem(final long key) {
    assert key <= Integer.MAX_VALUE;

    int index = (int) key;
    if (index < 0) {
      index = index * (-1);
    }
    index = index - 2;

    if (index >= 0 && index < items.size()) {
      return items.get(index);
    }
    return null;
  }

  @Override
  public int size() {
    return items.size();
  }

  @Override
  public String toString() {
    return "DefaultItemList: " + items;
  }
}
