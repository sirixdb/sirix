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

package org.sirix.gui.view.model.interfaces;

import java.beans.PropertyChangeListener;
import java.util.Iterator;
import java.util.List;

import org.sirix.gui.ReadDB;
import org.sirix.gui.view.IVisualItem;
import org.sirix.gui.view.sunburst.SunburstView;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.service.xml.shredder.EInsert;

/**
 * Interface which models of the {@link SunburstView} have to implement.
 * 
 * All methods should throw {@code NullPointerException}s in case of null values for reference parameters are
 * passed and {@code IllegalArgumentException} in case of any parameter which is invalid.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @param <S>
 *          type of container
 * @param <T>
 *          type of items
 * 
 */
public interface IModel<S, T extends IVisualItem> extends Iterable<T>, Iterator<T>, PropertyChangeListener {
  /**
   * Get the item reference which implements {@link IVisualItem} at the specified index in a datastructure.
   * 
   * @param paramIndex
   *          the index
   * @return the {@link SunburstItem} at the specified index
   * @throws IndexOutOfBoundsException
   *           if {@code index > mItems.size() - 1 or < 0}
   */
  T getItem(int paramIndex) throws IndexOutOfBoundsException;

  /**
   * Traverse the tree and create a {@link List} of {@link SunburstItem}s.
   * 
   * @param paramContainer
   *          {@link IContainer} implementation with options
   */
  void traverseTree(IContainer<S> paramContainer);

  /** Undo operation. */
  void undo();

  /**
   * Update root of the tree with the node currently clicked.
   * 
   * @param paramContainer
   *          {@link IContainer} reference with options
   */
  void update(final IContainer<S> paramContainer);

  /**
   * XPath evaluation.
   * 
   * @param paramXPathExpression
   *          XPath expression to evaluate.
   */
  void evaluateXPath(String paramXPathExpression);

  /**
   * Spefify how to insert an XML fragment.
   * 
   * @param paramInsert
   *          determines how to insert an XMl fragment
   */
  void setInsert(EInsert paramInsert);

  /**
   * Update {@link ReadDB} instance.
   * 
   * @param paramDB
   *          new {@link ReadDB} instance
   * @param paramContainer
   *          {@link IContainer} instance
   */
  void updateDb(ReadDB paramDB, IContainer<S> paramContainer);

  /**
   * Add a {@link PropertyChangeListener}.
   * 
   * @param paramListener
   *          the listener to add
   */
  void addPropertyChangeListener(PropertyChangeListener paramListener);

  /**
   * Remove a {@link PropertyChangeListener}.
   * 
   * @param paramListener
   *          the listener to remove
   */
  void removePropertyChangeListener(PropertyChangeListener paramListener);

  /**
   * Fire a property change.
   * 
   * @param paramPropertyName
   *          name of the property
   * @param paramOldValue
   *          old value
   * @param paramNewValue
   *          new value
   */
  void firePropertyChange(String paramPropertyName, Object paramOldValue, Object paramNewValue);

  /**
   * Get the database handle.
   * 
   * @return {@link ReadDB} reference
   */
  ReadDB getDb();

  /**
   * Set a list of new items.
   */
  void setItems(List<T> pItems);

  /**
   * Set minimum and maximum text length as well as descendant count.
   */
  void setMinMax();

  /**
   * Get maximum depth.
   * 
   * @return maximum depth
   */
  int getDepthMax();

  /**
   * Get the size of the item datastructure.
   * 
   * @return items size
   */
  int getItemsSize();

  /**
   * Set the depth max of the outer ring.
   * 
   * @param pDepthMax
   *          the new maximum depth
   */
  void setNewDepthMax(int pDepthMax);

  /**
   * Set the depth max of the inner ring.
   * 
   * @param pDepthMax
   *          the new maximum depth
   */
  void setOldDepthMax(int pOldDepthMax);

  /**
   * Get a sublist of items.
   * 
   * @param pFromIndex
   *          the index to start with (inclusive)
   * @param pToIndex
   *          the index to end (exlusive)
   * @return sublist of items
   */
  List<T> subList(int pFromIndex, int pToIndex);
}
