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

package org.sirix.io.file;

import org.sirix.io.IKey;
import org.sirix.io.KeyDelegate;

/**
 * FileKey, storing the offset and the length. The key is used for the mapping
 * between PageReerence and Page.
 * 
 * @author Sebastian Graf, University of Konstnz
 * 
 */
public final class FileKey implements IKey {

  /** Delegate for the key. */
  private final KeyDelegate mKey;

  /**
   * Constructor for direct data.
   * 
   * @param paramOffset
   *          Offset of data
   * @param paramLength
   *          Length of data
   */
  public FileKey(final long paramOffset, final long paramLength) {
    mKey = new KeyDelegate(paramOffset, paramLength);
  }

  /**
   * Getting the length of the file fragment.
   * 
   * @return the length of the file fragment
   */
  public int getLength() {
    return (int)getKeys()[1];
  }

  /**
   * Getting the offset of the file fragment.
   * 
   * @return the offset
   */
  public long getOffset() {
    return getKeys()[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getIdentifier() {
    return getKeys()[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long[] getKeys() {
    return mKey.getKeys();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return mKey.toString();
  }

}
