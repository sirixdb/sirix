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

package org.sirix.io.berkeley;

import org.sirix.io.IKey;
import org.sirix.io.KeyDelegate;

/**
 * Key for reference the data in the berkeley-db. The key is also the
 * soft-reference of the pages regarding the PageReference.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class BerkeleyKey implements IKey {

  /** Delegate for the key. */
  private final KeyDelegate mKey;

  /**
   * Public constructor.
   * 
   * @param pKey
   *          key coming from the application
   */
  public BerkeleyKey(final long pKey) {
    mKey = new KeyDelegate(pKey);
  }

  /**
   * Static method to get the key for the {@code StorageProperties}.
   * 
   * @return the key for the
   */
  public static final BerkeleyKey getPropsKey() {
    return new BerkeleyKey(-3);
  }

  /**
   * Static method to get the key about the information about the last
   * nodepagekey given.
   * 
   * @return the key for the last nodepage key
   */
  public static final BerkeleyKey getDataInfoKey() {
    return new BerkeleyKey(-2);
  }

  /**
   * Static method to get the key about the first reference of the Nodepages.
   * 
   * @return the key for the first nodepage
   */
  public static final BerkeleyKey getFirstRevKey() {
    return new BerkeleyKey(-1);
  }

  @Override
  public final long getIdentifier() {
    return getKeys()[0];
  }

  @Override
  public long[] getKeys() {
    return mKey.getKeys();
  }

  @Override
  public String toString() {
    return mKey.toString();
  }
}
