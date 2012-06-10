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

package org.treetank.io;

import java.util.Arrays;

/**
 * Delegate to host common data for the {@link IKey} interface.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class KeyDelegate {

  /** All keys. */
  private final long[] mKeys;

  /**
   * Public constructor, just setting the keys.
   * 
   * @param pKey
   *          first key
   * @param pMoreKeys
   *          remaining keys if any
   */
  public KeyDelegate(final long pKey, final long... pMoreKeys) {
    if (pMoreKeys.length == 0) {
      mKeys = new long[1];
      mKeys[0] = pKey;
    } else {
      mKeys = new long[1 + pMoreKeys.length];
      mKeys[0] = pKey;
      System.arraycopy(pMoreKeys, 0, mKeys, 1, pMoreKeys.length);
    }
  }

  /**
   * Getting all keys.
   * 
   * @return the keys
   */
  public long[] getKeys() {
    final long[] returnKeys = new long[mKeys.length];
    System.arraycopy(mKeys, 0, returnKeys, 0, mKeys.length);
    return returnKeys;
  }

  @Override
  public String toString() {
    return Arrays.toString(mKeys);
  }
}
