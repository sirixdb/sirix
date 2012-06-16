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

package org.sirix.page;

import org.sirix.page.interfaces.IPage;
import org.sirix.utils.IConstants;

/**
 * <h1>PageReference</h1>
 * 
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing to the start byte in a file,
 * including the length in bytes, and the checksum of the serialized page. Or it might be an immediate
 * reference to an in-memory instance of the deserialized page.
 * </p>
 */
public final class PageReference {

  /** In-memory deserialized page instance. */
  private IPage mPage;

  /** Corresponding mKey of the related node page. */
  private long mNodePageKey = -1;
  
  /** Key in persistent storage. */
  private long mKey = IConstants.NULL_ID;;

  /**
   * Default constructor setting up an uninitialized page reference.
   */
  public PageReference() {
  }

  /**
   * Get in-memory instance of deserialized page.
   * 
   * @return in-memory instance of deserialized page
   */
  public IPage getPage() {
    return mPage;
  }

  /**
   * Set in-memory instance of deserialized page.
   * 
   * @param pPage
   *          deserialized page
   */
  public void setPage(final IPage pPage) {
    mPage = pPage;
  }

  /**
   * Get start byte offset in file.
   * 
   * @return start offset in file
   */
  public long getKey() {
    return mKey;
  }

  /**
   * Set start byte offset in file.
   * 
   * @param pKey
   *          key of this reference set by the persistent storage
   */
  public void setKey(final long pKey) {
    mKey = pKey;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(super.toString());
      builder.append(": key=");
      builder.append(mKey);
    builder.append(", page=(");
    builder.append(mPage);
    builder.append(")");
    return builder.toString();
  }

  /**
   * Set nodepage key.
   * 
   * @param pNodePageKey
   *          the nodePageKey to set
   */
  public void setNodePageKey(final long pNodePageKey) {
    mNodePageKey = pNodePageKey;
  }

  /**
   * Get nodepage key.
   * 
   * @return the nodePageKey
   */
  public long getNodePageKey() {
    return mNodePageKey;
  }

}
