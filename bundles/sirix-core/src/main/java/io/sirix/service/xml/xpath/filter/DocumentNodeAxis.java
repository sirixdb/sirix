/**
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

package io.sirix.service.xml.xpath.filter;

import io.sirix.axis.AbstractAxis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.settings.Fixed;

/**
 * <p>
 * Iterate to document node starting at a given node.
 * </p>
 */
public class DocumentNodeAxis extends AbstractAxis {

  /** Track number of calls of next. */
  private boolean mFirst;

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx exclusive (immutable) transaction to iterate with
   */
  public DocumentNodeAxis(final XmlNodeReadOnlyTrx pRtx) {
    super(pRtx);
  }

  @Override
  public final void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mFirst = true;
  }

  @Override
  protected long nextKey() {
    if (mFirst) {
      mFirst = false;
      return Fixed.DOCUMENT_NODE_KEY.getStandardProperty();
    }

    return done();
  }
}
