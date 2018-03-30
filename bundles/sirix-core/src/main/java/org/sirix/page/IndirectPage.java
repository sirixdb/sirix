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

package org.sirix.page;

import java.io.DataInput;
import java.io.IOException;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

/**
 * <h1>IndirectPage</h1>
 *
 * <p>
 * Indirect page holds a set of references to build a reference tree.
 * </p>
 */
public final class IndirectPage extends AbstractForwardingPage {

  /** {@link PageDelegate} reference. */
  private final PageDelegate mDelegate;

  /**
   * Create indirect page.
   *
   */
  public IndirectPage() {
    mDelegate = new PageDelegate(Constants.INP_REFERENCE_COUNT);
  }

  /**
   * Read indirect page.
   *
   * @param in input source
   */
  public IndirectPage(final DataInput in, final SerializationType type) throws IOException {
    mDelegate = new PageDelegate(Constants.INP_REFERENCE_COUNT, in, type);
  }

  /**
   * Clone indirect page.
   *
   * @param page {@link IndirectPage} to clone
   */
  public IndirectPage(final IndirectPage page) {
    mDelegate = new PageDelegate(page, page.mDelegate.getBitmap());
  }

  @Override
  protected Page delegate() {
    return mDelegate;
  }
}
