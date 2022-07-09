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

package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.FullReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Bitmap based indirect page holds a set of references to build a reference tree.
 */
public final class IndirectPage extends AbstractForwardingPage {

  /**
   * The reference delegate.
   */
  private Page delegate;

  /**
   * Create indirect page.
   */
  public IndirectPage() {
    delegate = new ReferencesPage4();
  }

  /**
   * Read indirect page.
   *
   * @param in input source
   */
  public IndirectPage(final Bytes<ByteBuffer> in, final SerializationType type) {
    delegate = PageUtils.createDelegate(in, type);
  }

  /**
   * Clone indirect page.
   *
   * @param page {@link IndirectPage} to clone
   */
  public IndirectPage(final IndirectPage page) {
    final Page pageDelegate = page.delegate();

    if (pageDelegate instanceof ReferencesPage4) {
      delegate = new ReferencesPage4((ReferencesPage4) pageDelegate);
    } else if (pageDelegate instanceof BitmapReferencesPage) {
      delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
    } else if (pageDelegate instanceof FullReferencesPage) {
      delegate = new FullReferencesPage((FullReferencesPage) pageDelegate);
    }
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    PageReference reference = super.getOrCreateReference(offset);

    if (reference == null) {
      if (delegate instanceof ReferencesPage4) {
        delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
        reference = delegate.getOrCreateReference(offset);
      } else if (delegate instanceof BitmapReferencesPage) {
        delegate = new FullReferencesPage((BitmapReferencesPage) delegate());
        reference = delegate.getOrCreateReference(offset);
      }
    }

    return reference;
  }

  @Override
  public void serialize(Bytes<ByteBuffer> out, SerializationType type) {
    if (delegate instanceof ReferencesPage4) {
      out.writeByte((byte) 0);
    } else if (delegate instanceof BitmapReferencesPage) {
      out.writeByte((byte) 1);
    } else if (delegate instanceof FullReferencesPage) {
      out.writeByte((byte) 2);
    }
    super.serialize(out, type);
  }
}
