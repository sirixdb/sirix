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

import com.google.common.collect.ForwardingObject;
import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.PageTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

import org.checkerframework.checker.index.qual.NonNegative;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Forwarding the implementation of all methods in the {@link Page} interface to a delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingPage extends ForwardingObject implements Page {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingPage() {
  }

  @Override
  protected abstract Page delegate();

  @Override
  public void commit(final PageTrx pageWriteTrx) {
    delegate().commit(checkNotNull(pageWriteTrx));
  }

  @Override
  public List<PageReference> getReferences() {
    return delegate().getReferences();
  }

  @Override
  public PageReference getOrCreateReference(final @NonNegative int offset) {
    return delegate().getOrCreateReference(offset);
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    return delegate().setOrCreateReference(offset, pageReference);
  }

  @Override
  public void serialize(final Bytes<ByteBuffer> out, final SerializationType type) {
    delegate().serialize(checkNotNull(out), checkNotNull(type));
  }
}
