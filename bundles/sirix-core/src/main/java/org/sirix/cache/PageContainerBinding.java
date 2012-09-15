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

package org.sirix.cache;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import javax.annotation.Nullable;

import org.sirix.page.NodePage;
import org.sirix.page.PagePersistenter;

/**
 * Binding for {@link PageContainer} reference.
 */
public class PageContainerBinding extends TupleBinding<PageContainer> {

  @Override
  public PageContainer entryToObject(final @Nullable TupleInput pInput) {
    if (pInput == null) {
      return PageContainer.EMPTY_INSTANCE;
    }
    final ByteArrayDataInput source =
      ByteStreams.newDataInput(pInput.getBufferBytes());
    final NodePage current = (NodePage) PagePersistenter.deserializePage(source);
    final NodePage modified = (NodePage) PagePersistenter.deserializePage(source);
    final PageContainer container = new PageContainer(current, modified);
    return container;
  }

  @Override
  public void objectToEntry(final @Nullable PageContainer pPageContainer,
    final @Nullable TupleOutput pOutput) {
    if (pPageContainer != null && pOutput != null) {
      pPageContainer.serialize(pOutput);
    }
  }
}
