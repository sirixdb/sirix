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

package org.sirix.io.berkeley.binding;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.page.PagePersistenter;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binding for storing {@link PageDelegate} objects within the Berkeley DB.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class PageBinding extends TupleBinding<Page> {

  /** Logger. */
  private static final Logger LOGGER = LoggerFactory
    .getLogger(PageBinding.class);

  /** {@link ByteHandlePipeline} reference. */
  private final ByteHandlePipeline mByteHandler;

  /**
   * Copy constructor.
   * 
   * @param pageBinding
   *          page binding
   */
  public PageBinding(final @Nonnull PageBinding pageBinding) {
    mByteHandler = new ByteHandlePipeline(pageBinding.mByteHandler);
  }

  /**
   * Constructor.
   * 
   * @param byteHandler
   *          byte handler pipleine
   */
  public PageBinding(final @Nonnull ByteHandlePipeline byteHandler) {
    mByteHandler = checkNotNull(byteHandler);
  }

  @Override
  public Page entryToObject(final TupleInput input) {
    byte[] deserialized = new byte[0];
    try {
      deserialized = mByteHandler.deserialize(input.getBufferBytes());
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return PagePersistenter.deserializePage(ByteStreams
      .newDataInput(deserialized));
  }

  @Override
  public void objectToEntry(final Page page, final TupleOutput output) {
    final ByteArrayDataOutput outputData = ByteStreams.newDataOutput();
    PagePersistenter.serializePage(outputData, page);
    try {
      output.write(mByteHandler.serialize(outputData.toByteArray()));
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

}
