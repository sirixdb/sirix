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
package org.sirix.fs;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import javax.annotation.Nonnull;

import com.google.common.base.Optional;
import org.sirix.api.INodeReadTrx;

/**
 * Interface for all visitor implementations to equip the parsing of directories
 * with a thorough XML representation.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <T>
 *          type of the argument to the methods (usually in our case an instance of {@link WriteTransaction}
 * 
 */
@Nonnull
public interface IVisitor<T extends INodeReadTrx> {
  /**
   * Process a directory.
   * 
   * @param pTransaction
   *          the transaction to use
   * @throws NullPointerException
   *           if {@code pTransaction}, {@code pDir} or {@code pAtts} is {@code null}
   */
  void processDirectory(T pTransaction, Path pDir, Optional<BasicFileAttributes> pAttrs);

  /**
   * Process a file.
   * 
   * @param pTransaction
   *          the transaction to use
   * @throws NullPointerException
   *           if {@code pTransaction}, {@code pFile} or {@code pAtts} is {@code null}
   */
  void processFile(T pTransaction, Path pFile, Optional<BasicFileAttributes> pAttrs);
}
