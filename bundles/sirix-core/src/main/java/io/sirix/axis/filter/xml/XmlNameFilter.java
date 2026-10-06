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

package io.sirix.axis.filter.xml;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.brackit.query.atomic.QNm;
import io.sirix.axis.filter.AbstractFilter;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Filters named XML nodes. The {@link QNm} constructor matches namespace URI and local name,
 * ignoring the prefix. The {@link String} constructor matches the lexical prefix and local name
 * without resolving a namespace context.
 */
public final class XmlNameFilter extends AbstractFilter<XmlNodeReadOnlyTrx> {

  /** Local name to test. */
  private final String mLocalName;

  /** Prefix to test for a lexical name without a namespace context. */
  private final String mPrefix;

  /** Namespace URI for an expanded-name test, or null for a lexical name test. */
  private final @Nullable String mNamespaceURI;

  /**
   * Creates an expanded-name test. Prefix aliases do not affect matching.
   *
   * @param rtx the node trx/node cursor this filter is bound to
   * @param name namespace URI and local name to match
   */
  public XmlNameFilter(final XmlNodeReadOnlyTrx rtx, final QNm name) {
    super(rtx);
    requireNonNull(name);
    mPrefix = "";
    mNamespaceURI = name.getNamespaceURI() == null
        ? ""
        : name.getNamespaceURI();
    mLocalName = name.getLocalName();
  }

  /**
   * Creates a lexical-name test without resolving a namespace context.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} this filter is bound to
   * @param name local name with an optional prefix; both must match literally
   */
  public XmlNameFilter(final XmlNodeReadOnlyTrx rtx, final String name) {
    super(rtx);
    requireNonNull(name);
    mNamespaceURI = null;
    final int index = name.indexOf(":");
    if (index != -1) {
      mPrefix = name.substring(0, index);
    } else {
      mPrefix = "";
    }

    mLocalName = name.substring(index + 1);
  }

  @Override
  public boolean filter() {
    final XmlNodeReadOnlyTrx trx = getTrx();
    if (!trx.isNameNode() || !mLocalName.equals(trx.nameForKey(trx.getLocalNameKey()))) {
      return false;
    }
    if (mNamespaceURI == null) {
      return trx.getPrefixKey() == -1
          ? mPrefix.isEmpty()
          : mPrefix.equals(trx.nameForKey(trx.getPrefixKey()));
    }
    // Elements use -1 for an absent URI; attributes store the empty URI in the dictionary.
    return trx.getURIKey() == -1
        ? mNamespaceURI.isEmpty()
        : mNamespaceURI.equals(trx.getNamespaceURI());
  }
}
