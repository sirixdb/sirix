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

package org.sirix.axis.filter.xml;

import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;
import org.sirix.node.NodeKind;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Filters ELEMENTS and ATTRIBUTES and supports wildcards either instead of the namespace prefix, or
 * the local name.
 * </p>
 */
public final class WildcardFilter extends AbstractFilter<XmlNodeReadOnlyTrx> {

  /** Type. */
  public enum EType {
    /** Prefix filter. */
    PREFIX,

    /** Local name filter. */
    LOCALNAME
  }

  /** Defines, if the defined part of the qualified name is the local name. */
  private final EType mType;

  /** Name key of the defined name part. */
  private final int mKnownPartKey;

  /**
   * Default constructor.
   *
   * @param rtx transaction to operate on
   * @param knownPart part of the qualified name that is specified. This can be either the namespace
   *        prefix, or the local name
   * @param type defines, if the specified part is the prefix, or the local name (true, if it is
   *        the local name)
   */
  public WildcardFilter(final XmlNodeReadOnlyTrx rtx, final String knownPart, final EType type) {
    super(rtx);
    mType = requireNonNull(type);
    mKnownPartKey = getTrx().keyForName(requireNonNull(knownPart));
  }

  @Override
  public final boolean filter() {
    final NodeKind kind = getTrx().getKind();
    switch (kind) {
      case ELEMENT:
        if (mType == EType.LOCALNAME) { // local name is given
          return localNameMatch();
        } else { // namespace prefix is given
          final int prefixKey = mKnownPartKey;
          for (int i = 0, nsCount = getTrx().getNamespaceCount(); i < nsCount; i++) {
            getTrx().moveToNamespace(i);
            if (getTrx().getPrefixKey() == prefixKey) {
              getTrx().moveToParent();
              return true;
            }
            getTrx().moveToParent();
          }
          return false;
        }
      case ATTRIBUTE:
        if (mType == EType.LOCALNAME) { // local name is given
          return localNameMatch();
        } else {
          final String prefix = getTrx().nameForKey(getTrx().getPrefixKey());
          final int prefixKey = getTrx().keyForName(prefix);
          return prefixKey == mKnownPartKey;
        }
        // $CASES-OMITTED$
      default:
        return false;
    }
  }

  /**
   * Determines if local names match.
   *
   * @return {@code true}, if they match, {@code false} otherwise
   */
  private boolean localNameMatch() {
    final int localnameKey = getTrx().keyForName(getTrx().getName().getLocalName());
    return localnameKey == mKnownPartKey;
  }
}
