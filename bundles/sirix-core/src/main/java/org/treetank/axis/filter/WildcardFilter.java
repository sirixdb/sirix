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

package org.treetank.axis.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import org.treetank.api.INodeReadTrx;
import org.treetank.node.ENode;
import org.treetank.node.ElementNode;
import org.treetank.node.interfaces.INameNode;

/**
 * <h1>WildcardFilter</h1>
 * <p>
 * Filters ELEMENTS and ATTRIBUTES and supports wildcards either instead of the namespace prefix, or the local
 * name.
 * </p>
 */
public class WildcardFilter extends AbsFilter {

  public enum EType {
    PREFIX,

    LOCALNAME
  }

  /** Defines, if the defined part of the qualified name is the local name. */
  private final EType mType;

  /** Name key of the defined name part. */
  private final int mKnownPartKey;

  /**
   * Default constructor.
   * 
   * @param pRtx
   *          transaction to operate on
   * @param pKnownPart
   *          part of the qualified name that is specified. This can be
   *          either the namespace prefix, or the local name
   * @param pIsName
   *          defines, if the specified part is the prefix, or the local
   *          name (true, if it is the local name)
   */
  public WildcardFilter(final INodeReadTrx pRtx, final String pKnownPart, final EType pType) {
    super(pRtx);
    mType = checkNotNull(pType);
    mKnownPartKey = getTransaction().keyForName(checkNotNull(pKnownPart));
  }

  @Override
  public final boolean filter() {
    final ENode kind = getTransaction().getNode().getKind();
    switch (kind) {
    case ELEMENT_KIND:
      if (mType == EType.LOCALNAME) { // local name is given
        return localNameMatch();
      } else { // namespace prefix is given
        final int prefixKey = mKnownPartKey;
        for (int i = 0, nsCount = ((ElementNode)getTransaction().getNode()).getNamespaceCount(); i < nsCount; i++) {
          getTransaction().moveToNamespace(i);
          if (((INameNode)getTransaction().getNode()).getNameKey() == prefixKey) {
            getTransaction().moveToParent();
            return true;
          }
          getTransaction().moveToParent();
        }
        return false;
      }
    case ATTRIBUTE_KIND:
      if (mType == EType.LOCALNAME) { // local name is given
        return localNameMatch();
      } else {
        final String localname =
          getTransaction().nameForKey(((INameNode)getTransaction().getNode()).getNameKey()).replaceFirst(
            ":.*", "");
        final int localnameKey = getTransaction().keyForName(localname);
        return localnameKey == mKnownPartKey;
      }
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
    final String localname =
      getTransaction().nameForKey(((INameNode)getTransaction().getNode()).getNameKey()).replaceFirst(".*:",
        "");
    final int localnameKey = getTransaction().keyForName(localname);
    return localnameKey == mKnownPartKey;
  }
}
