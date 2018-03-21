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

package org.sirix.service.xml.xpath.filter;

import org.sirix.api.XdmNodeReadTrx;
import org.sirix.axis.filter.AbstractFilter;

/**
 * <h1>SchemaAttributeFilter</h1>
 * <p>
 * A SchemaAttributeTest matches an attribute node against a corresponding attribute declaration
 * found in the in-scope attribute declarations.
 * </p>
 * <p>
 * A SchemaAttributeTest matches a candidate attribute node if both of the following conditions are
 * satisfied:
 * </p>
 * <p>
 * <li>1. The name of the candidate node matches the specified AttributeName.</li>
 * <li>2. derives-from(AT, ET) is true, where AT is the type annotation of the candidate node and ET
 * is the schema type declared for attribute AttributeName in the in-scope attribute
 * declarations.</li>
 * </p>
 * <p>
 * If the AttributeName specified in the SchemaAttributeTest is not found in the in-scope attribute
 * declarations, a static error is raised [err:XPST0008].
 * </p>
 */
public class SchemaAttributeFilter extends AbstractFilter {

  // /** The specified name for the attribute. */
  // private final String attributeName;

  /**
   * Default constructor.
   * 
   * @param rtx Transaction this filter is bound to.
   */
  public SchemaAttributeFilter(final XdmNodeReadTrx rtx) {

    super(rtx);
    // attributeName = declaration;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean filter() {

    return false;

    // TODO: The result is only false, because support for schema
    // information is
    // not implemented in sirix yet. As soon as this situation changes it
    // is
    // necessary to change this method according to the following pseudo
    // code:
    //
    // if (attributeName is NOT in in-scope-declaration) {
    // throw new XPathError(ErrorType.XPST0008);
    // } else {
    // Type specifiedType = type of the attribute specified in the
    // declaration
    // return getTransaction().isAttributeKind()
    // && getTransaction().getName().equals(attributeName)
    // && (getTransaction().getValueTypeAsType() == specifiedType
    // getTransaction.getValueTypeAsType().
    // derivesFrom(specifiedDeclaration) );
    // }
    //
    // See W3C's XPath 2.0 specification for more details

  }

}
