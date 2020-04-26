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

import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;

/**
 * <p>
 * A SchemaElementTest matches an element node against a corresponding element declaration found in
 * the in-scope element declarations.
 * </p>
 * <p>
 * A SchemaElementTest matches a candidate element node if both of the following conditions are
 * satisfied:
 * </p>
 * <p>
 * <li>1. The name of the candidate node matches the specified ElementName or matches the name of an
 * element in a substitution group headed by an element named ElementName.</li>
 * <li>derives-from(AT, ET) is true, where AT is the type annotation of the candidate node and ET is
 * the schema type declared for element ElementName in the in-scope element declarations.</li>
 * <li>3. If the element declaration for ElementName in the in-scope element declarations is not
 * nillable, then the nilled property of the candidate node is false.</li>
 * </p>
 * <p>
 * If the ElementName specified in the SchemaElementTest is not found in the in-scope Element
 * declarations, a static error is raised [err:XPST0008].
 * </p>
 */
public class SchemaElementFilter extends AbstractFilter {

  // /** The specified name for the element. */
  // private final String elementName;

  /**
   * Default constructor.
   * 
   * @param rtx Transaction this filter is bound to..
   */
  public SchemaElementFilter(final XmlNodeReadOnlyTrx rtx) {

    super(rtx);
    // elementName = declaration;
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
    // if (ElementName is NOT in in-scope-declaration) {
    // throw new XPathError(ErrorType.XPST0008);
    // } else {
    // Type specifiedType = type of the Element specified in the declaration
    // return getTransaction().isElementKind()
    // && (getTransaction().getName().equals(ElementName)
    // || substitution group matches elementName)
    // && (getTransaction().getValueTypeAsType() == specifiedType
    // || getTransaction.getValueTypeAsType().
    // derivesFrom(specifiedDeclaration)
    // && if (element declaration is not nillable) nilled property of
    // candidate node has to be false);

    // }
    //
    // See W3C's XPath 2.0 specification for more details

  }

}
