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

package org.sirix.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.exception.SirixException;

/**
 * <h1>JSON-TestDocument</h1>
 *
 * <p>
 * This class creates an XML document that contains all features seen in the Extensible Markup
 * Language (XML) 1.1 (Second Edition) as well as the Namespaces in XML 1.1 (Second Edition).
 * </p>
 *
 * <p>
 * The following figure describes the created test document (see <code>xml/test.xml</code>). The
 * nodes are described as follows:
 *
 * <ul>
 * <li><code>Kind.ROOT: doc()</code></li>
 * <li><code>Kind.ELEMENT : &lt;prefix:localPart&gt;</code></li>
 * <li><code>Kind.NAMESPACE: §prefix:namespaceURI</code></li>
 * <li><code>Kind.ATTRIBUTE: &#64;prefix:localPart='value'</code></li>
 * <li><code>Kind.TEXT: #value</code></li>
 * <li><code>Kind.COMMENT: %comment</code></li>
 * <li><code>Kind.PI: &amp;content:target</code></li>
 * </ul>
 *
 * <code><pre>
 * {
 *   "foo": ["bar", null, 2.33],
 *   "bar": { "hello": "world", "hello": true },
 *   "baz": "hello"
 * }
 * </pre></code>
 */
public final class JsonDocumentCreator {

  /**
   * Private Constructor, not used.
   */
  private JsonDocumentCreator() {
    throw new AssertionError("Not permitted to call constructor!");
  }

  /**
   * Create simple test document containing all supported node kinds.
   *
   * @param wtx {@link JsonNodeWriteTrx} to write to
   * @throws SirixException if anything weird happens
   */
  public static void create(final JsonNodeTrx wtx) {
    assertNotNull(wtx);
    assertTrue(wtx.moveToDocumentRoot().hasMoved());

    wtx.insertObjectAsFirstChild();

    wtx.insertObjectKeyAsFirstChild("foo")
       .insertArrayAsFirstChild()
       .insertStringValueAsFirstChild("bar")
       .insertNullValueAsRightSibling()
       .insertNumberValueAsRightSibling(2.33);

    wtx.moveToParent().get().moveToParent();

    assert wtx.isObjectKey();

    wtx.insertObjectKeyAsRightSibling("bar")
       .insertObjectAsFirstChild()
       .insertObjectKeyAsFirstChild("hello")
       .insertStringValueAsFirstChild("world")
       .moveToParent();
    wtx.insertObjectKeyAsRightSibling("hello")
       .insertBooleanValueAsFirstChild(true)
       .moveToParent()
       .get()
       .moveToParent()
       .get()
       .moveToParent();

    assert wtx.isObjectKey();

    wtx.insertObjectAsRightSibling().insertStringValueAsFirstChild("hello");

    wtx.moveToDocumentRoot();
  }
}
