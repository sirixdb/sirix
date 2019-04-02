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
import org.sirix.access.trx.node.json.objectvalue.ArrayValue;
import org.sirix.access.trx.node.json.objectvalue.BooleanValue;
import org.sirix.access.trx.node.json.objectvalue.ObjectValue;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
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
 *   "bar": { "hello": "world", "helloo": true },
 *   "baz": "hello",
 *   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
 * }
 * </pre></code>
 */
public final class JsonDocumentCreator {

  public static final String JSON =
      "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

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

    wtx.insertObjectRecordAsFirstChild("foo", new ArrayValue())
       .insertStringValueAsFirstChild("bar")
       .insertNullValueAsRightSibling()
       .insertNumberValueAsRightSibling(2.33);

    wtx.moveToParent().getCursor().moveToParent();

    assert wtx.isObjectKey();

    wtx.insertObjectRecordAsRightSibling("bar", new ObjectValue())
       .insertObjectRecordAsFirstChild("hello", new StringValue("world"))
       .moveToParent();
    wtx.insertObjectRecordAsRightSibling("helloo", new BooleanValue(true))
       .moveToParent()
       .getCursor()
       .moveToParent()
       .getCursor()
       .moveToParent();

    assert wtx.isObjectKey();

    wtx.insertObjectRecordAsRightSibling("baz", new StringValue("hello")).moveToParent().getCursor();

    assert wtx.isObjectKey();

    wtx.insertObjectRecordAsRightSibling("tada", new ArrayValue())
       .insertObjectAsFirstChild()
       .insertObjectRecordAsFirstChild("foo", new StringValue("bar"))
       .moveToParent()
       .getCursor()
       .moveToParent();

    assert wtx.isObject();

    wtx.insertObjectAsRightSibling()
       .insertObjectRecordAsFirstChild("baz", new BooleanValue(false))
       .moveToParent()
       .getCursor()
       .moveToParent();

    assert wtx.isObject();

    wtx.insertStringValueAsRightSibling("boo");
    wtx.insertObjectAsRightSibling();
    wtx.insertArrayAsRightSibling();

    wtx.moveToDocumentRoot();
  }
}
