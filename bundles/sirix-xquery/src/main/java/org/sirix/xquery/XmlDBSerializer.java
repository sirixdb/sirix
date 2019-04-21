/**
XmlInsertionMode * Copyright (c) 2018, Sirix
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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
package org.sirix.xquery;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.OutputStream;
import java.io.PrintStream;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.xquery.node.XmlDBNode;

/**
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 *
 */
public final class XmlDBSerializer implements Serializer {

  private final PrintStream mOut;

  private final boolean mEmitRESTful;

  private final boolean mPrettyPrint;

  public XmlDBSerializer(final PrintStream out, final boolean emitRESTful, final boolean prettyPrint) {
    mOut = checkNotNull(out);
    mEmitRESTful = emitRESTful;
    mPrettyPrint = prettyPrint;
  }

  @Override
  public void serialize(Sequence sequence) throws QueryException {
    if (sequence != null) {
      if (mEmitRESTful) {
        if (mPrettyPrint) {
          mOut.println("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        } else {
          mOut.print("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        }
      }

      Iter it = sequence.iterate();

      boolean first = true;
      Item item;
      try {
        while ((item = it.next()) != null) {
          if (item instanceof XmlDBNode) {
            final XmlDBNode node = (XmlDBNode) item;
            final Kind kind = node.getKind();

            if (kind == Kind.ATTRIBUTE) {
              throw new QueryException(ErrorCode.ERR_SERIALIZE_ATTRIBUTE_OR_NAMESPACE_NODE);
            }

            final OutputStream pos = new PrintStream(mOut);

            XmlSerializer.XmlSerializerBuilder serializerBuilder =
                new XmlSerializer.XmlSerializerBuilder(node.getTrx().getResourceManager(), pos,
                    node.getTrx().getRevisionNumber()).serializeTimestamp(true).isXQueryResultSequence();
            if (mEmitRESTful)
              serializerBuilder = serializerBuilder.emitIDs().emitRESTful();
            if (mPrettyPrint)
              serializerBuilder = serializerBuilder.prettyPrint().withInitialIndent();
            final XmlSerializer serializer = serializerBuilder.startNodeKey(node.getNodeKey()).build();
            serializer.call();

            first = true;
          } else if (item instanceof Atomic) {
            if (!first) {
              mOut.print(" ");
            }

            mOut.print(item.toString());
            first = false;
          } else {
            // TODO
          }
        }
      } finally {
        it.close();
      }

      if (mEmitRESTful) {
        mOut.print("</rest:sequence>");
      }
    }
  }
}
