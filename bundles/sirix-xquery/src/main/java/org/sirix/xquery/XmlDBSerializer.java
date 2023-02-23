/*
 * Copyright (c) 2023, Sirix Contributors
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

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.jdm.Kind;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.xquery.node.XmlDBNode;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class XmlDBSerializer implements Serializer, AutoCloseable {

  private final PrintStream out;

  private final boolean emitRESTful;

  private final boolean prettyPrint;

  private boolean first;

  private final Set<XmlNodeReadOnlyTrx> trxSet;

  public XmlDBSerializer(final PrintStream out, final boolean emitRESTful, final boolean prettyPrint) {
    this.out = checkNotNull(out);
    this.emitRESTful = emitRESTful;
    this.prettyPrint = prettyPrint;
    first = true;
    trxSet = new HashSet<>();
  }

  @Override
  public void serialize(Sequence sequence) throws QueryException {
    if (sequence != null) {
      if (emitRESTful && first) {
        first = false;
        if (prettyPrint) {
          out.println("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        } else {
          out.print("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        }
      }

      try (Iter it = sequence.iterate()) {
        boolean first = true;
        Item item;
        while ((item = it.next()) != null) {
          if (item instanceof final XmlDBNode node) {
            trxSet.add(node.getTrx());
            final Kind kind = node.getKind();

            if (kind == Kind.ATTRIBUTE) {
              throw new QueryException(ErrorCode.ERR_SERIALIZE_ATTRIBUTE_OR_NAMESPACE_NODE);
            }

            final OutputStream pos = new PrintStream(out);

            XmlSerializer.XmlSerializerBuilder serializerBuilder =
                new XmlSerializer.XmlSerializerBuilder(node.getTrx().getResourceSession(),
                                                       pos,
                                                       node.getTrx().getRevisionNumber()).serializeTimestamp(true)
                                                                                         .isXQueryResultSequence();
            if (emitRESTful)
              serializerBuilder = serializerBuilder.emitIDs().emitRESTful();
            if (prettyPrint)
              serializerBuilder = serializerBuilder.prettyPrint().withInitialIndent();
            final XmlSerializer serializer = serializerBuilder.startNodeKey(node.getNodeKey()).build();
            serializer.call();

            first = true;
          } else if (item instanceof Atomic) {
            if (!first) {
              out.print(" ");
            }

            out.print(item);
            first = false;
          } else {
            new StringSerializer(out).serialize(item);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    if (emitRESTful) {
      out.print("</rest:sequence>");
    }

    trxSet.forEach(XmlNodeReadOnlyTrx::close);
  }
}
