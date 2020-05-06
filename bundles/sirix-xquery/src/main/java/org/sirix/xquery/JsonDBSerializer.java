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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Record;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.service.json.serialize.JsonSerializer;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class JsonDBSerializer implements Serializer, AutoCloseable {

  private final Appendable out;

  private final boolean prettyPrint;

  private boolean first;

  public JsonDBSerializer(final Appendable out, final boolean prettyPrint) {
    this.out = checkNotNull(out);
    this.prettyPrint = prettyPrint;
    first = true;
  }

  @Override
  public void serialize(final Sequence sequence) {
    try {
      if (sequence != null) {
        if (first) {
          out.append("{\"rest\":[");
        }

        var it = sequence.iterate();

        Item item;
        try {
          item = it.next();
          while (item != null) {
            if (item instanceof StructuredDBItem) {
              @SuppressWarnings("unchecked")
              final var node = (StructuredDBItem<JsonNodeReadOnlyTrx>) item;

              var serializerBuilder = new JsonSerializer.Builder(node.getTrx().getResourceManager(), out,
                  node.getTrx().getRevisionNumber()).serializeTimestamp(true).isXQueryResultSequence();
              if (prettyPrint)
                serializerBuilder = serializerBuilder.prettyPrint().withInitialIndent();
              final JsonSerializer serializer = serializerBuilder.startNodeKey(node.getNodeKey()).build();
              serializer.call();

              item = it.next();

              if (item != null)
                out.append(",");
            } else if (item instanceof Atomic) {
              if (((Atomic) item).type() == Type.STR) {
                out.append("\"");
              }
              out.append(item.toString());
              if (((Atomic) item).type() == Type.STR) {
                out.append("\"");
              }

              item = it.next();

              if (item != null)
                out.append(",");
            } else if ((item instanceof Array) || (item instanceof Record)) {
              final var out = new ByteArrayOutputStream();
              final var printWriter = new PrintWriter(out);
              new StringSerializer(printWriter).serialize(item);
              this.out.append(out.toString(StandardCharsets.UTF_8));

              item = it.next();

              if (item != null)
                this.out.append(",");
            }
          }
        } finally {
          it.close();
        }

      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      out.append("]}");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
