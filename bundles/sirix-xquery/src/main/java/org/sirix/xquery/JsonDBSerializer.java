/*
 * Copyright (c) 2022, SirixDB
 * <p>
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the <organization> nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
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

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Object;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.service.json.serialize.JsonSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class JsonDBSerializer implements Serializer, AutoCloseable {

  private final Appendable out;

  private final boolean prettyPrint;

  private boolean first;

  private final Set<JsonNodeReadOnlyTrx> trxSet;

  public JsonDBSerializer(final Appendable out, final boolean prettyPrint) {
    this.out = checkNotNull(out);
    this.prettyPrint = prettyPrint;
    first = true;
    trxSet = new HashSet<>();
  }

  @Override
  public void serialize(final Sequence sequence) {
    try {
      if (first) {
        first = false;
        out.append("{\"rest\":[");
      } else {
        out.append(",");
      }

      if (sequence != null) {
        Item item = null;
        Iter it;
        if (sequence instanceof Array || sequence instanceof Object) {
          item = (Item) sequence;
          it = null;
        } else {
          it = sequence.iterate();
        }

        try {
          if (item == null) {
            item = it.next();
          }
          while (item != null) {
            if (item instanceof StructuredDBItem) {
              @SuppressWarnings("unchecked")
              final var node = (StructuredDBItem<JsonNodeReadOnlyTrx>) item;
              trxSet.add(node.getTrx());

              var serializerBuilder =
                  new JsonSerializer.Builder(node.getTrx().getResourceSession(), out, node.getTrx().getRevisionNumber())
                      .serializeTimestamp(true)
                      .isXQueryResultSequence();
              if (prettyPrint) {
                serializerBuilder.prettyPrint().withInitialIndent();
              }
              final JsonSerializer serializer = serializerBuilder.startNodeKey(node.getNodeKey()).build();
              serializer.call();

              item = printCommaIfNextItemExists(it);
            } else if (item instanceof Atomic) {
              if (((Atomic) item).type() == Type.STR) {
                out.append("\"");
              }
              out.append(item.toString());
              if (((Atomic) item).type() == Type.STR) {
                out.append("\"");
              }

              item = printCommaIfNextItemExists(it);
            } else if ((item instanceof Array) || (item instanceof Object)) {
              try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
                new StringSerializer(printWriter).serialize(item);
                this.out.append(out.toString(StandardCharsets.UTF_8));
              }

              item = printCommaIfNextItemExists(it);
            }
          }
        } finally {
          if (it != null) {
            it.close();
          }
        }

      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Item printCommaIfNextItemExists(Iter it) throws IOException {
    Item item = null;
    if (it != null) {
      item = it.next();

      if (item != null) {
        out.append(",");
      }
    }
    return item;
  }

  @Override
  public void close() {
    try {
      out.append("]}");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    trxSet.forEach(JsonNodeReadOnlyTrx::close);
  }
}
