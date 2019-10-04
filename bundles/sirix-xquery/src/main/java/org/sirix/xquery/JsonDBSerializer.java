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

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Record;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.service.json.serialize.JsonSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 *
 */
public final class JsonDBSerializer implements Serializer {

  private final Appendable mOut;

  private final boolean mPrettyPrint;

  public JsonDBSerializer(final Appendable out, final boolean prettyPrint) {
    mOut = checkNotNull(out);
    mPrettyPrint = prettyPrint;
  }

  @Override
  public void serialize(final Sequence sequence) {
    try {
      if (sequence != null) {
        if (mPrettyPrint) {
          mOut.append("{\"rest\":[");
        } else {
          mOut.append("{\"rest\":[");
        }

        var it = sequence.iterate();

        Item item;
        try {
          item = it.next();
          while (item != null) {
            if (item instanceof StructuredDBItem) {
              @SuppressWarnings("unchecked")
              final var node = (StructuredDBItem<JsonNodeReadOnlyTrx>) item;

              var serializerBuilder = new JsonSerializer.Builder(node.getTrx().getResourceManager(), mOut,
                  node.getTrx().getRevisionNumber()).serializeTimestamp(true).isXQueryResultSequence();
              if (mPrettyPrint)
                serializerBuilder = serializerBuilder.prettyPrint().withInitialIndent();
              final JsonSerializer serializer = serializerBuilder.startNodeKey(node.getNodeKey()).build();
              serializer.call();

              item = it.next();

              if (item != null)
                mOut.append(",");
            } else if (item instanceof Atomic) {
              mOut.append(item.toString());

              item = it.next();

              if (item != null)
                mOut.append(",");
            } else if ((item instanceof Array) || (item instanceof Record)) {
              final var out = new ByteArrayOutputStream();
              final var printWriter = new PrintWriter(out);
              new StringSerializer(printWriter).serialize(item);
              mOut.append(out.toString(StandardCharsets.UTF_8));

              item = it.next();

              if (item != null)
                mOut.append(",");
            }
          }
        } finally {
          it.close();
        }

        mOut.append("]}");
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
