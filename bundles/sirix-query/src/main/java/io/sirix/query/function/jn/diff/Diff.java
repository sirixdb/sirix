/**
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
package io.sirix.query.function.jn.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.CompactObject;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.JsonDiff;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.SirixDeweyID;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;

/**
 * <p>
 * Function for diffing two revisions of a resource in a collection/database. The Supported
 * signature is:
 * </p>
 *
 * <pre>
 * <code>sdb:diff($coll as xs:string, $res as xs:string, $rev1 as xs:int, $rev2 as xs:int, $startNodeKey as xs:int?, $maxLevel as xs:int?) as xs:string</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Diffing of two versions of a resource.",
    parameters = {"$coll, $res, $rev1, $rev2, $startNodeKey, $maxLevel"})
public final class Diff extends AbstractFunction {

  /**
   * Sort by document order name.
   */
  public final static QNm DIFF = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "diff");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Diff(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length < 4 || args.length > 7) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final var databaseName = ((Str) args[0]).stringValue();
    final var collection = (JsonDBCollection) ctx.getJsonItemStore().lookup(databaseName);

    if (collection == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final var resourceName = ((Str) args[1]).stringValue();
    final var oldRevision = FunUtil.getInt(args, 2, "revision1", -1, null, true);
    final var newRevision = FunUtil.getInt(args, 3, "revision2", -1, null, true);
    // Validate the revision pair — same as the REST DiffHandler. A reversed pair (old >= new)
    // otherwise skipped the dewey fast path and produced a silent BACKWARDS diff (inserts and
    // deletes swapped).
    if (oldRevision < 1 || newRevision < 1) {
      throw new QueryException(new QNm("Revisions must be >= 1."));
    }
    if (oldRevision >= newRevision) {
      throw new QueryException(new QNm("revision1 (" + oldRevision + ") must be less than revision2 ("
          + newRevision + ")."));
    }
    final var startNodeKey = FunUtil.getInt(args, 4, "startNodeKey", 0, null, false);
    final var maxLevel = FunUtil.getInt(args, 5, "maxLevel", 0, null, false);
    final var document = collection.getDocument(resourceName);
    final var resourceSession = document.getResourceSession();

    // The Dewey-ID fast path reads the pre-computed per-revision diff sidecar. Its core reader does
    // the strict single read plus identity, integrity, Unicode, schema, and fragment-node checks;
    // any failure below falls through to the authoritative revision-to-revision computation.
    final var updateOperationsFile = resourceSession.getResourceConfig()
                                                    .getResource()
                                                    .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                                    .resolve("diffFromRev" + oldRevision + "toRev" + newRevision
                                                                 + ".json");

    if (resourceSession.getResourceConfig().areDeweyIDsStored && oldRevision == newRevision - 1
        && Files.exists(updateOperationsFile)) {
      try {
        return readDiffFromFileAndCalculateViaDeweyIDs(databaseName,
            resourceName,
            oldRevision,
            newRevision,
            startNodeKey,
            maxLevel == 0
                ? Integer.MAX_VALUE
                : maxLevel,
            resourceSession);
      } catch (final RuntimeException ignored) {
        // The sidecar is a rebuildable cache. Missing integrity metadata, a torn/tampered payload,
        // incomplete operations, or an unreadable referenced fragment must never escape this path.
      }
    }

    final JsonDiff jsonDiff = new BasicJsonDiff(collection.getDatabase().getName());
    final String diffJson =
        jsonDiff.generateDiff(document.getResourceSession(), oldRevision, newRevision, startNodeKey, maxLevel);

    return parseJsonToBrackitItem(diffJson);
  }

  private Item readDiffFromFileAndCalculateViaDeweyIDs(String databaseName, String resourceName, int oldRevision,
      int newRevision, int startNodeKey, int maxLevel, JsonResourceSession resourceSession) {
    // Fast track... just read the info from a file and use dewey IDs to determine changes in the
    // desired subtree.
    try (final var rtx = resourceSession.beginNodeReadOnlyTrx(newRevision)) {
      final SirixDeweyID rootDeweyId;
      if (rtx.moveTo(startNodeKey)) {
        rootDeweyId = rtx.getDeweyID();
      } else {
        // A requested subtree root can itself be deleted or replaced, so its node key need not
        // exist in the new revision used to hydrate cached inserts/replacements. Resolve the
        // filtering anchor in the old revision instead; silently retaining the failed new-revision
        // cursor would filter from the document root and leak unrelated operations.
        try (final var oldRtx = resourceSession.beginNodeReadOnlyTrx(oldRevision)) {
          if (!oldRtx.moveTo(startNodeKey)) {
            throw new IllegalStateException("Diff start node " + startNodeKey
                + " exists in neither requested revision");
          }
          rootDeweyId = oldRtx.getDeweyID();
        }
      }

      final var metaInfo = createMetaInfo(databaseName, resourceName, oldRevision, newRevision);
      final var diffs = metaInfo.getAsJsonArray("diffs");
      final var updateOperations = rtx.getUpdateOperationsInSubtreeOfNode(rootDeweyId, maxLevel);
      updateOperations.forEach(diffs::add);

      return toBrackitItem(metaInfo);
    }
  }

  /**
   * Convert JSON text to a Brackit item with semantic, decoded string values.
   *
   * <p>Brackit's legacy tokenizer-backed {@code JSONParser} preserves JSON escape spellings in
   * {@link Str} values. That used to pair with a serializer which wrote strings verbatim, but an
   * RFC 8259-correct serializer escapes those retained backslashes a second time. The byte-oriented
   * {@code FastJSONParser} decodes escapes, but its escaped-string path currently corrupts raw
   * multi-byte UTF-8 following an escape. A strict streaming Gson reader validates and decodes the
   * generated diff directly into the Brackit tree, keeping quotes, controls, backslashes, Unicode,
   * and arbitrary-precision JSON numbers exact without retaining an intermediate Gson DOM.</p>
   *
   * @param json the JSON string to parse
   * @return the parsed Brackit item
   */
  static Item parseJsonToBrackitItem(final String json) {
    if (json == null) {
      throw new QueryException(JSONFun.ERR_PARSING_ERROR, (Object) "JSON input must not be null");
    }

    try (final var reader = new JsonReader(new StringReader(json))) {
      reader.setStrictness(Strictness.STRICT);
      if (reader.peek() == JsonToken.END_DOCUMENT) {
        throw new QueryException(JSONFun.ERR_PARSING_ERROR, (Object) "Empty JSON input");
      }

      final Item item = readBrackitItem(reader);
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw new QueryException(JSONFun.ERR_PARSING_ERROR, (Object) "Trailing content after JSON value");
      }
      return item;
    } catch (final IOException | JsonParseException e) {
      throw new QueryException(e, JSONFun.ERR_PARSING_ERROR, (Object) e.getMessage());
    }
  }

  private static Item readBrackitItem(final JsonReader reader) throws IOException {
    return switch (reader.peek()) {
      case BEGIN_OBJECT -> readBrackitObject(reader);
      case BEGIN_ARRAY -> readBrackitArray(reader);
      case STRING -> {
        final String value = reader.nextString();
        validateUnicodeScalarValue(value);
        yield new Str(value);
      }
      case NUMBER -> toBrackitNumber(reader.nextString());
      case BOOLEAN -> reader.nextBoolean()
          ? Bool.TRUE
          : Bool.FALSE;
      case NULL -> {
        reader.nextNull();
        yield Null.INSTANCE;
      }
      default -> throw new QueryException(JSONFun.ERR_PARSING_ERROR,
          (Object) ("Unexpected JSON token " + reader.peek()));
    };
  }

  private static CompactObject readBrackitObject(final JsonReader reader) throws IOException {
    reader.beginObject();
    final var fieldNames = new ArrayList<String>();
    final var values = new ArrayList<Sequence>();
    while (reader.hasNext()) {
      final String fieldName = reader.nextName();
      validateUnicodeScalarValue(fieldName);
      final Sequence value = readBrackitItem(reader);

      // Match Gson's established last-value-wins handling while retaining the first occurrence's
      // object position. Diff objects are deliberately narrow, so this allocation-free linear
      // duplicate check is cheaper than constructing a hash table for every object.
      final int duplicateIndex = fieldNames.indexOf(fieldName);
      if (duplicateIndex >= 0) {
        values.set(duplicateIndex, value);
      } else {
        fieldNames.add(fieldName);
        values.add(value);
      }
    }
    reader.endObject();

    final var names = new QNm[fieldNames.size()];
    for (int index = 0; index < names.length; index++) {
      names[index] = new QNm(fieldNames.get(index));
    }
    return new CompactObject(names, values.toArray(Sequence[]::new));
  }

  private static DArray readBrackitArray(final JsonReader reader) throws IOException {
    reader.beginArray();
    final var values = new ArrayList<Sequence>();
    while (reader.hasNext()) {
      values.add(readBrackitItem(reader));
    }
    reader.endArray();
    return new DArray(values);
  }

  private static Item toBrackitItem(final JsonElement element) {
    validateUnicodeScalarValues(element);
    return toBrackitItemUnchecked(element);
  }

  private static Item toBrackitItemUnchecked(final JsonElement element) {
    if (element.isJsonNull()) {
      return Null.INSTANCE;
    }

    if (element.isJsonObject()) {
      final JsonObject object = element.getAsJsonObject();
      final QNm[] names = new QNm[object.size()];
      final Sequence[] values = new Sequence[object.size()];
      int index = 0;
      for (final var entry : object.entrySet()) {
        names[index] = new QNm(entry.getKey());
        values[index] = toBrackitItemUnchecked(entry.getValue());
        index++;
      }
      return new CompactObject(names, values);
    }

    if (element.isJsonArray()) {
      final JsonArray array = element.getAsJsonArray();
      final var values = new ArrayList<Sequence>(array.size());
      for (final JsonElement value : array) {
        values.add(toBrackitItemUnchecked(value));
      }
      return new DArray(values);
    }

    final JsonPrimitive primitive = element.getAsJsonPrimitive();
    if (primitive.isBoolean()) {
      return primitive.getAsBoolean()
          ? Bool.TRUE
          : Bool.FALSE;
    }
    if (primitive.isString()) {
      return new Str(primitive.getAsString());
    }
    if (primitive.isNumber()) {
      return toBrackitNumber(primitive.getAsString());
    }
    throw new IllegalArgumentException("Unsupported JSON primitive: " + primitive);
  }

  private static Item toBrackitNumber(final String value) {
    try {
      final boolean hasExponent = value.indexOf('e') >= 0 || value.indexOf('E') >= 0;
      if (hasExponent || value.indexOf('.') >= 0) {
        final BigDecimal exactValue = new BigDecimal(value);
        if (hasExponent) {
          // Keep the historical xs:double type when Brackit's serializer can round-trip the exact
          // JSON number. Promote only lossy/overflowing/underflowing exponent forms to xs:decimal;
          // otherwise values such as 1e309 become invalid JSON (INF) and 1e-400 silently becomes 0.
          final Dbl doubleValue = new Dbl(value);
          if (Double.isFinite(doubleValue.doubleValue())
              && exactValue.compareTo(BigDecimal.valueOf(doubleValue.doubleValue())) == 0) {
            return doubleValue;
          }
          return new JsonExponentDecimal(exactValue);
        }
        return new Dec(exactValue);
      }
      return Int32.parse(value);
    } catch (final NumberFormatException e) {
      // JSON itself permits an exponent outside BigDecimal's signed-int scale range. Brackit has
      // no exact representation for it; report the same parse-error code as every other bridge
      // conversion failure instead of leaking an implementation exception.
      throw new QueryException(e, JSONFun.ERR_PARSING_ERROR, (Object) e.getMessage());
    }
  }

  /**
   * Exact decimal fallback for exponent-form JSON numbers which cannot survive an xs:double
   * round trip. Brackit's JSON serializer renders numeric items through {@link #toString()}, while
   * the inherited {@link Dec#stringValue()} retains the canonical xs:decimal lexical form. Keeping
   * those paths distinct prevents a compact valid JSON token such as {@code 1e1000000000} from
   * expanding to roughly one gigabyte during JSON serialization without changing query-visible
   * xs:decimal string and cast semantics.
   */
  private static final class JsonExponentDecimal extends Dec {
    private JsonExponentDecimal(final BigDecimal value) {
      super(value);
    }

    @Override
    public String toString() {
      return decimalValue().toString();
    }
  }

  private static void validateUnicodeScalarValues(final JsonElement element) {
    if (element.isJsonObject()) {
      for (final var entry : element.getAsJsonObject().entrySet()) {
        validateUnicodeScalarValue(entry.getKey());
        validateUnicodeScalarValues(entry.getValue());
      }
      return;
    }

    if (element.isJsonArray()) {
      for (final JsonElement value : element.getAsJsonArray()) {
        validateUnicodeScalarValues(value);
      }
      return;
    }

    if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
      validateUnicodeScalarValue(element.getAsString());
    }
  }

  private static void validateUnicodeScalarValue(final String value) {
    for (int index = 0; index < value.length(); index++) {
      final char current = value.charAt(index);
      if (Character.isHighSurrogate(current)) {
        if (++index >= value.length() || !Character.isLowSurrogate(value.charAt(index))) {
          throw invalidUnicodeScalarValue();
        }
      } else if (Character.isLowSurrogate(current)) {
        throw invalidUnicodeScalarValue();
      }
    }
  }

  private static QueryException invalidUnicodeScalarValue() {
    return new QueryException(JSONFun.ERR_PARSING_ERROR,
        (Object) "JSON strings must not contain unpaired UTF-16 surrogates");
  }

  private JsonObject createMetaInfo(String databaseName, String resourceName, int oldRevision, int newRevision) {
    final var json = new JsonObject();
    json.addProperty("database", databaseName);
    json.addProperty("resource", resourceName);
    json.addProperty("old-revision", oldRevision);
    json.addProperty("new-revision", newRevision);
    final var diffsArray = new JsonArray();
    json.add("diffs", diffsArray);
    return json;
  }
}
