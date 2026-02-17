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
import com.google.gson.JsonObject;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.function.json.JSONParser;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;

import io.sirix.api.JsonDiff;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;

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
    final var startNodeKey = FunUtil.getInt(args, 4, "startNodeKey", 0, null, false);
    final var maxLevel = FunUtil.getInt(args, 5, "maxLevel", 0, null, false);
    final var document = collection.getDocument(resourceName);
    final var resourceSession = document.getResourceSession();

    if (resourceSession.getResourceConfig().areDeweyIDsStored && oldRevision == newRevision - 1) {
      return readDiffFromFileAndCalculateViaDeweyIDs(databaseName, resourceName, oldRevision, newRevision, startNodeKey,
          maxLevel == 0
              ? Integer.MAX_VALUE
              : maxLevel,
          resourceSession);
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
      rtx.moveTo(startNodeKey);

      final var metaInfo = createMetaInfo(databaseName, resourceName, oldRevision, newRevision);
      final var diffs = metaInfo.getAsJsonArray("diffs");
      final var updateOperations = rtx.getUpdateOperationsInSubtreeOfNode(rtx.getDeweyID(), maxLevel);
      updateOperations.forEach(diffs::add);

      return parseJsonToBrackitItem(metaInfo.toString());
    }
  }

  /**
   * Parse a JSON string into a brackit Item for proper serialization.
   *
   * @param json the JSON string to parse
   * @return the parsed brackit Item
   */
  private Item parseJsonToBrackitItem(String json) {
    return new JSONParser(json).parse();
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
