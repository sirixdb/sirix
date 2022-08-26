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
package org.sirix.xquery.function.jn.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.JsonDiff;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.service.json.BasicJsonDiff;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.json.JsonDBCollection;

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
    parameters = { "$coll, $res, $rev1, $rev2, $startNodeKey, $maxLevel" })
public final class Diff extends AbstractFunction {

  /**
   * Sort by document order name.
   */
  public final static QNm DIFF = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "diff");

  /**
   * Constructor.
   *
   * @param name      the name of the function
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

    final var dbName = ((Str) args[0]).stringValue();
    final var col = (JsonDBCollection) ctx.getJsonItemStore().lookup(dbName);

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final var expResName = ((Str) args[1]).stringValue();
    final var oldRevision = FunUtil.getInt(args, 2, "revision1", -1, null, true);
    final var newRevision = FunUtil.getInt(args, 3, "revision2", -1, null, true);
    final var startNodeKey = FunUtil.getInt(args, 4, "startNodeKey", 0, null, false);
    final var maxLevel = FunUtil.getInt(args, 5, "maxLevel", 0, null, false);
    final var doc = col.getDocument(expResName);
    final var resourceMgr = doc.getResourceSession();

    if (resourceMgr.getResourceConfig().areDeweyIDsStored && oldRevision == newRevision - 1) {
      return readDiffFromFileAndCalculateViaDeweyIDs(dbName,
                                                     expResName,
                                                     oldRevision,
                                                     newRevision,
                                                     startNodeKey,
                                                     maxLevel == 0 ? Integer.MAX_VALUE : maxLevel,
                                                     resourceMgr);
    }

    final JsonDiff jsonDiff = new BasicJsonDiff(col.getDatabase().getName());

    return new Str(jsonDiff.generateDiff(doc.getResourceSession(), oldRevision, newRevision, startNodeKey, maxLevel));
  }

  @NotNull
  private Str readDiffFromFileAndCalculateViaDeweyIDs(String dbName, String expResName, int oldRevision,
      int newRevision, int startNodeKey, int maxLevel, JsonResourceSession resourceMgr) {
    // Fast track... just read the info from a file and use dewey IDs to determine changes in the desired subtree.
    try (final var rtx = resourceMgr.beginNodeReadOnlyTrx(newRevision)) {
      rtx.moveTo(startNodeKey);

      final var metaInfo = createMetaInfo(dbName, expResName, oldRevision, newRevision);
      final var diffs = metaInfo.getAsJsonArray("diffs");
      final var updateOperations = rtx.getUpdateOperationsInSubtreeOfNode(rtx.getDeweyID(), maxLevel);
      updateOperations.forEach(diffs::add);

      return new Str(metaInfo.toString());
    }
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
